# -*- coding: utf-8 -*-
"""
scripts/cloud_runner.py
Headless cloud orchestrator for GitHub Actions (zero-cost modular architecture):
  - `--task crawl`: Run pure-HTTP supplier spiders -> classify pending products -> sync images & catalog_db.dat -> email summary
  - `--task pdf`  : Scan Google Drive `待解析PDF/` -> identify & parse PDFs -> classify -> sync -> archive to `已归档PDF/` -> email report
  - `--task sync` : Classify any pending products and sync images + catalog_db.dat to R2 / Drive API
"""
import argparse
import asyncio
import json
import logging
import os
import shutil
import sys
import tempfile
from datetime import datetime
from typing import Dict, List, Optional

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("cloud_runner")

from database.db_manager import DatabaseManager
from crawler.scheduler import (
    CrawlerScheduler,
    _is_alibaba1688,
    _is_modapoint,
    _is_smartycart,
    _is_tiendanube,
    _is_wc_store,
)
from crawler.whatsapp_monitor import (
    PDFSupplierDetector,
    import_pdf_to_supplier,
    send_email_notification,
)
from scripts.classification_batches.apply_standard_categories import classify_pending_products
from scripts.cloud_sync import (
    CLOUD_FOLDER_ID,
    build_gdrive_service,
    ensure_gdrive_folder,
    load_env_file,
    run_full_cloud_sync,
)

PDF_INBOX_FOLDER_NAME = os.getenv("GDRIVE_PDF_INBOX", "待解析PDF")
PDF_ARCHIVE_FOLDER_NAME = os.getenv("GDRIVE_PDF_ARCHIVE", "已归档PDF")


def is_cloud_http_supplier(supplier: dict) -> bool:
    """Return True if supplier can run headlessly without local Chrome cookies or Playwright."""
    if not supplier.get("is_active"):
        return False
    base_url = (supplier.get("base_url") or "").strip().lower()
    list_url = (supplier.get("list_url") or "").strip().lower()
    if not base_url or base_url in ("nashi", "pdf") or list_url.startswith("whatsapp:"):
        return False
    if _is_alibaba1688(supplier):
        return False
    if (
        _is_tiendanube(supplier)
        or _is_wc_store(supplier)
        or _is_modapoint(supplier)
        or _is_smartycart(supplier)
    ):
        return True
    if supplier.get("use_js") or supplier.get("use_chrome_cookies"):
        return False
    return True


def select_target_suppliers(db: DatabaseManager, spec: str) -> List[dict]:
    all_suppliers = db.get_all_suppliers()
    if spec and spec.strip().lower() not in ("all", "all_http", "default"):
        wanted = {int(x.strip()) for x in spec.split(",") if x.strip().isdigit()}
        return [s for s in all_suppliers if s["id"] in wanted]
    return [s for s in all_suppliers if is_cloud_http_supplier(s)]


def _notify_safe(subject: str, body: str) -> None:
    if not os.getenv("SMTP_AUTH_CODE", "").strip():
        logger.info("SMTP_AUTH_CODE 未配置，跳过云端邮件通知。")
        return
    try:
        send_email_notification({"email_notify": {}}, subject, body)
    except Exception as e:
        logger.warning(f"邮件通知发送失败: {e}")


async def _run_crawl_task(db: DatabaseManager, args) -> dict:
    targets = select_target_suppliers(db, args.suppliers)
    logger.info(f"[Crawl] 目标云端 HTTP 供应商 ({len(targets)} 家): {[(s['id'], s['name']) for s in targets]}")

    if args.dry_run:
        return {"dry_run": True, "targets": [{"id": s["id"], "name": s["name"]} for s in targets]}

    scheduler = CrawlerScheduler(db)
    per_supplier = []
    touched_ids = []

    for s in targets:
        sid = s["id"]
        sname = s["name"]
        try:
            logger.info(f"=== 开始爬取 [{sid}] {sname} ===")
            await scheduler.run_supplier(sid)
            per_supplier.append({"id": sid, "name": sname, "status": "ok"})
            touched_ids.append(sid)
        except Exception as e:
            logger.exception(f"[{sid}] {sname} 爬取失败: {e}")
            per_supplier.append({"id": sid, "name": sname, "status": "error", "error": str(e)})

    cls_stats = {}
    for sid in touched_ids:
        cls_stats[sid] = classify_pending_products(db, supplier_id=sid)

    sync_res = None
    if not args.skip_sync:
        sync_res = run_full_cloud_sync(max_copy=args.max_copy, db=db)

    ok_cnt = sum(1 for r in per_supplier if r["status"] == "ok")
    err_cnt = len(per_supplier) - ok_cnt
    bundle_ver = (sync_res or {}).get("bundle", {}).get("version", "-")
    summary_lines = [
        f"GitHub Actions 云端定时爬虫任务完成 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})",
        f"成功: {ok_cnt} 家 | 失败: {err_cnt} 家 | Catalog 版本: {bundle_ver}",
        "",
    ]
    for r in per_supplier:
        cls_updated = cls_stats.get(r["id"], {}).get("updated_products", 0)
        line = f"- [{r['id']}] {r['name']}: {r['status']} (自动分类新/变更商品 {cls_updated} 件)"
        if r.get("error"):
            line += f" | Error: {r['error']}"
        summary_lines.append(line)

    _notify_safe(f"[云端爬虫] {ok_cnt}/{len(per_supplier)} 家完成 (v{bundle_ver})", "\n".join(summary_lines))
    return {"suppliers": per_supplier, "classification": cls_stats, "sync": sync_res}


def _collect_drive_inbox_pdfs(service, inbox_folder_id: str) -> List[dict]:
    """List PDF files directly inside inbox_folder_id and one level of subfolders (excluding 无法识别)."""
    from scripts.cloud_sync import _gdrive_list_children

    found = []
    top_items = _gdrive_list_children(service, inbox_folder_id)
    for item in top_items:
        mime = item.get("mimeType", "")
        name = item.get("name", "")
        if mime == "application/vnd.google-apps.folder":
            if name in ("无法识别", "_unrecognized"):
                continue
            sub_items = _gdrive_list_children(service, item["id"], "mimeType != 'application/vnd.google-apps.folder'")
            for sf in sub_items:
                if sf.get("name", "").lower().endswith(".pdf"):
                    found.append({
                        "file_id": sf["id"],
                        "name": sf["name"],
                        "parent_id": item["id"],
                        "hint_name": name,
                    })
        elif name.lower().endswith(".pdf"):
            found.append({
                "file_id": item["id"],
                "name": name,
                "parent_id": inbox_folder_id,
                "hint_name": "",
            })
    return found


def _download_drive_file(service, file_id: str, dst_path: str) -> None:
    from googleapiclient.http import MediaIoBaseDownload

    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    with open(dst_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def _move_drive_file(service, file_id: str, old_parent_id: str, new_parent_id: str) -> None:
    service.files().update(
        fileId=file_id,
        addParents=new_parent_id,
        removeParents=old_parent_id,
        fields="id, parents",
        supportsAllDrives=True,
    ).execute()


def _run_pdf_task(db: DatabaseManager, args) -> dict:
    service = build_gdrive_service()
    inbox_id = ensure_gdrive_folder(service, PDF_INBOX_FOLDER_NAME, CLOUD_FOLDER_ID)
    archive_root_id = ensure_gdrive_folder(service, PDF_ARCHIVE_FOLDER_NAME, CLOUD_FOLDER_ID)

    pdfs = _collect_drive_inbox_pdfs(service, inbox_id)
    logger.info(f"[PDF Inbox] 发现待解析 PDF {len(pdfs)} 份")
    if args.dry_run:
        return {"dry_run": True, "inbox_pdfs": pdfs}

    if not pdfs:
        return {"processed": 0, "message": "待解析PDF文件夹为空，无需同步"}

    month_folder_id = ensure_gdrive_folder(service, datetime.now().strftime("%Y-%m"), archive_root_id)
    unrecognized_folder_id: Optional[str] = None

    identifier = PDFSupplierDetector({"auto_detect_from_pdf_content": True}, db=db)
    images_dir = os.path.join(ROOT_DIR, "images")
    os.makedirs(images_dir, exist_ok=True)
    tmp_dir = tempfile.mkdtemp(prefix="cloud_pdfs_")

    results = []
    touched_suppliers = set()

    try:
        for item in pdfs:
            fid = item["file_id"]
            fname = item["name"]
            hint_name = item["hint_name"]
            local_pdf = os.path.join(tmp_dir, fname)

            logger.info(f"[PDF] 下载 {fname} (hint={hint_name or '-'})...")
            _download_drive_file(service, fid, local_pdf)

            ident = identifier.identify(local_pdf, filename=fname, chat_name=hint_name)
            if not ident:
                logger.warning(f"[PDF] 无法识别供应商: {fname}")
                if unrecognized_folder_id is None:
                    unrecognized_folder_id = ensure_gdrive_folder(service, "无法识别", inbox_id)
                _move_drive_file(service, fid, item["parent_id"], unrecognized_folder_id)
                results.append({"file": fname, "status": "unrecognized"})
                continue

            sid = ident["supplier_id"]
            sname = ident["supplier_name"]
            parser_id = ident["parser_id"]
            logger.info(f"[PDF] 识别成功 -> [{sid}] {sname} (parser={parser_id}, match={ident['match_source']})")

            try:
                res = import_pdf_to_supplier(
                    db=db,
                    pdf_path=local_pdf,
                    supplier_id=sid,
                    parser_id=parser_id,
                    images_dir=images_dir,
                    source_label="CloudDrivePDF",
                )
                touched_suppliers.add(sid)
                _move_drive_file(service, fid, item["parent_id"], month_folder_id)
                results.append({
                    "file": fname,
                    "status": "ok",
                    "supplier_id": sid,
                    "supplier_name": sname,
                    "parser_id": parser_id,
                    **res,
                })
            except Exception as e:
                logger.exception(f"[PDF] 解析入库失败 {fname}: {e}")
                results.append({
                    "file": fname,
                    "status": "error",
                    "supplier_id": sid,
                    "supplier_name": sname,
                    "error": str(e),
                })
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    cls_stats = {}
    for sid in sorted(touched_suppliers):
        cls_stats[sid] = classify_pending_products(db, supplier_id=sid)

    sync_res = None
    if touched_suppliers and not args.skip_sync:
        sync_res = run_full_cloud_sync(max_copy=args.max_copy, db=db)

    ok_cnt = sum(1 for r in results if r["status"] == "ok")
    lines = [
        f"Google Drive 自动解析 PDF 完成 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})",
        f"总计 {len(results)} 份 | 成功 {ok_cnt} 份",
        "",
    ]
    for r in results:
        if r["status"] == "ok":
            lines.append(f"- ✅ {r['file']}: {r.get('message', 'OK')}")
        else:
            lines.append(f"- ❌ {r['file']}: {r['status']} {r.get('error', '')}")
    if sync_res:
        lines.append(f"\nCatalog 版本已更新: v{sync_res.get('bundle', {}).get('version')}")

    _notify_safe(f"[云端PDF解析] {ok_cnt}/{len(results)} 份完成", "\n".join(lines))
    return {"pdfs": results, "classification": cls_stats, "sync": sync_res}


def _run_sync_task(db: DatabaseManager, args) -> dict:
    if args.dry_run:
        return {"dry_run": True, "task": "sync"}
    cls_stats = classify_pending_products(db)
    sync_res = run_full_cloud_sync(max_copy=args.max_copy, db=db)
    return {"classification": cls_stats, "sync": sync_res}


def main():
    parser = argparse.ArgumentParser(description="Headless cloud runner for GitHub Actions")
    parser.add_argument("--task", required=True, choices=["crawl", "pdf", "sync"])
    parser.add_argument("--suppliers", default="default", help="Comma-separated supplier IDs or 'default'")
    parser.add_argument("--max-copy", type=int, default=2000)
    parser.add_argument("--skip-sync", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_env_file()
    db = DatabaseManager.from_env(default_host="localhost")

    if args.task == "crawl":
        out = asyncio.run(_run_crawl_task(db, args))
    elif args.task == "pdf":
        out = _run_pdf_task(db, args)
    else:
        out = _run_sync_task(db, args)

    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
