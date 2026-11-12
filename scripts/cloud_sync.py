# -*- coding: utf-8 -*-
"""
scripts/cloud_sync.py
Unified incremental cloud sync for supplier_crawler supporting three backends:
1. Cloudflare R2 (`R2_ENDPOINT`, `R2_BUCKET`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_PUBLIC_URL`)
   - Compresses images (max 800px RGB JPEG q78) to fit cleanly within R2's 10 GB free tier
   - Marks synced items with `did = "@r2"` and sets `r2_base` in `catalog_db.dat`
2. Google Drive API v3 (`GDRIVE_CLIENT_ID`, `GDRIVE_CLIENT_SECRET`, `GDRIVE_REFRESH_TOKEN` or `GDRIVE_SERVICE_ACCOUNT_JSON`)
   - Headless upload/harvest in GitHub Actions without DriveFS desktop client
   - Updates `catalog_db.dat` and `catalog_meta.json` in-place via `GDRIVE_DAT_FILE_ID` / `GDRIVE_FOLDER_ID`
3. Local Windows DriveFS fallback (`G:\我的云端硬盘\Supplier_Crawler_Cloud`)
"""
import argparse
import base64
import gzip
import io
import json
import os
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime
from typing import Dict, Optional, Set, Tuple

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from database.db_manager import DatabaseManager

CLOUD_DIR = os.getenv("CLOUD_DIR", r"G:\我的云端硬盘\Supplier_Crawler_Cloud")
CLOUD_IMG_DIR = os.path.join(CLOUD_DIR, "images")
DRIVEFS_DB_PATH = os.getenv(
    "DRIVEFS_DB_PATH",
    r"C:\Users\Administrator\AppData\Local\Google\DriveFS\110723887289889288645\metadata_sqlite_db",
)
CLOUD_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "1uM4bqickgGQjoAf7v1cdlJ16mDhMDvxP")
DEFAULT_DAT_FILE_ID = os.getenv("GDRIVE_DAT_FILE_ID", "1Wywu6NtTkiY5gUztbsmjoPuey8JvRLEC")


def load_env_file(env_path: Optional[str] = None) -> None:
    """Populate missing os.environ entries from local .env file if it exists."""
    path = env_path or os.path.join(ROOT_DIR, ".env")
    if not os.path.isfile(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def get_db() -> DatabaseManager:
    load_env_file()
    return DatabaseManager.from_env(default_host="localhost")


def resolve_local_image_path(image_local: str, image_url: str) -> str:
    """Resolve existing disk image path from products.image_local / products.image_url."""
    for rel in (image_local, image_url):
        if not rel or rel.startswith("http://") or rel.startswith("https://"):
            continue
        clean_rel = rel.lstrip("/\\").replace("/", os.sep)
        full_path = os.path.join(ROOT_DIR, clean_rel)
        if os.path.isfile(full_path):
            return full_path
    return ""


def optimize_image_bytes(src_path: str, max_side: int = 800, quality: int = 78) -> Tuple[bytes, str]:
    """
    Downscale and compress images to compact RGB JPEG bytes for cloud storage.
    Falls back cleanly to original file bytes if Pillow is unavailable or decode fails.
    """
    try:
        from PIL import Image, ImageOps

        with Image.open(src_path) as im:
            im = ImageOps.exif_transpose(im)
            if im.mode in ("RGBA", "LA", "P"):
                bg = Image.new("RGB", im.size, (255, 255, 255))
                if im.mode == "P":
                    im = im.convert("RGBA")
                bg.paste(im, mask=im.split()[-1] if im.mode in ("RGBA", "LA") else None)
                im = bg
            elif im.mode != "RGB":
                im = im.convert("RGB")

            if max(im.size) > max_side:
                im.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)

            buf = io.BytesIO()
            im.save(buf, format="JPEG", quality=quality, optimize=True)
            return buf.getvalue(), "image/jpeg"
    except Exception:
        with open(src_path, "rb") as f:
            return f.read(), "image/jpeg"


# ---------------------------------------------------------------------------
# Backend 1: Local Windows DriveFS (existing behavior preserved)
# ---------------------------------------------------------------------------
def harvest_drive_file_ids(db: DatabaseManager) -> int:
    """Read synced p_{id}.jpg Google Drive file IDs from local DriveFS SQLite cache."""
    if not os.path.exists(DRIVEFS_DB_PATH):
        return 0

    tmp_db = tempfile.mktemp(suffix=".db")
    try:
        shutil.copy2(DRIVEFS_DB_PATH, tmp_db)
        con = sqlite3.connect(tmp_db)
        cur = con.cursor()
        cur.execute("""
            SELECT local_title, id
            FROM items
            WHERE trashed = 0
              AND is_folder = 0
              AND local_title LIKE 'p_%'
              AND id NOT LIKE 'local-%'
        """)
        rows = cur.fetchall()
        con.close()
    finally:
        if os.path.exists(tmp_db):
            try:
                os.remove(tmp_db)
            except OSError:
                pass

    id_map = []
    for title, cloud_id in rows:
        stem = os.path.splitext(title)[0]
        if stem.startswith("p_"):
            pid_str = stem[2:]
            if pid_str.isdigit():
                id_map.append((cloud_id, int(pid_str)))

    return _apply_drive_id_updates(db, id_map)


def _apply_drive_id_updates(db: DatabaseManager, id_map: list) -> int:
    if not id_map:
        return 0
    dedup: Dict[int, str] = {}
    for cid, pid in id_map:
        if cid and pid:
            dedup[int(pid)] = str(cid)
    if not dedup:
        return 0

    with db.connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, drive_file_id FROM products")
        existing = {int(pid): (did or "") for pid, did in cursor.fetchall()}

        pending = [
            (pid, cid)
            for pid, cid in dedup.items()
            if pid in existing and existing[pid] != cid
        ]
        if not pending:
            cursor.close()
            return 0

        updated = 0
        batch_size = 500
        for i in range(0, len(pending), batch_size):
            batch = pending[i:i + batch_size]
            cases = " ".join(["WHEN %s THEN %s"] * len(batch))
            placeholders = ", ".join(["%s"] * len(batch))
            params = []
            for pid, cid in batch:
                params.extend([pid, cid])
            params.extend([pid for pid, _ in batch])
            cursor.execute(
                f"UPDATE products SET drive_file_id = CASE id {cases} END WHERE id IN ({placeholders})",
                params,
            )
            updated += max(cursor.rowcount, 0)
        cursor.close()
    return updated


def copy_missing_images_to_drive(db: DatabaseManager, max_copy: int = 2000) -> dict:
    """Copy local product images missing drive_file_id into G:\\ DriveFS sync directory."""
    os.makedirs(CLOUD_IMG_DIR, exist_ok=True)
    with db.connection() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT id, supplier_id, sku, name, price, category, image_url, image_local
            FROM products
            WHERE is_available = 1
              AND (drive_file_id IS NULL OR drive_file_id = '')
              AND (image_local != '' OR image_url LIKE '/images/%')
            ORDER BY (image_url LIKE 'http%') ASC, id DESC
        """)
        candidates = cur.fetchall()
        cur.close()

    copied = 0
    already_in_g = 0
    missing_disk = 0

    for p in candidates:
        if copied >= max_copy:
            break
        sup_dir = os.path.join(CLOUD_IMG_DIR, str(p["supplier_id"]))
        os.makedirs(sup_dir, exist_ok=True)
        target_file = os.path.join(sup_dir, f"p_{p['id']}.jpg")
        if os.path.exists(target_file):
            already_in_g += 1
            continue
        src_path = resolve_local_image_path(p.get("image_local") or "", p.get("image_url") or "")
        if not src_path:
            missing_disk += 1
            continue
        shutil.copy2(src_path, target_file)
        copied += 1

    return {
        "candidates": len(candidates),
        "copied_now": copied,
        "already_in_gdrive_folder": already_in_g,
        "missing_on_local_disk": missing_disk,
    }


# ---------------------------------------------------------------------------
# Backend 2: Google Drive API v3 (OAuth2 Refresh Token OR Service Account)
# ---------------------------------------------------------------------------
def is_gdrive_api_configured() -> bool:
    has_oauth = all(
        os.getenv(k, "").strip()
        for k in ("GDRIVE_CLIENT_ID", "GDRIVE_CLIENT_SECRET", "GDRIVE_REFRESH_TOKEN")
    )
    has_sa = bool(os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON", "").strip()) or bool(
        os.getenv("GDRIVE_SERVICE_ACCOUNT_FILE", "").strip()
    )
    return has_oauth or has_sa


def build_gdrive_service():
    """Authenticate Google Drive v3 API using OAuth refresh token or service account."""
    from googleapiclient.discovery import build

    scopes = ["https://www.googleapis.com/auth/drive"]
    sa_raw = os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON", "").strip()
    sa_file = os.getenv("GDRIVE_SERVICE_ACCOUNT_FILE", "").strip()

    if sa_raw or sa_file:
        from google.oauth2 import service_account

        if sa_raw.startswith("{"):
            info = json.loads(sa_raw)
            creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        else:
            creds = service_account.Credentials.from_service_account_file(sa_file or sa_raw, scopes=scopes)
        return build("drive", "v3", credentials=creds, cache_discovery=False)

    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials

    creds = Credentials(
        token=None,
        refresh_token=os.getenv("GDRIVE_REFRESH_TOKEN", "").strip(),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.getenv("GDRIVE_CLIENT_ID", "").strip(),
        client_secret=os.getenv("GDRIVE_CLIENT_SECRET", "").strip(),
        scopes=scopes,
    )
    creds.refresh(Request())
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _gdrive_find_child(service, name: str, parent_id: str, is_folder: Optional[bool] = None) -> Optional[str]:
    safe_name = name.replace("'", "\\'")
    clauses = [f"name = '{safe_name}'", f"'{parent_id}' in parents", "trashed = false"]
    if is_folder is True:
        clauses.append("mimeType = 'application/vnd.google-apps.folder'")
    elif is_folder is False:
        clauses.append("mimeType != 'application/vnd.google-apps.folder'")
    resp = service.files().list(
        q=" and ".join(clauses),
        fields="files(id, name)",
        pageSize=5,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def ensure_gdrive_folder(service, name: str, parent_id: str) -> str:
    existing = _gdrive_find_child(service, name, parent_id, is_folder=True)
    if existing:
        return existing
    meta = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    created = service.files().create(body=meta, fields="id", supportsAllDrives=True).execute()
    return created["id"]


def _gdrive_list_children(service, parent_id: str, extra_q: str = "") -> list:
    items = []
    page_token = None
    q = f"'{parent_id}' in parents and trashed = false"
    if extra_q:
        q += f" and {extra_q}"
    while True:
        resp = service.files().list(
            q=q,
            fields="nextPageToken, files(id, name, mimeType)",
            pageSize=1000,
            pageToken=page_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        items.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return items


def upsert_gdrive_file(
    service,
    local_path: str,
    filename: str,
    parent_id: str,
    mime_type: str = "application/octet-stream",
    fixed_file_id: str = "",
) -> str:
    from googleapiclient.http import MediaFileUpload

    media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)
    target_id = (fixed_file_id or "").strip() or _gdrive_find_child(service, filename, parent_id, is_folder=False)
    if target_id:
        try:
            updated = service.files().update(
                fileId=target_id,
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute()
            return updated["id"]
        except Exception:
            if fixed_file_id:
                # Fallback to name lookup/create if fixed ID doesn't belong to this account
                fallback_id = _gdrive_find_child(service, filename, parent_id, is_folder=False)
                if fallback_id:
                    media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)
                    updated = service.files().update(
                        fileId=fallback_id, media_body=media, fields="id", supportsAllDrives=True
                    ).execute()
                    return updated["id"]
            else:
                raise
    media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)
    created = service.files().create(
        body={"name": filename, "parents": [parent_id]},
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return created["id"]


def sync_images_via_gdrive_api(db: DatabaseManager, service, folder_id: str, max_copy: int = 1000) -> dict:
    """
    1. Scan `Supplier_Crawler_Cloud/images/{supplier_id}/p_*.jpg` via Drive API and backfill DB drive_file_id.
    2. Upload missing local-disk product images via Drive API (compressed <=800px JPEG) and update DB.
    """
    from googleapiclient.http import MediaIoBaseUpload

    images_root_id = ensure_gdrive_folder(service, "images", folder_id)
    subfolders = _gdrive_list_children(
        service, images_root_id, "mimeType = 'application/vnd.google-apps.folder'"
    )
    sup_folder_ids: Dict[str, str] = {f["name"]: f["id"] for f in subfolders}
    existing_drive_files: Dict[int, str] = {}

    for _, sf_id in sup_folder_ids.items():
        for f in _gdrive_list_children(service, sf_id, "mimeType != 'application/vnd.google-apps.folder'"):
            stem = os.path.splitext(f["name"])[0]
            if stem.startswith("p_") and stem[2:].isdigit():
                existing_drive_files[int(stem[2:])] = f["id"]

    harvested = _apply_drive_id_updates(
        db, [(cid, pid) for pid, cid in existing_drive_files.items()]
    )

    with db.connection() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT id, supplier_id, image_url, image_local
            FROM products
            WHERE is_available = 1
              AND (drive_file_id IS NULL OR drive_file_id = '')
              AND (image_local != '' OR image_url LIKE '/images/%')
            ORDER BY (image_url LIKE 'http%') ASC, id DESC
        """)
        candidates = cur.fetchall()
        cur.close()

    uploaded_now = 0
    already_remote = 0
    missing_disk = 0
    new_id_map = []

    for p in candidates:
        pid = int(p["id"])
        if pid in existing_drive_files:
            already_remote += 1
            continue
        if uploaded_now >= max_copy:
            break
        src_path = resolve_local_image_path(p.get("image_local") or "", p.get("image_url") or "")
        if not src_path:
            missing_disk += 1
            continue

        sid_str = str(p["supplier_id"])
        if sid_str not in sup_folder_ids:
            sup_folder_ids[sid_str] = ensure_gdrive_folder(service, sid_str, images_root_id)

        img_bytes, mime = optimize_image_bytes(src_path)
        media = MediaIoBaseUpload(io.BytesIO(img_bytes), mimetype=mime, resumable=False)
        created = service.files().create(
            body={"name": f"p_{pid}.jpg", "parents": [sup_folder_ids[sid_str]]},
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        new_id_map.append((created["id"], pid))
        uploaded_now += 1

    updated_new = _apply_drive_id_updates(db, new_id_map)
    return {
        "harvested_existing": harvested,
        "candidates": len(candidates),
        "uploaded_now": uploaded_now,
        "updated_db_ids": updated_new,
        "already_in_gdrive": already_remote,
        "missing_on_local_disk": missing_disk,
    }


# ---------------------------------------------------------------------------
# Backend 3: Cloudflare R2 (S3-compatible, 10 GB free storage, zero egress fee)
# ---------------------------------------------------------------------------
def is_r2_configured() -> bool:
    return all(
        os.getenv(k, "").strip()
        for k in ("R2_ENDPOINT", "R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY")
    )


def build_r2_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT", "").strip(),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID", "").strip(),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY", "").strip(),
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )


def list_r2_image_keys(s3, bucket: str) -> Tuple[Set[str], Set[int]]:
    """List existing `images/{sid}/p_{id}.jpg` keys in Cloudflare R2 bucket."""
    keys: Set[str] = set()
    pids: Set[int] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="images/"):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            keys.add(k)
            fname = k.rsplit("/", 1)[-1]
            stem = os.path.splitext(fname)[0]
            if stem.startswith("p_") and stem[2:].isdigit():
                pids.add(int(stem[2:]))
    return keys, pids


def sync_images_to_r2(db: DatabaseManager, s3, bucket: str, max_copy: int = 2000) -> Tuple[dict, Set[int]]:
    """Upload missing local images (compressed JPEG <=800px) to Cloudflare R2."""
    existing_keys, r2_pids = list_r2_image_keys(s3, bucket)
    upload_scope = os.getenv("R2_UPLOAD_SCOPE", "local_only").strip().lower()

    extra_filter = "" if upload_scope == "all" else "AND (image_url IS NULL OR image_url NOT LIKE 'http%')"
    with db.connection() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT id, supplier_id, image_url, image_local
            FROM products
            WHERE is_available = 1
              AND (image_local != '' OR image_url LIKE '/images/%')
              {extra_filter}
            ORDER BY (image_url LIKE 'http%') ASC, id DESC
        """)
        candidates = cur.fetchall()
        cur.close()

    uploaded = 0
    already_in_r2 = 0
    missing_disk = 0

    for p in candidates:
        pid = int(p["id"])
        sid = int(p["supplier_id"])
        key = f"images/{sid}/p_{pid}.jpg"
        if key in existing_keys:
            already_in_r2 += 1
            continue
        if uploaded >= max_copy:
            break
        src_path = resolve_local_image_path(p.get("image_local") or "", p.get("image_url") or "")
        if not src_path:
            missing_disk += 1
            continue

        body, ctype = optimize_image_bytes(src_path)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType=ctype,
            CacheControl="public, max-age=2592000",
        )
        existing_keys.add(key)
        r2_pids.add(pid)
        uploaded += 1

    return (
        {
            "scope": upload_scope,
            "candidates": len(candidates),
            "existing_in_r2": len(r2_pids),
            "uploaded_now": uploaded,
            "already_in_r2": already_in_r2,
            "missing_on_local_disk": missing_disk,
        },
        r2_pids,
    )


# ---------------------------------------------------------------------------
# Catalog Bundle Export & Dispatch
# ---------------------------------------------------------------------------
def export_catalog_bundle(
    db: DatabaseManager,
    output_dir: Optional[str] = None,
    r2_public_url: str = "",
    r2_pids: Optional[Set[int]] = None,
) -> dict:
    """
    Export compact catalog_db.dat + catalog_meta.json + image_descriptions.json (+ cloud_state.json).
    Preserves columnar schema: [0:id, 1:sid, 2:sku, 3:name, 4:cat, 5:price, 6:pkg, 7:avail, 8:did, 9:img_http, 10:purl, 11:ts]
    """
    target_dir = output_dir or CLOUD_DIR
    os.makedirs(target_dir, exist_ok=True)

    with db.connection() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT id, name, base_url, price_adjustment, is_active FROM suppliers WHERE is_active = 1 ORDER BY id")
        suppliers = cur.fetchall()

        cur.execute("""
            SELECT id, supplier_id, sku, name, category, price, unit, min_qty, package_qty,
                   image_url, product_url, is_available, is_missing, stock, drive_file_id, updated_at
            FROM products
            ORDER BY supplier_id, id DESC
        """)
        products = cur.fetchall()

        cur.execute("SELECT product_id, quantity FROM cart_items")
        local_cart = {str(r["product_id"]): int(r["quantity"]) for r in cur.fetchall()}

        cur.execute("SELECT product_id FROM favorites")
        local_favs = [int(r["product_id"]) for r in cur.fetchall()]
        cur.close()

    fields = ["id", "sid", "sku", "name", "cat", "price", "pkg", "avail", "did", "img_http", "purl", "ts"]
    rows = []
    desc_items = []

    sup_map = {s["id"]: s["name"] for s in suppliers}
    clean_r2_base = (r2_public_url or "").strip().rstrip("/")
    r2_pid_set = r2_pids or set()

    for p in products:
        pid = int(p["id"])
        img_u = p["image_url"] or ""
        http_img = img_u if img_u.startswith("http") else ""
        orig_did = p["drive_file_id"] or ""
        did = "@r2" if (clean_r2_base and pid in r2_pid_set) else orig_did
        price_val = float(p["price"] or 0)

        is_missing = int(p.get("is_missing") or 0)
        stock_val = p.get("stock")
        is_avail = p.get("is_available")
        if price_val <= 0 or is_missing == 1 or (stock_val is not None and stock_val <= 0) or is_avail == 0:
            avail_flag = 0
        else:
            avail_flag = 1

        upd = p.get("updated_at")
        upd_str = upd.strftime("%Y-%m-%d %H:%M") if hasattr(upd, "strftime") else str(upd or "")[:16]

        rows.append([
            pid,
            int(p["supplier_id"]),
            p["sku"] or "",
            p["name"] or "",
            p["category"] or "",
            price_val,
            int(p["package_qty"] or 1),
            avail_flag,
            did,
            http_img,
            p["product_url"] or "",
            upd_str,
        ])
        if orig_did:
            desc_items.append({
                "did": orig_did,
                "desc": f"SKU: {p['sku'] or '-'} | 店铺: {sup_map.get(p['supplier_id'], '')} | 价格: ${price_val:,.2f} | 品名: {p['name']} | 分类: {p['category'] or '-'}",
            })

    version_ts = int(datetime.now().timestamp())
    payload = {
        "version": version_ts,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "folder_id": CLOUD_FOLDER_ID,
        "suppliers": [
            {
                "id": int(s["id"]),
                "name": s["name"],
                "adj": float(s["price_adjustment"] or 0),
            }
            for s in suppliers
        ],
        "fields": fields,
        "rows": rows,
    }
    if clean_r2_base:
        payload["r2_base"] = clean_r2_base

    raw_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    compressed = gzip.compress(raw_json, compresslevel=9)
    b64_data = base64.b64encode(compressed).decode("ascii")

    dat_path = os.path.join(target_dir, "catalog_db.dat")
    with open(dat_path, "w", encoding="ascii") as f:
        f.write(b64_data)

    meta_path = os.path.join(target_dir, "catalog_meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "version": version_ts,
                "generated_at": payload["generated_at"],
                "total_products": len(rows),
                "in_stock_products": sum(1 for r in rows if r[7] == 1),
                "out_of_stock_products": sum(1 for r in rows if r[7] == 0),
                "total_suppliers": len(suppliers),
                "r2_images": sum(1 for r in rows if r[8] == "@r2"),
                "dat_size_kb": round(len(b64_data) / 1024, 1),
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    desc_path = os.path.join(target_dir, "image_descriptions.json")
    with open(desc_path, "w", encoding="utf-8") as f:
        json.dump(desc_items[:3000], f, ensure_ascii=False)

    state_path = os.path.join(target_dir, "cloud_state.json")
    if not os.path.exists(state_path):
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "updated_at": payload["generated_at"],
                    "cart": local_cart,
                    "favorites": local_favs,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )

    return {
        "version": version_ts,
        "total_products": len(rows),
        "products_with_drive_id": len(desc_items),
        "r2_linked_products": sum(1 for r in rows if r[8] == "@r2"),
        "dat_size_kb": round(len(b64_data) / 1024, 1),
        "dat_path": dat_path,
        "meta_path": meta_path,
        "desc_path": desc_path,
        "state_path": state_path,
    }


def _detect_backends() -> dict:
    load_env_file()
    mode_override = os.getenv("CLOUD_SYNC_MODE", "").strip().lower()
    if mode_override:
        tokens = {t.strip() for t in mode_override.replace(",", "+").split("+") if t.strip()}
        return {
            "r2": "r2" in tokens and is_r2_configured(),
            "gdrive_api": "gdrive_api" in tokens and is_gdrive_api_configured(),
            "local_drivefs": "local_drivefs" in tokens or "local" in tokens,
        }
    use_r2 = is_r2_configured()
    use_gdrive_api = is_gdrive_api_configured()
    use_local = (not use_r2 and not use_gdrive_api) or os.path.exists(DRIVEFS_DB_PATH)
    return {
        "r2": use_r2,
        "gdrive_api": use_gdrive_api,
        "local_drivefs": use_local,
    }


def run_full_cloud_sync(
    max_copy: int = 2000,
    db: Optional[DatabaseManager] = None,
    init_schema: bool = False,
    bundle_only: bool = False,
) -> dict:
    """
    Sync images + regenerate catalog_db.dat + publish to configured cloud backend(s).
    Compatible with local Windows DriveFS, Google Drive API v3, and Cloudflare R2.
    """
    load_env_file()
    db_conn = db or get_db()
    if init_schema:
        db_conn.init_schema()

    backends = _detect_backends()
    result = {"backends": backends}

    s3 = None
    r2_bucket = os.getenv("R2_BUCKET", "").strip()
    r2_public_url = os.getenv("R2_PUBLIC_URL", "").strip()
    r2_pids: Set[int] = set()

    if backends["r2"]:
        s3 = build_r2_client()
        if bundle_only:
            _, r2_pids = list_r2_image_keys(s3, r2_bucket)
            result["r2_images"] = {"skipped_upload": True, "existing_in_r2": len(r2_pids)}
        else:
            r2_stats, r2_pids = sync_images_to_r2(db_conn, s3, r2_bucket, max_copy=max_copy)
            result["r2_images"] = r2_stats

    gdrive_service = None
    if backends["gdrive_api"]:
        gdrive_service = build_gdrive_service()
        # If R2 is also enabled, R2 hosts images so GDrive API only needs to update catalog_db.dat
        if not bundle_only and not backends["r2"]:
            result["gdrive_api_images"] = sync_images_via_gdrive_api(
                db_conn, gdrive_service, CLOUD_FOLDER_ID, max_copy=max_copy
            )

    local_drive_writable = os.path.isdir(os.path.dirname(CLOUD_DIR)) or os.path.isdir(CLOUD_DIR)
    if backends["local_drivefs"] and local_drive_writable:
        harvested_before = harvest_drive_file_ids(db_conn)
        if bundle_only:
            result["local_image_copy"] = {"skipped_copy": True}
        else:
            result["local_image_copy"] = copy_missing_images_to_drive(db_conn, max_copy=max_copy)
        harvested_after = harvest_drive_file_ids(db_conn)
        result["harvested_drive_ids"] = harvested_before + harvested_after

    tmp_bundle_dir = None
    if local_drive_writable:
        output_dir = CLOUD_DIR
    else:
        tmp_bundle_dir = tempfile.mkdtemp(prefix="catalog_bundle_")
        output_dir = tmp_bundle_dir

    try:
        bundle_stats = export_catalog_bundle(
            db_conn,
            output_dir=output_dir,
            r2_public_url=r2_public_url if backends["r2"] else "",
            r2_pids=r2_pids if backends["r2"] else None,
        )
        result["bundle"] = bundle_stats

        if s3 is not None:
            with open(bundle_stats["dat_path"], "rb") as f:
                s3.put_object(
                    Bucket=r2_bucket,
                    Key="catalog_db.dat",
                    Body=f.read(),
                    ContentType="text/plain; charset=ascii",
                    CacheControl="no-cache",
                )
            with open(bundle_stats["meta_path"], "rb") as f:
                s3.put_object(
                    Bucket=r2_bucket,
                    Key="catalog_meta.json",
                    Body=f.read(),
                    ContentType="application/json",
                    CacheControl="no-cache",
                )
            result["r2_bundle_uploaded"] = True

        if gdrive_service is not None:
            dat_id = upsert_gdrive_file(
                gdrive_service,
                bundle_stats["dat_path"],
                "catalog_db.dat",
                CLOUD_FOLDER_ID,
                mime_type="text/plain",
                fixed_file_id=DEFAULT_DAT_FILE_ID,
            )
            meta_id = upsert_gdrive_file(
                gdrive_service,
                bundle_stats["meta_path"],
                "catalog_meta.json",
                CLOUD_FOLDER_ID,
                mime_type="application/json",
            )
            result["gdrive_bundle_uploaded"] = {"dat_file_id": dat_id, "meta_file_id": meta_id}
    finally:
        if tmp_bundle_dir and os.path.isdir(tmp_bundle_dir):
            shutil.rmtree(tmp_bundle_dir, ignore_errors=True)

    return result


def main():
    parser = argparse.ArgumentParser(description="Incremental cloud sync (R2 / Drive API / Local DriveFS)")
    parser.add_argument("--max-copy", type=int, default=1000)
    parser.add_argument("--bundle-only", action="store_true", help="Skip image uploads and regenerate bundle only")
    args = parser.parse_args()

    res = run_full_cloud_sync(max_copy=args.max_copy, bundle_only=args.bundle_only)
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
