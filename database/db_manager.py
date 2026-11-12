"""
supplier_crawler/database/db_manager.py
MySQL 数据库管理模块
"""
import json
import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

import mysql.connector
from mysql.connector import pooling

logger = logging.getLogger(__name__)

SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS suppliers (
        id            INT AUTO_INCREMENT PRIMARY KEY,
        name          VARCHAR(100) NOT NULL UNIQUE,
        base_url      VARCHAR(500) NOT NULL,
        list_url      VARCHAR(500) NOT NULL,
        selectors     JSON         DEFAULT NULL,
        use_js        TINYINT(1)   DEFAULT 0,
        use_chrome_cookies TINYINT(1) DEFAULT 0,
        request_delay FLOAT        DEFAULT 1.5,
        cron_expr     VARCHAR(50)  DEFAULT '0 2 * * *',
        price_adjustment FLOAT DEFAULT 0,
        missing_threshold INT DEFAULT 80,
        is_active     TINYINT(1)   DEFAULT 1,
        created_at    DATETIME     DEFAULT CURRENT_TIMESTAMP,
        updated_at    DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_active (is_active)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS products (
        id            BIGINT AUTO_INCREMENT PRIMARY KEY,
        supplier_id   INT          NOT NULL,
        name          VARCHAR(300) NOT NULL,
        sku           VARCHAR(100) DEFAULT '',
        category      VARCHAR(100) DEFAULT '',
        brand         VARCHAR(100) DEFAULT '',
        price         DECIMAL(12,2) NOT NULL,
        currency      VARCHAR(10)  DEFAULT 'ARS',
        unit          VARCHAR(50)  DEFAULT '',
        min_qty       INT          DEFAULT 1,
        package_qty   INT          DEFAULT 1,
        stock         INT          DEFAULT NULL,
        description   TEXT,
        image_url     VARCHAR(1000) DEFAULT '',
        image_local   VARCHAR(500)  DEFAULT '',
        product_url   VARCHAR(1000) DEFAULT NULL,
        is_available  TINYINT(1)   DEFAULT 1,
        created_at    DATETIME     DEFAULT CURRENT_TIMESTAMP,
        updated_at    DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_supplier (supplier_id),
        INDEX idx_category (category),
        INDEX idx_price    (price),
        INDEX idx_available (is_available),
        FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS price_history (
        id          BIGINT AUTO_INCREMENT PRIMARY KEY,
        product_id  BIGINT        NOT NULL,
        price       DECIMAL(12,2) NOT NULL,
        recorded_at DATETIME      DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_product_time (product_id, recorded_at),
        FOREIGN KEY (product_id) REFERENCES products(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS crawl_logs (
        id             INT AUTO_INCREMENT PRIMARY KEY,
        supplier_id    INT  NOT NULL,
        status         ENUM('running','success','failed','paused') DEFAULT 'running',
        pages_crawled  INT  DEFAULT 0,
        products_found INT  DEFAULT 0,
        errors         INT  DEFAULT 0,
        started_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
        finished_at    DATETIME DEFAULT NULL,
        duration_sec   INT  DEFAULT NULL,
        message        TEXT DEFAULT NULL,
        INDEX idx_supplier_time (supplier_id, started_at),
        FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS favorites (
        id          BIGINT AUTO_INCREMENT PRIMARY KEY,
        product_id  BIGINT       NOT NULL,
        saved_at    DATETIME     DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_product (product_id),
        FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS cart_items (
        id          BIGINT AUTO_INCREMENT PRIMARY KEY,
        product_id  BIGINT       NOT NULL,
        quantity    INT          NOT NULL DEFAULT 1,
        created_at  DATETIME     DEFAULT CURRENT_TIMESTAMP,
        updated_at  DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_cart_product (product_id),
        FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS system_settings (
        `key`       VARCHAR(100) PRIMARY KEY,
        `value`     TEXT         NOT NULL,
        updated_at  DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


SCHEMA_SQL.append("""
    CREATE TABLE IF NOT EXISTS product_categories (
        id         BIGINT AUTO_INCREMENT PRIMARY KEY,
        product_id BIGINT       NOT NULL,
        category   VARCHAR(100) NOT NULL,
        created_at DATETIME     DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_product_category (product_id, category),
        INDEX idx_product (product_id),
        INDEX idx_category (category),
        FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""")


def _resolve_ssl_options(host: str, ssl_ca: str = None, use_ssl: bool = None) -> dict:
    """Build SSL kwargs for MySQL/TiDB Cloud connections when TLS is requested or required."""
    import os

    env_ssl = os.getenv("DB_USE_SSL", "").strip().lower()
    env_ca = os.getenv("DB_SSL_CA", "").strip()
    ca_path = ssl_ca or env_ca or None

    if use_ssl is None:
        if env_ssl in ("1", "true", "yes"):
            use_ssl = True
        elif env_ssl in ("0", "false", "no"):
            use_ssl = False
        else:
            use_ssl = bool(ca_path) or ("tidbcloud" in (host or "").lower())

    if not use_ssl:
        return {}

    if not ca_path:
        try:
            import certifi
            ca_path = certifi.where()
        except ImportError:
            for candidate in (
                "/etc/ssl/certs/ca-certificates.crt",
                "/etc/pki/tls/certs/ca-bundle.crt",
            ):
                if os.path.exists(candidate):
                    ca_path = candidate
                    break

    opts = {"ssl_verify_cert": bool(ca_path)}
    if ca_path:
        opts["ssl_ca"] = ca_path
    return opts


class DatabaseManager:
    def __init__(self, host, port, user, password, database, ssl_ca=None, use_ssl=None):
        import os

        ssl_opts = _resolve_ssl_options(host, ssl_ca=ssl_ca, use_ssl=use_ssl)
        self.config = dict(host=host, port=port, user=user, password=password, database=database, **ssl_opts)

        host_norm = (host or "").strip().lower()
        is_local = host_norm in ("127.0.0.1", "localhost", "::1", "")
        pool_size = max(1, int(os.getenv("DB_POOL_SIZE", "5" if is_local else "1")))
        connect_timeout = max(5, int(os.getenv("DB_CONNECT_TIMEOUT", "15")))
        read_timeout = max(10, int(os.getenv("DB_READ_TIMEOUT", "60")))
        write_timeout = max(10, int(os.getenv("DB_WRITE_TIMEOUT", "60")))

        self._conn_kwargs = dict(
            **self.config,
            charset="utf8mb4",
            use_unicode=True,
            autocommit=False,
            use_pure=True,
            connection_timeout=connect_timeout,
            read_timeout=read_timeout,
            write_timeout=write_timeout,
        )

        ssl_label = " [TLS]" if ssl_opts else ""
        print(
            f"[DB] 连接 {host}:{port} 数据库={database}{ssl_label} "
            f"(pool_size={pool_size}, read_timeout={read_timeout}s) ...",
            flush=True,
        )
        try:
            self.pool = pooling.MySQLConnectionPool(
                pool_name="crawler_pool",
                pool_size=pool_size,
                pool_reset_session=False,
                **self._conn_kwargs,
            )
        except (AttributeError, TypeError, ValueError):
            self._conn_kwargs.pop("read_timeout", None)
            self._conn_kwargs.pop("write_timeout", None)
            self.pool = pooling.MySQLConnectionPool(
                pool_name="crawler_pool",
                pool_size=pool_size,
                pool_reset_session=False,
                **self._conn_kwargs,
            )

        with self.connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
        print("[DB] 连接池就绪", flush=True)

    @classmethod
    def from_env(
        cls,
        default_host: str = "127.0.0.1",
        default_port: int = 3306,
        default_user: str = "root",
        default_password: str = "",
        default_database: str = "supplier_crawler",
    ) -> "DatabaseManager":
        """Create a DatabaseManager from DB_* environment variables (with local fallback)."""
        import os

        host = os.getenv("DB_HOST", default_host).strip() or default_host
        port = int(os.getenv("DB_PORT", str(default_port)))
        user = os.getenv("DB_USER", default_user).strip() or default_user
        password = os.getenv("DB_PASSWORD", default_password)
        database = os.getenv("DB_NAME", default_database).strip() or default_database
        ssl_ca = os.getenv("DB_SSL_CA", "").strip() or None
        return cls(host, port, user, password, database, ssl_ca=ssl_ca)

    @contextmanager
    def connection(self):
        import time as _time

        conn = None
        for attempt in range(5):
            try:
                conn = self.pool.get_connection()
                break
            except pooling.PoolError:
                if attempt < 4:
                    _time.sleep(0.1 * (attempt + 1))
                else:
                    conn = mysql.connector.connect(**self._conn_kwargs)
        try:
            conn.ping(reconnect=True, attempts=2, delay=1)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            conn = mysql.connector.connect(**self._conn_kwargs)
        try:
            yield conn
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def init_schema(self):
        with self.connection() as conn:
            cursor = conn.cursor()
            for stmt in SCHEMA_SQL:
                cursor.execute(stmt.strip())
        # 迁移：补充字段
        for sql in [
            "ALTER TABLE suppliers ADD COLUMN cron_expr VARCHAR(50) DEFAULT '0 2 * * *'",
            "ALTER TABLE suppliers ADD COLUMN price_adjustment FLOAT DEFAULT 0",
            "ALTER TABLE suppliers ADD COLUMN use_chrome_cookies TINYINT(1) DEFAULT 0",
            "ALTER TABLE products ADD COLUMN unit VARCHAR(50) DEFAULT ''",
            "ALTER TABLE products ADD COLUMN min_qty INT DEFAULT 1",
            "ALTER TABLE products ADD COLUMN package_qty INT DEFAULT 1",
            "ALTER TABLE products ADD COLUMN sub_pack_qty INT DEFAULT 1",
            "ALTER TABLE products ADD COLUMN drive_file_id VARCHAR(64) DEFAULT NULL",
            "ALTER TABLE products ADD INDEX idx_drive_file_id (drive_file_id)",
        ]:
            try:
                with self.connection() as conn:
                    conn.cursor().execute(sql)
            except Exception:
                pass
        # 移除product_url UNIQUE约束
        try:
            with self.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS "
                    "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' "
                    "AND CONSTRAINT_NAME='product_url' AND CONSTRAINT_TYPE='UNIQUE'"
                )
                if cursor.fetchone()[0]:
                    cursor.execute("ALTER TABLE products DROP INDEX product_url")
        except Exception:
            pass
        # 迁移：加 is_new 字段
        try:
            with self.connection() as conn:
                conn.cursor().execute("ALTER TABLE products ADD COLUMN is_new TINYINT(1) DEFAULT 0")
        except Exception:
            pass
        # 迁移：加 is_missing 字段
        try:
            with self.connection() as conn:
                conn.cursor().execute("ALTER TABLE products ADD COLUMN is_missing TINYINT(1) DEFAULT 0")
        except Exception:
            pass
        # 迁移：加 idx_product_url 索引加速 upsert 查询
        try:
            with self.connection() as conn:
                conn.cursor().execute("CREATE INDEX idx_product_url ON products (product_url(255))")
        except Exception:
            pass
        # 默认系统设置
        try:
            with self.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT IGNORE INTO system_settings (`key`, `value`) VALUES ('mobile_retail_mult', '2.2')"
                )
        except Exception:
            pass
        logger.info("数据库初始化完成")

    # ── 供应商 ────────────────────────────────
    def upsert_supplier(self, name, base_url, list_url,
                        selectors=None, use_js=False, use_chrome_cookies=False,
                        request_delay=1.5, cron_expr="0 2 * * *", price_adjustment=0.0,
                        missing_threshold=80) -> int:
        sql = """
            INSERT INTO suppliers (name,base_url,list_url,selectors,use_js,use_chrome_cookies,
                                   request_delay,cron_expr,price_adjustment,missing_threshold)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                base_url=VALUES(base_url), list_url=VALUES(list_url),
                selectors=VALUES(selectors), use_js=VALUES(use_js),
                use_chrome_cookies=VALUES(use_chrome_cookies),
                request_delay=VALUES(request_delay), cron_expr=VALUES(cron_expr),
                price_adjustment=VALUES(price_adjustment),
                missing_threshold=VALUES(missing_threshold)
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (
                name, base_url, list_url,
                json.dumps(selectors, ensure_ascii=False) if selectors else None,
                int(use_js), int(use_chrome_cookies), request_delay, cron_expr,
                price_adjustment, missing_threshold
            ))
            sid = cursor.lastrowid
        return sid if sid else self._get_supplier_id(name)

    def _get_supplier_id(self, name) -> int:
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM suppliers WHERE name=%s", (name,))
            row = cursor.fetchone()
            return row[0] if row else 0

    def get_all_suppliers(self) -> list:
        with self.connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT s.*,
                    (SELECT COUNT(*) FROM products p WHERE p.supplier_id=s.id AND p.is_available=1) AS product_count,
                    (SELECT MAX(cl.started_at) FROM crawl_logs cl WHERE cl.supplier_id=s.id) AS last_crawl_at
                FROM suppliers s WHERE s.is_active=1 ORDER BY s.created_at DESC
            """)
            rows = cursor.fetchall()
        for r in rows:
            if isinstance(r.get("selectors"), str):
                try: r["selectors"] = json.loads(r["selectors"])
                except: r["selectors"] = {}
            for f in ("created_at", "updated_at", "last_crawl_at"):
                if isinstance(r.get(f), datetime):
                    r[f] = r[f].strftime("%Y-%m-%d %H:%M:%S")
        return rows

    # ── 商品 ──────────────────────────────────
    def upsert_product(self, product) -> int:
        product_url = (product.product_url or "").strip() or None

        with self.connection() as conn:
            cursor = conn.cursor(buffered=True)
            if product_url:
                cursor.execute("SELECT id, price, category FROM products WHERE product_url=%s LIMIT 1", (product_url,))
            else:
                cursor.execute("SELECT id, price, category FROM products WHERE supplier_id=%s AND name=%s LIMIT 1",
                               (product.supplier_id, product.name[:300]))
            existing = cursor.fetchone()

            if existing:
                pid, old_price = existing[0], float(existing[1])
                existing_category = existing[2] if len(existing) > 2 else ""
                new_price = float(product.price)
                # 已有分类不覆盖（保留手动设置的分类）
                keep_category = existing_category if existing_category and existing_category.strip() else (product.category or "")
                min_q = int(getattr(product, 'min_qty', 1) or 1)
                pkg_q = int(getattr(product, 'package_qty', 1) or 1)
                cursor.execute(
                    """UPDATE products SET name=%s,sku=%s,description=%s,price=%s,stock=%s,
                       image_url=%s,image_local=%s,category=%s,brand=%s,unit=%s,
                       min_qty=%s,package_qty=%s,
                       is_available=1,updated_at=NOW() WHERE id=%s""",
                    (product.name[:300], product.sku or "", (product.description or "")[:500],
                     new_price, product.stock,
                     product.image_url or "", product.image_local_path or "",
                     keep_category, product.brand or "",
                     getattr(product, 'unit', '') or "",
                     min_q, pkg_q, pid)
                )
                if abs(new_price - old_price) > 0.001:
                    cursor.execute("INSERT INTO price_history (product_id,price) VALUES (%s,%s)", (pid, new_price))
            else:
                # 判断是否为原有供应商的新商品
                is_new_product = 0
                try:
                    cursor.execute("""
                        SELECT TIMESTAMPDIFF(HOUR, created_at, NOW()) > 24
                               AND (SELECT COUNT(*) FROM products WHERE supplier_id=%s AND is_available=1) > 100
                        FROM suppliers WHERE id=%s
                    """, (product.supplier_id, product.supplier_id))
                    row = cursor.fetchone()
                    if row and row[0]:
                        is_new_product = 1
                except Exception:
                    pass
                min_q = int(getattr(product, 'min_qty', 1) or 1)
                pkg_q = int(getattr(product, 'package_qty', 1) or 1)
                cursor.execute(
                    """INSERT INTO products
                       (supplier_id,name,sku,category,brand,price,currency,unit,
                        min_qty,package_qty,
                        stock,description,image_url,image_local,product_url,is_available,is_new)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s)""",
                    (product.supplier_id, product.name[:300], product.sku or "",
                     product.category or "", product.brand or "",
                     float(product.price), product.currency or "ARS",
                     getattr(product, 'unit', '') or "",
                     min_q, pkg_q,
                     product.stock, (product.description or "")[:500],
                     product.image_url or "", product.image_local_path or "", product_url,
                     is_new_product)
                )
                pid = cursor.lastrowid
                if pid:
                    cursor.execute("INSERT INTO price_history (product_id,price) VALUES (%s,%s)", (pid, float(product.price)))
        return pid or 0

    def bulk_upsert_products(self, products: list) -> int:
        if not products:
            return 0
        deduped_map = {}
        order_keys = []
        for p in products:
            purl = (getattr(p, "product_url", "") or "").strip() or None
            key = ("url", purl) if purl else ("name", p.supplier_id, (p.name or "")[:300])
            if key not in deduped_map:
                order_keys.append(key)
            deduped_map[key] = p
        items = [deduped_map[k] for k in order_keys]
        try:
            return self._bulk_upsert_products_batched(items)
        except Exception as e:
            logger.warning(f"批量入库回退逐条模式 ({len(items)} 条): {e}")
            count = 0
            for p in items:
                try:
                    if self.upsert_product(p):
                        count += 1
                except Exception as item_err:
                    logger.error(f"商品入库失败 [{getattr(p, 'name', '?')}]: {item_err}")
            return count

    def _bulk_upsert_products_batched(self, items: list) -> int:
        existing_by_url = {}
        existing_by_name = {}
        unique_urls = list(dict.fromkeys((getattr(p, "product_url", "") or "").strip() for p in items if (getattr(p, "product_url", "") or "").strip()))
        no_url_items = [p for p in items if not ((getattr(p, "product_url", "") or "").strip())]

        with self.connection() as conn:
            cursor = conn.cursor(buffered=True)
            for i in range(0, len(unique_urls), 500):
                chunk = unique_urls[i:i + 500]
                ph = ",".join(["%s"] * len(chunk))
                cursor.execute(
                    f"SELECT id, price, category, product_url FROM products WHERE product_url IN ({ph}) ORDER BY id ASC",
                    tuple(chunk),
                )
                for pid, price, cat, purl in cursor.fetchall():
                    existing_by_url.setdefault(purl, (int(pid), float(price), cat or ""))

            if no_url_items:
                names_by_sid = {}
                for p in no_url_items:
                    names_by_sid.setdefault(int(p.supplier_id), []).append((p.name or "")[:300])
                for sid, raw_names in names_by_sid.items():
                    uniq_names = list(dict.fromkeys(raw_names))
                    for i in range(0, len(uniq_names), 300):
                        chunk = uniq_names[i:i + 300]
                        ph = ",".join(["%s"] * len(chunk))
                        cursor.execute(
                            f"SELECT id, price, category, name FROM products WHERE supplier_id=%s AND name IN ({ph}) ORDER BY id ASC",
                            (sid, *chunk),
                        )
                        for pid, price, cat, nm in cursor.fetchall():
                            existing_by_name.setdefault((sid, nm), (int(pid), float(price), cat or ""))

            updates_by_id = {}
            to_insert = []
            price_history_rows = []
            for p in items:
                purl = (getattr(p, "product_url", "") or "").strip() or None
                matched = (
                    existing_by_url.get(purl)
                    if purl
                    else existing_by_name.get((int(p.supplier_id), (p.name or "")[:300]))
                )
                new_price = float(p.price)
                if matched:
                    pid, old_price, existing_cat = matched
                    keep_cat = existing_cat if (existing_cat and str(existing_cat).strip()) else (p.category or "")
                    updates_by_id[pid] = (pid, p, new_price, keep_cat)
                    if abs(new_price - old_price) > 0.001:
                        price_history_rows.append((pid, new_price))
                else:
                    to_insert.append((purl, p))

            to_update = list(updates_by_id.values())
            col_specs = [
                ("name", lambda r: (r[1].name or "")[:300]),
                ("sku", lambda r: r[1].sku or ""),
                ("description", lambda r: (r[1].description or "")[:500]),
                ("price", lambda r: r[2]),
                ("stock", lambda r: r[1].stock),
                ("image_url", lambda r: r[1].image_url or ""),
                ("image_local", lambda r: getattr(r[1], "image_local_path", "") or ""),
                ("category", lambda r: r[3]),
                ("brand", lambda r: r[1].brand or ""),
                ("unit", lambda r: getattr(r[1], "unit", "") or ""),
                ("min_qty", lambda r: int(getattr(r[1], "min_qty", 1) or 1)),
                ("package_qty", lambda r: int(getattr(r[1], "package_qty", 1) or 1)),
            ]
            for i in range(0, len(to_update), 100):
                chunk = to_update[i:i + 100]
                pids = [r[0] for r in chunk]
                whens = " ".join(["WHEN %s THEN %s"] * len(chunk))
                set_clauses = []
                params = []
                for col_name, getter in col_specs:
                    set_clauses.append(f"{col_name} = CASE id {whens} END")
                    for r in chunk:
                        params.append(r[0])
                        params.append(getter(r))
                id_ph = ",".join(["%s"] * len(pids))
                params.extend(pids)
                cursor.execute(
                    f"UPDATE products SET {', '.join(set_clauses)}, is_available=1, updated_at=NOW() WHERE id IN ({id_ph})",
                    tuple(params),
                )

            if to_insert:
                ins_sids = sorted({int(p.supplier_id) for _, p in to_insert})
                sid_ph = ",".join(["%s"] * len(ins_sids))
                is_new_map = {}
                try:
                    cursor.execute(
                        f"""SELECT s.id,
                                   TIMESTAMPDIFF(HOUR, s.created_at, NOW()) > 24
                                   AND (SELECT COUNT(*) FROM products p WHERE p.supplier_id=s.id AND p.is_available=1) > 100
                            FROM suppliers s WHERE s.id IN ({sid_ph})""",
                        tuple(ins_sids),
                    )
                    for sid_val, flag in cursor.fetchall():
                        is_new_map[int(sid_val)] = 1 if flag else 0
                except Exception:
                    pass

                row_ph = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s)"
                for i in range(0, len(to_insert), 100):
                    chunk = to_insert[i:i + 100]
                    params = []
                    for purl, p in chunk:
                        min_q = int(getattr(p, 'min_qty', 1) or 1)
                        pkg_q = int(getattr(p, 'package_qty', 1) or 1)
                        params.extend([
                            p.supplier_id, (p.name or '')[:300], p.sku or '',
                            p.category or '', p.brand or '',
                            float(p.price), p.currency or 'ARS',
                            getattr(p, 'unit', '') or '',
                            min_q, pkg_q,
                            p.stock, (p.description or '')[:500],
                            p.image_url or '', getattr(p, 'image_local_path', '') or '', purl,
                            is_new_map.get(int(p.supplier_id), 0),
                        ])
                    values_sql = ",".join([row_ph] * len(chunk))
                    cursor.execute(
                        f"""INSERT INTO products
                           (supplier_id,name,sku,category,brand,price,currency,unit,
                            min_qty,package_qty,
                            stock,description,image_url,image_local,product_url,is_available,is_new)
                           VALUES {values_sql}""",
                        tuple(params),
                    )

                ins_urls = [purl for purl, _ in to_insert if purl]
                ins_id_by_url = {}
                for i in range(0, len(ins_urls), 500):
                    chunk = ins_urls[i:i + 500]
                    ph = ",".join(["%s"] * len(chunk))
                    cursor.execute(
                        f"SELECT id, product_url FROM products WHERE product_url IN ({ph}) ORDER BY id DESC",
                        tuple(chunk),
                    )
                    for pid, purl in cursor.fetchall():
                        ins_id_by_url.setdefault(purl, int(pid))

                ins_no_url_by_sid = {}
                for purl, p in to_insert:
                    if not purl:
                        ins_no_url_by_sid.setdefault(int(p.supplier_id), []).append((p.name or "")[:300])
                ins_id_by_name = {}
                for sid, nms in ins_no_url_by_sid.items():
                    uniq_nms = list(dict.fromkeys(nms))
                    for i in range(0, len(uniq_nms), 300):
                        chunk = uniq_nms[i:i + 300]
                        ph = ",".join(["%s"] * len(chunk))
                        cursor.execute(
                            f"SELECT id, name FROM products WHERE supplier_id=%s AND name IN ({ph}) ORDER BY id DESC",
                            (sid, *chunk),
                        )
                        for pid, nm in cursor.fetchall():
                            ins_id_by_name.setdefault((sid, nm), int(pid))

                for purl, p in to_insert:
                    pid = (
                        ins_id_by_url.get(purl)
                        if purl
                        else ins_id_by_name.get((int(p.supplier_id), (p.name or "")[:300]))
                    )
                    if pid:
                        price_history_rows.append((pid, float(p.price)))

            for i in range(0, len(price_history_rows), 500):
                chunk = price_history_rows[i:i + 500]
                ph = ",".join(["(%s,%s)"] * len(chunk))
                flat = []
                for pid, pr in chunk:
                    flat.extend((pid, pr))
                cursor.execute(f"INSERT INTO price_history (product_id,price) VALUES {ph}", tuple(flat))

            cursor.close()
        return len(items)

    def get_existing_product_names(self, supplier_id: int) -> set:
        """获取某供应商所有在售商品名集合，用于断点续传跳过已爬商品"""
        names = set()
        try:
            with self.connection() as conn:
                cursor = conn.cursor(buffered=True)
                cursor.execute(
                    "SELECT name FROM products WHERE supplier_id=%s AND is_available=1",
                    (supplier_id,)
                )
                for row in cursor.fetchall():
                    if row[0]:
                        names.add(row[0].strip())
        except Exception as e:
            logger.error(f"获取已爬商品名失败: {e}")
        return names

    def get_existing_offer_ids(self, supplier_id: int) -> set:
        """获取某供应商所有在售商品的offer_id集合（从product_url提取），用于断点续传"""
        offer_ids = set()
        try:
            with self.connection() as conn:
                cursor = conn.cursor(buffered=True)
                cursor.execute(
                    "SELECT product_url FROM products WHERE supplier_id=%s AND is_available=1",
                    (supplier_id,)
                )
                import re as _re
                for row in cursor.fetchall():
                    url = (row[0] or '').strip()
                    m = _re.search(r'/offer/(\d+)\.html', url)
                    if m:
                        offer_ids.add(m.group(1))
        except Exception as e:
            logger.error(f"获取已爬offer_id失败: {e}")
        return offer_ids

    def mark_missing_products(self, supplier_id: int, crawled_products: list) -> int:
        """
        爬取完成后标记缺失商品。
        条件：本次爬到数量达到供应商配置的阈值（missing_threshold，默认80%），
        且商品本次未被爬到。
        返回标记数量，-1表示爬取不完整跳过。
        """
        with self.connection() as conn:
            cursor = conn.cursor(buffered=True)

            # 从 suppliers 表读取该供应商的缺失判定阈值
            cursor.execute(
                "SELECT missing_threshold FROM suppliers WHERE id=%s",
                (supplier_id,)
            )
            row = cursor.fetchone()
            threshold_pct = row[0] if row and row[0] else 80
            threshold_ratio = threshold_pct / 100.0

            # 数据库原有商品数
            cursor.execute(
                "SELECT COUNT(*) FROM products WHERE supplier_id=%s AND is_available=1",
                (supplier_id,)
            )
            existing_count = cursor.fetchone()[0]

            if existing_count == 0:
                return 0

            crawled_count = len(crawled_products)
            ratio = crawled_count / existing_count

            # 不足阈值认为爬取不完整，跳过标记
            if ratio < threshold_ratio:
                logger.info(f"[mark_missing] supplier={supplier_id} 爬取不完整"
                            f"（本次{crawled_count}/原有{existing_count}={ratio:.1%}，阈值{threshold_pct}%），跳过缺失标记")
                return -1

            # 收集本次爬到的商品标识（product_url和name都收集，确保两种匹配方式都能命中）
            crawled_urls  = set()
            crawled_names = set()
            for p in crawled_products:
                url = (getattr(p, 'product_url', '') or '').strip()
                name = (getattr(p, 'name', '') or '').strip()[:300]
                if url:
                    crawled_urls.add(url)
                if name:
                    crawled_names.add(name)

            # 先清除该供应商的旧缺失标记
            cursor.execute(
                "UPDATE products SET is_missing=0 WHERE supplier_id=%s", (supplier_id,)
            )

            # 查出该供应商所有商品
            cursor.execute(
                "SELECT id, name, product_url FROM products WHERE supplier_id=%s AND is_available=1",
                (supplier_id,)
            )
            rows = cursor.fetchall()

            missing_ids = []
            for row in rows:
                pid, name, purl = row[0], row[1], row[2] or ''
                # 有url的先用url匹配，未命中再用name兜底
                if purl:
                    if purl in crawled_urls:
                        continue
                    if name and name in crawled_names:
                        continue
                    missing_ids.append(pid)
                else:
                    # 无url的用name匹配
                    if name not in crawled_names:
                        missing_ids.append(pid)

            if missing_ids:
                placeholders = ','.join(['%s'] * len(missing_ids))
                cursor.execute(
                    f"UPDATE products SET is_missing=1 WHERE id IN ({placeholders})",
                    missing_ids
                )

            logger.info(f"[mark_missing] supplier={supplier_id} "
                        f"本次{crawled_count}/原有{existing_count}={ratio:.1%}，"
                        f"标记缺失{len(missing_ids)}条")
            return len(missing_ids)

    def mark_pdf_supplier_missing(self, supplier_id: int) -> dict:
        """
        更新 PDF 导入店铺的缺货商品逻辑：
        非最后一个导入日导入的商品全部标记为缺货商品 (is_missing=1)
        最后一个导入日导入的商品标记为正常 (is_missing=0)
        """
        with self.connection() as conn:
            cursor = conn.cursor(buffered=True)
            cursor.execute("""
                SELECT DATE(updated_at) as dt, COUNT(*) as cnt
                FROM products 
                WHERE supplier_id = %s
                GROUP BY DATE(updated_at)
                ORDER BY dt DESC
            """, (supplier_id,))
            rows = cursor.fetchall()
            if not rows:
                return {
                    "supplier_id": supplier_id,
                    "latest_date": None,
                    "marked_missing": 0,
                    "available_count": 0,
                }

            # 寻找最后一个批量导入日（优先选取商品数>=5的最近日期，防止单品编辑日期干扰；若均小于5则取最新日期）
            latest_date = rows[0][0]
            for dt, cnt in rows:
                if cnt >= 5:
                    latest_date = dt
                    break

            # 1. 最后一个导入日导入（及后续）的商品标记为正常未缺货
            cursor.execute("""
                UPDATE products 
                SET is_missing = 0, updated_at = updated_at
                WHERE supplier_id = %s AND DATE(updated_at) >= %s
            """, (supplier_id, latest_date))
            available_count = cursor.rowcount

            # 2. 非最后一个导入日导入的商品全部标记为缺货商品
            cursor.execute("""
                UPDATE products 
                SET is_missing = 1, updated_at = updated_at
                WHERE supplier_id = %s AND DATE(updated_at) < %s
            """, (supplier_id, latest_date))
            marked_missing = cursor.rowcount

            conn.commit()
            logger.info(f"[mark_pdf_supplier_missing] supplier={supplier_id} 最新导入日={latest_date}，正常={available_count}条，标记缺货={marked_missing}条")
            return {
                "supplier_id": supplier_id,
                "latest_date": str(latest_date),
                "marked_missing": marked_missing,
                "available_count": available_count,
            }

    def sync_all_pdf_suppliers_missing(self) -> list:
        """同步所有 PDF 导入店铺的缺货商品状态"""
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name FROM suppliers 
                WHERE base_url IN ('nashi', '') OR list_url IN ('nashi', '') OR base_url NOT LIKE 'http%'
                ORDER BY id
            """)
            suppliers = cursor.fetchall()
        
        results = []
        for sid, sname in suppliers:
            res = self.mark_pdf_supplier_missing(sid)
            res["supplier_name"] = sname
            results.append(res)
        return results

    def get_products(self, supplier_id=None, category=None, category_prefix=None,
                     no_category=False, keyword=None, page=1, page_size=20,
                     sort=None, min_price=None, max_price=None, include_missing=False) -> dict:
        conditions = ["p.is_available=1"]
        if not include_missing:
            conditions.append("p.is_missing=0")
        params = []
        if supplier_id:    conditions.append("p.supplier_id=%s"); params.append(supplier_id)
        if no_category:
            conditions.append("(p.category IS NULL OR p.category='')")
        elif category and category.strip() and category != '__none__':
            cat = category.strip()
            if ' > ' in cat:
                # 二级分类：精确匹配 OR 前缀匹配（含三级）OR 多分类关联表
                conditions.append("""(p.category=%s OR p.category LIKE %s
                    OR p.id IN (SELECT pc.product_id FROM product_categories pc WHERE pc.category=%s OR pc.category LIKE %s))""")
                params.extend([cat, cat + " > %", cat, cat + " > %"])
            else:
                # 一级分类：精确匹配 OR 前缀匹配 OR 多分类关联表
                conditions.append("""(p.category=%s OR p.category LIKE %s
                    OR p.id IN (SELECT pc.product_id FROM product_categories pc WHERE pc.category=%s OR pc.category LIKE %s))""")
                params.extend([cat, cat + " > %", cat, cat + " > %"])
        elif category_prefix and category_prefix.strip():
            conditions.append("p.category LIKE %s"); params.append(category_prefix.strip() + " > %")
        if keyword:        conditions.append("(p.name LIKE %s OR p.sku LIKE %s)");   params.append(f"%{keyword}%"); params.append(f"%{keyword}%")
        if min_price is not None:
            conditions.append("p.price * (1 + IFNULL(s.price_adjustment,0)/100) >= %s"); params.append(float(min_price))
        if max_price is not None:
            conditions.append("p.price * (1 + IFNULL(s.price_adjustment,0)/100) <= %s"); params.append(float(max_price))
        where = "WHERE " + " AND ".join(conditions)
        offset = (page - 1) * page_size

        # sort: price_asc / price_desc / name_asc / name_desc / default updated_at DESC
        order_clause = "ORDER BY p.updated_at DESC"
        if sort == 'price_asc':
            order_clause = "ORDER BY p.price * (1 + IFNULL(s.price_adjustment,0)/100) ASC"
        elif sort == 'price_desc':
            order_clause = "ORDER BY p.price * (1 + IFNULL(s.price_adjustment,0)/100) DESC"
        elif sort == 'name_asc':
            order_clause = "ORDER BY p.name ASC"
        elif sort == 'name_desc':
            order_clause = "ORDER BY p.name DESC"

        with self.connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(f"SELECT COUNT(*) as total FROM products p JOIN suppliers s ON p.supplier_id=s.id {where}", params)
            total = cursor.fetchone()["total"]
            cursor.execute(
                f"""SELECT p.*, s.name as supplier_name, IFNULL(c.quantity, 0) as cart_qty
                    FROM products p JOIN suppliers s ON p.supplier_id=s.id
                    LEFT JOIN cart_items c ON p.id=c.product_id
                    {where} {order_clause} LIMIT %s OFFSET %s""",
                params + [page_size, offset]
            )
            rows = cursor.fetchall()
        for r in rows:
            for f in ("created_at", "updated_at"):
                if isinstance(r.get(f), datetime): r[f] = r[f].strftime("%Y-%m-%d %H:%M:%S")
        return {"total": total, "page": page, "page_size": page_size, "data": rows}

    def get_product(self, product_id: int):
        with self.connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """SELECT p.*, s.name as supplier_name, IFNULL(c.quantity, 0) as cart_qty
                   FROM products p JOIN suppliers s ON p.supplier_id=s.id
                   LEFT JOIN cart_items c ON p.id=c.product_id
                   WHERE p.id=%s""",
                (product_id,)
            )
            r = cursor.fetchone()
        if r:
            for f in ("created_at", "updated_at"):
                if isinstance(r.get(f), datetime): r[f] = r[f].strftime("%Y-%m-%d %H:%M:%S")
        return r

    def get_price_history(self, product_id: int, days: int = 30) -> list:
        with self.connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT price, recorded_at FROM price_history "
                "WHERE product_id=%s AND recorded_at >= DATE_SUB(NOW(), INTERVAL %s DAY) ORDER BY recorded_at",
                (product_id, days)
            )
            rows = cursor.fetchall()
        for r in rows:
            if isinstance(r.get("recorded_at"), datetime): r["recorded_at"] = r["recorded_at"].strftime("%Y-%m-%d %H:%M:%S")
        return rows

    def get_dashboard_stats(self) -> dict:
        with self.connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT COUNT(*) as cnt FROM suppliers WHERE is_active=1")
            suppliers = cursor.fetchone()["cnt"]
            cursor.execute("SELECT COUNT(*) as cnt FROM products WHERE is_available=1")
            products = cursor.fetchone()["cnt"]
            cursor.execute("SELECT COUNT(*) as cnt FROM crawl_logs WHERE DATE(started_at)=CURDATE()")
            tasks_today = cursor.fetchone()["cnt"]
            cursor.execute("SELECT COUNT(*) as cnt FROM crawl_logs WHERE status='running'")
            running = cursor.fetchone()["cnt"]
            cursor.execute("""
                SELECT s.name, COUNT(p.id) as count
                FROM suppliers s LEFT JOIN products p ON s.id=p.supplier_id AND p.is_available=1
                WHERE s.is_active=1 GROUP BY s.id, s.name ORDER BY count DESC
            """)
            by_supplier = cursor.fetchall()
        return {"suppliers": suppliers, "products": products,
                "tasks_today": tasks_today, "running_tasks": running, "by_supplier": by_supplier}

    def create_crawl_log(self, supplier_id: int) -> int:
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO crawl_logs (supplier_id,status) VALUES (%s,'running')", (supplier_id,))
            return cursor.lastrowid

    def update_crawl_log_progress(self, log_id: int, pages: int, products: int, errors: int):
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE crawl_logs SET pages_crawled=%s,products_found=%s,errors=%s WHERE id=%s",
                           (pages, products, errors, log_id))

    def finish_crawl_log(self, log_id: int, status: str, pages: int, products: int, errors: int, msg: str = ""):
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE crawl_logs SET status=%s,pages_crawled=%s,products_found=%s,errors=%s,
                    finished_at=NOW(), duration_sec=TIMESTAMPDIFF(SECOND,started_at,NOW()), message=%s
                WHERE id=%s
            """, (status, pages, products, errors, (msg or "")[:500], log_id))

    def get_crawl_logs(self, supplier_id: int = None, limit: int = 20) -> list:
        conditions = []
        params = []
        if supplier_id: conditions.append("cl.supplier_id=%s"); params.append(supplier_id)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        with self.connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                f"""SELECT cl.*, s.name as supplier_name
                    FROM crawl_logs cl JOIN suppliers s ON cl.supplier_id=s.id
                    {where} ORDER BY cl.started_at DESC LIMIT %s""",
                params + [limit]
            )
            rows = cursor.fetchall()
        for r in rows:
            for f in ("started_at", "finished_at"):
                if isinstance(r.get(f), datetime): r[f] = r[f].strftime("%Y-%m-%d %H:%M:%S")
        return rows

    # ================================================================
    # Favorites
    # ================================================================
    def add_favorite(self, product_id: int) -> bool:
        """添加收藏，已存在则忽略。返回是否新增。"""
        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "INSERT IGNORE INTO favorites (product_id) VALUES (%s)", (product_id,)
                )
                return cursor.rowcount > 0
            finally:
                cursor.close()

    def remove_favorite(self, product_id: int) -> bool:
        """取消收藏。返回是否删除了记录。"""
        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DELETE FROM favorites WHERE product_id=%s", (product_id,))
                return cursor.rowcount > 0
            finally:
                cursor.close()

    def clear_all_favorites(self) -> int:
        """清空所有收藏"""
        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("DELETE FROM favorites")
                return cursor.rowcount
            finally:
                cursor.close()

    def get_products_by_ids(self, product_ids: list) -> list:
        """批量按ID获取商品数据（含供应商名）"""
        if not product_ids:
            return []
        with self.connection() as conn:
            cursor = conn.cursor(dictionary=True)
            placeholders = ','.join(['%s'] * len(product_ids))
            cursor.execute(
                f"""SELECT p.*, s.name as supplier_name, IFNULL(c.quantity, 0) as cart_qty
                    FROM products p LEFT JOIN suppliers s ON p.supplier_id=s.id
                    LEFT JOIN cart_items c ON p.id=c.product_id
                    WHERE p.id IN ({placeholders})""",
                product_ids
            )
            rows = cursor.fetchall()
        for r in rows:
            for f2 in ("created_at", "updated_at"):
                if isinstance(r.get(f2), datetime):
                    r[f2] = r[f2].strftime("%Y-%m-%d %H:%M:%S")
        return rows

    def get_favorite_ids(self) -> list:
        """返回所有收藏的 product_id 列表。"""
        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT product_id FROM favorites ORDER BY saved_at DESC")
                return [row[0] for row in cursor.fetchall()]
            finally:
                cursor.close()

    def is_favorite(self, product_id: int) -> bool:
        """检查是否已收藏。"""
        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT 1 FROM favorites WHERE product_id=%s", (product_id,))
                return cursor.fetchone() is not None
            finally:
                cursor.close()

    def sync_favorites(self, product_ids: list) -> int:
        """批量同步收藏（用于迁移），返回成功数量。"""
        count = 0
        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                for pid in product_ids:
                    try:
                        cursor.execute(
                            "INSERT IGNORE INTO favorites (product_id) VALUES (%s)", (int(pid),)
                        )
                        if cursor.rowcount > 0:
                            count += 1
                    except (ValueError, TypeError):
                        continue
                conn.commit()
            finally:
                cursor.close()
        return count

    # ================================================================
    # Shopping Cart
    # ================================================================
    def get_cart_items(self) -> list:
        """获取购物车所有商品列表，含商品详情及供应商信息"""
        with self.connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT 
                    c.id AS cart_id,
                    c.product_id,
                    c.quantity,
                    c.created_at AS cart_added_at,
                    c.updated_at AS cart_updated_at,
                    p.name,
                    p.sku,
                    p.category,
                    p.brand,
                    CAST(p.price AS DOUBLE) AS price,
                    p.currency,
                    p.unit,
                    p.stock,
                    p.description,
                    p.image_url,
                    p.image_local,
                    p.product_url,
                    p.is_available,
                    p.is_missing,
                    IFNULL(p.min_qty, 1) AS min_qty,
                    IFNULL(p.package_qty, 1) AS package_qty,
                    IFNULL(p.sub_pack_qty, 1) AS sub_pack_qty,
                    p.supplier_id,
                    s.name AS supplier_name,
                    IFNULL(s.price_adjustment, 0) AS price_adjustment
                FROM cart_items c
                JOIN products p ON c.product_id = p.id
                LEFT JOIN suppliers s ON p.supplier_id = s.id
                ORDER BY c.updated_at DESC, c.id DESC
            """)
            rows = cursor.fetchall()
        for r in rows:
            for f in ("cart_added_at", "cart_updated_at"):
                if isinstance(r.get(f), datetime):
                    r[f] = r[f].strftime("%Y-%m-%d %H:%M:%S")
        return rows

    def get_cart_stats(self, include_missing: bool = False) -> dict:
        """获取购物车统计信息（品种数、总件数）"""
        with self.connection() as conn:
            cursor = conn.cursor()
            if include_missing:
                cursor.execute("""
                    SELECT COUNT(*), IFNULL(SUM(quantity), 0) FROM cart_items
                """)
            else:
                cursor.execute("""
                    SELECT COUNT(*), IFNULL(SUM(c.quantity), 0) 
                    FROM cart_items c JOIN products p ON c.product_id = p.id
                    WHERE p.is_missing = 0
                """)
            row = cursor.fetchone()
            cursor.execute("SELECT product_id, quantity FROM cart_items WHERE quantity > 0")
            items_rows = cursor.fetchall()
            items_map = {str(r[0]): int(r[1]) for r in items_rows if r[1] and r[1] > 0}
            return {
                "distinct_count": row[0] if row else 0,
                "total_quantity": int(row[1]) if row and row[1] else 0,
                "items_map": items_map
            }

    def add_to_cart(self, product_id: int, quantity: int = 1, mode: str = "add") -> dict:
        """
        添加商品至购物车。
        mode='add': 累加数量
        mode='set': 覆盖数量
        """
        qty = max(1, int(quantity))
        with self.connection() as conn:
            cursor = conn.cursor()
            if mode == "set":
                cursor.execute("""
                    INSERT INTO cart_items (product_id, quantity)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE quantity = VALUES(quantity)
                """, (product_id, qty))
            else:
                cursor.execute("""
                    INSERT INTO cart_items (product_id, quantity)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE quantity = quantity + VALUES(quantity)
                """, (product_id, qty))
            cursor.execute("SELECT quantity FROM cart_items WHERE product_id=%s", (product_id,))
            row = cursor.fetchone()
            current_qty = row[0] if row else qty
        return {"product_id": product_id, "quantity": current_qty}

    def update_cart_quantity(self, product_id: int, quantity: int) -> dict:
        """更新购物车中某商品的数量。如果 <= 0 则删除。"""
        qty = int(quantity)
        with self.connection() as conn:
            cursor = conn.cursor()
            if qty <= 0:
                cursor.execute("DELETE FROM cart_items WHERE product_id=%s", (product_id,))
                return {"product_id": product_id, "quantity": 0, "deleted": True}
            else:
                cursor.execute("""
                    INSERT INTO cart_items (product_id, quantity)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE quantity = VALUES(quantity)
                """, (product_id, qty))
                return {"product_id": product_id, "quantity": qty, "deleted": False}

    def remove_from_cart(self, product_id: int) -> bool:
        """从购物车移除商品"""
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM cart_items WHERE product_id=%s", (product_id,))
            return cursor.rowcount > 0

    def clear_cart(self) -> int:
        """清空购物车"""
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM cart_items")
            return cursor.rowcount

    def batch_add_to_cart(self, items: list) -> dict:
        """
        批量添加商品至购物车。
        items 可包含 {"product_id": int, "quantity": int} 或 {"sku": str, "quantity": int}
        """
        success = []
        failed = []
        with self.connection() as conn:
            cursor = conn.cursor(dictionary=True)
            for it in items:
                pid = it.get("product_id")
                qty = max(1, int(it.get("quantity") or 1))
                sku = str(it.get("sku") or "").strip()

                if not pid and sku:
                    clean_sku = sku.replace("-", "").replace(" ", "")
                    cursor.execute("""
                        SELECT id, sku, name FROM products 
                        WHERE sku = %s 
                           OR REPLACE(REPLACE(sku, '-', ''), ' ', '') = %s
                           OR sku LIKE %s
                        ORDER BY is_available DESC, id DESC
                        LIMIT 1
                    """, (sku, clean_sku, f"{sku}%"))
                    row = cursor.fetchone()
                    if row:
                        pid = row["id"]
                    else:
                        failed.append({"sku": sku, "quantity": qty, "reason": "未找到匹配的商品"})
                        continue

                if pid:
                    cursor.execute("""
                        INSERT INTO cart_items (product_id, quantity)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE quantity = quantity + VALUES(quantity)
                    """, (pid, qty))
                    success.append({"product_id": pid, "sku": sku, "quantity": qty})
                else:
                    failed.append({"sku": sku, "quantity": qty, "reason": "缺少商品ID"})

        return {
            "success_count": len(success),
            "failed_count": len(failed),
            "success": success,
            "failed": failed
        }

    # ================================================================
    # System Settings
    # ================================================================
    def get_setting(self, key: str, default: str = "") -> str:
        """获取系统配置项"""
        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT `value` FROM system_settings WHERE `key`=%s", (key,))
                row = cursor.fetchone()
                return str(row[0]) if row and row[0] is not None else default
            except Exception:
                return default
            finally:
                cursor.close()

    def set_setting(self, key: str, value: str) -> None:
        """保存系统配置项"""
        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "INSERT INTO system_settings (`key`, `value`) VALUES (%s, %s) "
                    "ON DUPLICATE KEY UPDATE `value`=VALUES(`value`), updated_at=NOW()",
                    (key, str(value))
                )
            finally:
                cursor.close()

    def get_mobile_multiplier(self) -> float:
        """获取手机端零售倍率（默认 2.2）"""
        val_str = self.get_setting("mobile_retail_mult", "2.2")
        try:
            val = float(val_str)
            return val if val > 0 else 2.2
        except (ValueError, TypeError):
            return 2.2

    def set_mobile_multiplier(self, multiplier: float) -> None:
        """设置手机端零售倍率"""
        self.set_setting("mobile_retail_mult", str(round(float(multiplier), 4)))

