# -*- coding: utf-8 -*-
"""
Created on Wed May 20 15:13:38 2026

@author: wits
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pymysql
from pymysql import err as pymysql_err


def get_logger(name: str = "gcis_pipeline") -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    os.makedirs("logs", exist_ok=True)

    log_filename = os.path.join(
        "logs",
        f"gcis_pipeline_{datetime.now():%Y%m%d}.txt"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def get_mysql_settings_from_env() -> Dict[str, Any]:
    host = os.getenv("MYSQL_HOST", "localhost")
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "Wits-1302745")
    db = os.getenv("MYSQL_DB", "crawlerdb_v2")
    port = int(os.getenv("MYSQL_PORT", "3306"))

    if not host or not user or not db:
        raise ValueError("MySQL 設定不足：MYSQL_HOST / MYSQL_USER / MYSQL_DB 必填")

    return {
        "host": host,
        "user": user,
        "password": password,
        "database": db,
        "port": port,
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": False,
    }


def connect_mysql(cfg: Dict[str, Any]):
    return pymysql.connect(**cfg)


def _current_db_name(conn) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT DATABASE() AS db")
        row = cur.fetchone()
        return str(row.get("db") or "").strip()


def table_exists(conn, table_name: str, schema_name: Optional[str] = None) -> bool:
    schema = (schema_name or _current_db_name(conn) or "crawlerdb_v2").strip()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 AS ok
            FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
            LIMIT 1
            """,
            (schema, table_name),
        )
        return cur.fetchone() is not None


def truncate_daily_tables(conn, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or get_logger()

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE crawlerdb_v2.tmp_rawData")
        cur.execute("TRUNCATE TABLE crawlerdb_v2.Tmp_TaxInfo")

    conn.commit()
    logger.info("🧹 已清空日批暫存表：tmp_rawData / Tmp_TaxInfo")


def truncate_tmp_rawdata(conn, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or get_logger()

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE crawlerdb_v2.tmp_rawData")

    conn.commit()
    logger.info("✅ 已清空 tmp_rawData")


def truncate_legacy_tmp_taxinfo(conn, logger: Optional[logging.Logger] = None) -> None:
    logger = logger or get_logger()

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE crawlerdb_v2.Tmp_TaxInfo")

    conn.commit()
    logger.info("✅ 已清空 Tmp_TaxInfo")


def insert_tmp_rawdata(
    conn,
    raw_rows: List[Tuple[Any, ...]],
    logger: Optional[logging.Logger] = None,
    batch_size: int = 10_000,
) -> int:
    logger = logger or get_logger()

    sql = """
    INSERT INTO crawlerdb_v2.tmp_rawData
      (run_id, source_url, local_zip_path, local_csv_path,
       downloaded_at, file_date,
       row_num, row_type,
       c01,c02,c03,c04,c05,c06,c07,c08,c09,c10,c11,c12,c13,c14,c15,c16)
    VALUES
      (%s,%s,%s,%s,%s,%s,%s,%s,
       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    inserted = 0

    with conn.cursor() as cur:
        for i in range(0, len(raw_rows), batch_size):
            chunk = raw_rows[i:i + batch_size]
            cur.executemany(sql, chunk)
            inserted += len(chunk)

    conn.commit()
    logger.info(f"✅ tmp_rawData 入庫完成：{inserted} 列")

    return inserted


def count_rawdata_data_rows(conn, run_id: str, logger: Optional[logging.Logger] = None) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM crawlerdb_v2.tmp_rawData
            WHERE run_id=%s AND row_type='DATA'
            """,
            (run_id,),
        )
        return int(cur.fetchone()["cnt"])


def insert_legacy_tmp_taxinfo(
    conn,
    rows: List[Dict[str, Any]],
    logger: Optional[logging.Logger] = None,
    batch_size: int = 10_000,
) -> int:
    logger = logger or get_logger()

    if not rows:
        logger.warning("⚠️ Tmp_TaxInfo 無資料可寫入")
        return 0

    sql = """
    INSERT INTO crawlerdb_v2.Tmp_TaxInfo
      (Party_Addr, Party_ID, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
       Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
       Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3)
    VALUES
      (%(party_addr)s, %(party_id)s, %(parent_party_id)s, %(party_name)s,
       %(paidin_capital)s, %(setup_date)s,
       %(party_type)s, %(use_invoice)s, %(ind_code)s, %(ind_name)s,
       %(ind_code1)s, %(ind_name1)s,
       %(ind_code2)s, %(ind_name2)s, %(ind_code3)s, %(ind_name3)s)
    """

    inserted = 0

    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            cur.executemany(sql, chunk)
            inserted += len(chunk)

    conn.commit()
    logger.info(f"✅ Tmp_TaxInfo 入庫完成：{inserted} 筆")

    return inserted


def count_legacy_tmp_taxinfo(conn, logger: Optional[logging.Logger] = None) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM crawlerdb_v2.Tmp_TaxInfo")
        return int(cur.fetchone()["cnt"])


def merge_diff_tmp_to_main_taxinfo(
    conn,
    logger: Optional[logging.Logger] = None
) -> Dict[str, int]:
    logger = logger or get_logger()

    stats_sql = """
        WITH latest_taxinfo AS (
          SELECT
            Party_ID,
            ROW_NUMBER() OVER (
              PARTITION BY Party_ID
              ORDER BY Update_Time DESC
            ) AS rn,
            MD5(CONCAT_WS('|',
              IFNULL(Party_Addr,''), IFNULL(Parent_Party_ID,''), IFNULL(Party_Name,''),
              IFNULL(CAST(PaidIn_Capital AS CHAR),''), IFNULL(CAST(Setup_Date AS CHAR),''),
              IFNULL(Party_Type,''), IFNULL(Use_Invoice,''),
              IFNULL(Ind_Code,''), IFNULL(Ind_Name,''),
              IFNULL(Ind_Code1,''), IFNULL(Ind_Name1,''),
              IFNULL(Ind_Code2,''), IFNULL(Ind_Name2,''),
              IFNULL(Ind_Code3,''), IFNULL(Ind_Name3,'')
            )) AS row_hash
          FROM crawlerdb_v2.TaxInfo
        ),
        tmp_latest AS (
          SELECT
            Party_ID,
            ROW_NUMBER() OVER (
              PARTITION BY Party_ID
              ORDER BY Update_Time DESC
            ) AS rn,
            MD5(CONCAT_WS('|',
              IFNULL(Party_Addr,''), IFNULL(Parent_Party_ID,''), IFNULL(Party_Name,''),
              IFNULL(CAST(PaidIn_Capital AS CHAR),''), IFNULL(CAST(Setup_Date AS CHAR),''),
              IFNULL(Party_Type,''), IFNULL(Use_Invoice,''),
              IFNULL(Ind_Code,''), IFNULL(Ind_Name,''),
              IFNULL(Ind_Code1,''), IFNULL(Ind_Name1,''),
              IFNULL(Ind_Code2,''), IFNULL(Ind_Name2,''),
              IFNULL(Ind_Code3,''), IFNULL(Ind_Name3,'')
            )) AS row_hash
          FROM crawlerdb_v2.Tmp_TaxInfo
        )
        SELECT
          SUM(
            CASE
              WHEN latest.Party_ID IS NULL THEN 1
              ELSE 0
            END
          ) AS new_party_id_cnt,

          SUM(
            CASE
              WHEN latest.Party_ID IS NOT NULL
                   AND tmp.row_hash <> latest.row_hash
              THEN 1
              ELSE 0
            END
          ) AS changed_party_id_cnt

        FROM tmp_latest tmp
        LEFT JOIN latest_taxinfo latest
          ON latest.Party_ID = tmp.Party_ID
         AND latest.rn = 1
        WHERE tmp.rn = 1
          AND (
            latest.Party_ID IS NULL
            OR tmp.row_hash <> latest.row_hash
          )
    """

    insert_sql = """
        INSERT INTO crawlerdb_v2.TaxInfo
          (Party_ID, Party_Addr, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
           Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
           Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3)
        WITH latest_taxinfo AS (
          SELECT
            Party_ID, Party_Addr, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
            Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
            Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3,
            ROW_NUMBER() OVER (
              PARTITION BY Party_ID
              ORDER BY Update_Time DESC
            ) AS rn,
            MD5(CONCAT_WS('|',
              IFNULL(Party_Addr,''), IFNULL(Parent_Party_ID,''), IFNULL(Party_Name,''),
              IFNULL(CAST(PaidIn_Capital AS CHAR),''), IFNULL(CAST(Setup_Date AS CHAR),''),
              IFNULL(Party_Type,''), IFNULL(Use_Invoice,''),
              IFNULL(Ind_Code,''), IFNULL(Ind_Name,''),
              IFNULL(Ind_Code1,''), IFNULL(Ind_Name1,''),
              IFNULL(Ind_Code2,''), IFNULL(Ind_Name2,''),
              IFNULL(Ind_Code3,''), IFNULL(Ind_Name3,'')
            )) AS row_hash
          FROM crawlerdb_v2.TaxInfo
        ),
        tmp_latest AS (
          SELECT
            Party_ID, Party_Addr, Parent_Party_ID, Party_Name, PaidIn_Capital, Setup_Date,
            Party_Type, Use_Invoice, Ind_Code, Ind_Name, Ind_Code1, Ind_Name1,
            Ind_Code2, Ind_Name2, Ind_Code3, Ind_Name3,
            ROW_NUMBER() OVER (
              PARTITION BY Party_ID
              ORDER BY Update_Time DESC
            ) AS rn,
            MD5(CONCAT_WS('|',
              IFNULL(Party_Addr,''), IFNULL(Parent_Party_ID,''), IFNULL(Party_Name,''),
              IFNULL(CAST(PaidIn_Capital AS CHAR),''), IFNULL(CAST(Setup_Date AS CHAR),''),
              IFNULL(Party_Type,''), IFNULL(Use_Invoice,''),
              IFNULL(Ind_Code,''), IFNULL(Ind_Name,''),
              IFNULL(Ind_Code1,''), IFNULL(Ind_Name1,''),
              IFNULL(Ind_Code2,''), IFNULL(Ind_Name2,''),
              IFNULL(Ind_Code3,''), IFNULL(Ind_Name3,'')
            )) AS row_hash
          FROM crawlerdb_v2.Tmp_TaxInfo
        )
        SELECT
          tmp.Party_ID, tmp.Party_Addr, tmp.Parent_Party_ID, tmp.Party_Name,
          tmp.PaidIn_Capital, tmp.Setup_Date,
          tmp.Party_Type, tmp.Use_Invoice, tmp.Ind_Code, tmp.Ind_Name,
          tmp.Ind_Code1, tmp.Ind_Name1,
          tmp.Ind_Code2, tmp.Ind_Name2, tmp.Ind_Code3, tmp.Ind_Name3
        FROM tmp_latest tmp
        LEFT JOIN latest_taxinfo latest
          ON latest.Party_ID = tmp.Party_ID
         AND latest.rn = 1
        WHERE tmp.rn = 1
          AND (
            latest.Party_ID IS NULL
            OR tmp.row_hash <> latest.row_hash
          )
    """

    with conn.cursor() as cur:
        cur.execute(stats_sql)
        stats = cur.fetchone()

    new_party_id_cnt = int(stats["new_party_id_cnt"] or 0)
    changed_party_id_cnt = int(stats["changed_party_id_cnt"] or 0)

    with conn.cursor() as cur:
        affected = cur.execute(insert_sql)

    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM crawlerdb_v2.Tmp_TaxInfo")
        tmp_cnt = int(cur.fetchone()["cnt"])

        cur.execute("SELECT COUNT(*) AS cnt FROM crawlerdb_v2.TaxInfo")
        main_cnt = int(cur.fetchone()["cnt"])

    logger.info(
        f"✅ TaxInfo 差異寫入完成："
        f"目前有幾筆資料={tmp_cnt}，"
        f"實際新增了幾筆={affected}，"
        f"新的統一編號={new_party_id_cnt}，"
        f"更新資料筆數={changed_party_id_cnt}，"
        f"TaxInfo(累積歷史)總筆數={main_cnt}"
    )

    return {
        "目前有幾筆資料": tmp_cnt,
        "實際新增了幾筆": int(affected),
        "新的統一編號": new_party_id_cnt,
        "更新資料筆數": changed_party_id_cnt,
        "累積歷史總筆數": main_cnt,
    }


def ensure_taxrecord_for_new_party_ids(
    conn,
    logger: Optional[logging.Logger] = None
) -> int:
    logger = logger or get_logger()

    if os.getenv("DISABLE_TAXRECORD", "").strip().lower() in {"1", "true", "yes", "on"}:
        logger.warning("⚠️ 已停用 TaxRecord 寫入（DISABLE_TAXRECORD=1）")
        return 0

    schema = _current_db_name(conn) or "crawlerdb_v2"

    if not table_exists(conn, "TaxRecord", schema_name=schema):
        logger.warning(f"⚠️ 找不到 {schema}.TaxRecord，跳過 TaxRecord 補寫")
        return 0

    sql = f"""
    INSERT INTO {schema}.TaxRecord (Party_ID, Insert_Time)
    SELECT DISTINCT TI.Party_ID, NOW()
    FROM {schema}.TaxInfo TI
    WHERE NOT EXISTS (
      SELECT 1
      FROM {schema}.TaxRecord TR
      WHERE TR.Party_ID = TI.Party_ID
    )
    """

    try:
        with conn.cursor() as cur:
            inserted = cur.execute(sql)

        conn.commit()

    except pymysql_err.ProgrammingError as e:
        if getattr(e, "args", []) and len(e.args) >= 1 and int(e.args[0]) == 1146:
            logger.warning(f"⚠️ TaxRecord 缺表，跳過補寫：{e}")
            conn.rollback()
            return 0

        raise

    logger.info(f"✅ TaxRecord 新增 Party_ID：{inserted}")

    return int(inserted)