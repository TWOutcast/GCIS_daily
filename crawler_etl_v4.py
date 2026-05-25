# -*- coding: utf-8 -*-
"""crawler_etl_v3.py

重點：保留我們新增的 tmp_rawData 概念，但下游 tmp/main table 要走「舊版 DDL」。

流程：
1) 下載 ZIP / 解壓 CSV（檔名加時間戳）或指定既有 CSV
2) CSV 第 2 行（META 行）日期解析 + 驗證
3) 原封不動寫入 crawlerdb.tmp_rawData（新表）
4) 由 tmp_rawData 做 ETL -> 寫入 crawlerdb.Tmp_TaxInfo（舊表，日批暫存）

注意：
- tmp_rawData / Tmp_TaxInfo 的 TRUNCATE 由 run_daily_job_v3.py 控制（跑完會清空）。
"""

from __future__ import annotations

import csv
import os
import re
import shutil
import time
import zipfile
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import certifi
import requests
import urllib3

from db_loader_v5 import (
    get_logger,
    connect_mysql,
    get_mysql_settings_from_env,
    insert_tmp_rawdata,
    insert_legacy_tmp_taxinfo,
)

SOURCE_URL = "https://eip.fia.gov.tw/data/BGMOPEN1.zip"
CSV_COLS = 16

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def normalize_text_keep_spaces(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    s = re.sub(r"[\u3000]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s or None


def only_digits(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    d = "".join(ch for ch in str(s) if ch.isdigit())
    return d or None


def to_int_or_none(v: Optional[str]) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    return int(s) if s.isdigit() else None


def roc_yyyMMdd_to_date(v: Optional[str]) -> Optional[date]:
    if not v:
        return None

    s = str(v).strip()

    if not re.fullmatch(r"\d{7}", s):
        return None

    y = int(s[:3]) + 1911
    m = int(s[3:5])
    d = int(s[5:7])

    try:
        return date(y, m, d)
    except ValueError:
        return None


def parse_meta_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None

    s = str(s).strip()

    try:
        return datetime.strptime(s, "%d-%b-%y").date()
    except ValueError:
        return None


def _format_bytes(num_bytes: int) -> str:
    mb = num_bytes / 1024 / 1024
    return f"{mb:.2f} MB"


def _log_remote_file_size(
    url: str,
    *,
    logger,
    timeout: int,
    verify: Any,
) -> None:
    try:
        r = requests.head(
            url,
            timeout=timeout,
            verify=verify,
            allow_redirects=True,
        )

        content_length = r.headers.get("Content-Length")

        if content_length and content_length.isdigit():
            logger.info(f"準備下載 ZIP 大小：{_format_bytes(int(content_length))}")
        else:
            logger.info("準備下載 ZIP 大小：伺服器未提供 Content-Length")

    except requests.exceptions.SSLError:
        pass

    except Exception as e:
        logger.info(f"無法取得 ZIP 大小，略過 size log：{e}")


def _download_file(
    url: str,
    dst_path: str,
    *,
    logger,
    timeout: int = 120,
    max_retry: int = 3,
) -> None:
    allow_insecure = (
        os.getenv("ALLOW_INSECURE_SSL", "1")
        .strip()
        .lower()
        not in {"0", "false", "no", "off"}
    )

    last_err: Optional[Exception] = None

    for attempt in range(1, max_retry + 1):
        is_last = attempt == max_retry

        verify: Any = certifi.where()
        verify_label = "certifi"

        if is_last and allow_insecure:
            verify = False
            verify_label = "INSECURE(verify=False)"

        try:
            logger.info(
                f"下載中：{url} "
                f"(attempt={attempt}/{max_retry}, verify={verify_label})"
            )

            _log_remote_file_size(
                url,
                logger=logger,
                timeout=timeout,
                verify=verify,
            )

            with requests.get(
                url,
                stream=True,
                timeout=timeout,
                verify=verify,
            ) as r:
                r.raise_for_status()

                with open(dst_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

            local_size = os.path.getsize(dst_path)
            logger.info(f"✅ 下載成功：{dst_path}")
            logger.info(f"✅ 本機 ZIP 大小：{_format_bytes(local_size)}")

            return

        except requests.exceptions.SSLError as e:
            last_err = e

        except Exception as e:
            last_err = e
            logger.warning(f"下載失敗：{e}")

        time.sleep(min(5, 1 + attempt))

    raise RuntimeError(f"下載失敗（重試 {max_retry} 次仍失敗）：{last_err}")


def _log_csv_meta_date(csv_path: str, logger) -> None:
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)

            next(reader, None)
            meta_row = next(reader, None)

        if meta_row and len(meta_row) > 0:
            meta_raw = str(meta_row[0]).strip()
            parsed_date = parse_meta_date(meta_raw)

            if parsed_date:
                logger.info(f"CSV META 日期：{meta_raw} / parsed={parsed_date}")
            else:
                logger.warning(f"CSV META 日期解析失敗：{meta_raw}")
        else:
            logger.warning("CSV META 日期讀取失敗：找不到第2行")

    except Exception as e:
        logger.warning(f"CSV META 日期讀取失敗：{e}")


def download_and_extract(work_dir: str) -> Tuple[str, str, datetime]:
    logger = get_logger()

    os.makedirs(work_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    zip_name = f"BGMOPEN1_{ts}.zip"
    csv_name = f"BGMOPEN1_{ts}.csv"

    zip_path = os.path.join(work_dir, zip_name)
    csv_path = os.path.join(work_dir, csv_name)

    downloaded_at = datetime.now()

    _download_file(
        SOURCE_URL,
        zip_path,
        logger=logger,
        timeout=120,
        max_retry=3,
    )

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()

        target = None
        for m in members:
            if m.lower().endswith("bgmopen1.csv"):
                target = m
                break

        if not target:
            raise RuntimeError(f"ZIP 內找不到 BGMOPEN1.csv，內容：{members[:20]}")

        extracted_path = zf.extract(target, path=work_dir)
        shutil.move(extracted_path, csv_path)

    csv_size = os.path.getsize(csv_path)

    logger.info(f"✅ 解壓完成：{csv_path}")
    logger.info(f"✅ 本機 CSV 大小：{_format_bytes(csv_size)}")

    _log_csv_meta_date(csv_path, logger)

    return zip_path, csv_path, downloaded_at


def csv_to_tmp_rawdata(
    conn,
    *,
    run_id: str,
    source_url: str,
    local_zip_path: str | None = None,
    local_csv_path: str | None = None,
    downloaded_at: datetime | None = None,
    zip_path: str | None = None,
    csv_path: str | None = None,
) -> Tuple[date, int]:
    logger = get_logger()

    if local_zip_path is None and zip_path is not None:
        local_zip_path = zip_path

    if local_csv_path is None and csv_path is not None:
        local_csv_path = csv_path

    if not local_csv_path:
        raise ValueError("local_csv_path / csv_path 必填")

    if local_zip_path is None:
        local_zip_path = ""

    if downloaded_at is None:
        downloaded_at = datetime.now()

    raw_rows: List[Tuple[Any, ...]] = []
    file_date: Optional[date] = None
    data_cnt_with_party_id = 0

    with open(local_csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)

        for idx, row in enumerate(reader, start=1):
            row = (row + [""] * CSV_COLS)[:CSV_COLS]

            if idx == 1:
                row_type = "HEADER"

            elif idx == 2:
                row_type = "META"
                file_date = parse_meta_date(row[0])
                logger.info(f"CSV META 原始日期：{row[0]}")

            else:
                row_type = "DATA"
                party_id = normalize_text_keep_spaces(row[1])

                if party_id:
                    data_cnt_with_party_id += 1

            raw_rows.append(
                (
                    run_id,
                    source_url,
                    local_zip_path,
                    local_csv_path,
                    downloaded_at.strftime("%Y-%m-%d %H:%M:%S"),
                    file_date.strftime("%Y-%m-%d") if file_date else None,
                    idx,
                    row_type,
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11],
                    row[12],
                    row[13],
                    row[14],
                    row[15],
                )
            )

    insert_tmp_rawdata(conn, raw_rows, logger=logger)

    if not file_date:
        raise RuntimeError(
            "CSV META 行日期解析失敗：第2行第1欄不是預期格式，例如 20-MAY-26。"
        )

    logger.info(f"CSV DATA rows with Party_ID：{data_cnt_with_party_id}")

    return file_date, data_cnt_with_party_id


def rawdata_to_legacy_tmp_taxinfo(
    conn,
    *,
    run_id: str,
    source_file_date: date,
) -> int:
    logger = get_logger()

    sql = """
    SELECT row_num,
           c01 AS party_addr,
           c02 AS party_id,
           c03 AS parent_party_id,
           c04 AS party_name,
           c05 AS paidin_capital,
           c06 AS setup_date,
           c07 AS party_type,
           c08 AS use_invoice,
           c09 AS ind_code,
           c10 AS ind_name,
           c11 AS ind_code1,
           c12 AS ind_name1,
           c13 AS ind_code2,
           c14 AS ind_name2,
           c15 AS ind_code3,
           c16 AS ind_name3
    FROM crawlerdb_v2.tmp_rawData
    WHERE run_id=%s AND row_type='DATA'
    ORDER BY row_num
    """

    clean_rows: List[Dict[str, Any]] = []

    with conn.cursor() as cur:
        cur.execute(sql, (run_id,))

        for r in cur.fetchall():
            party_id = normalize_text_keep_spaces(r.get("party_id"))

            if not party_id:
                continue

            setup_dt = roc_yyyMMdd_to_date(r.get("setup_date"))
            setup_date_str = setup_dt.strftime("%Y-%m-%d") if setup_dt else None

            clean_rows.append(
                {
                    "party_addr": normalize_text_keep_spaces(r.get("party_addr")),
                    "party_id": party_id,
                    "parent_party_id": normalize_text_keep_spaces(r.get("parent_party_id")),
                    "party_name": normalize_text_keep_spaces(r.get("party_name")),
                    "paidin_capital": to_int_or_none(r.get("paidin_capital")),
                    "setup_date": setup_date_str,
                    "party_type": normalize_text_keep_spaces(r.get("party_type")),
                    "use_invoice": normalize_text_keep_spaces(r.get("use_invoice")) or None,
                    "ind_code": normalize_text_keep_spaces(r.get("ind_code")),
                    "ind_name": normalize_text_keep_spaces(r.get("ind_name")),
                    "ind_code1": normalize_text_keep_spaces(r.get("ind_code1")),
                    "ind_name1": normalize_text_keep_spaces(r.get("ind_name1")),
                    "ind_code2": only_digits(r.get("ind_code2")),
                    "ind_name2": normalize_text_keep_spaces(r.get("ind_name2")),
                    "ind_code3": normalize_text_keep_spaces(r.get("ind_code3")),
                    "ind_name3": normalize_text_keep_spaces(r.get("ind_name3")),
                }
            )

    inserted = insert_legacy_tmp_taxinfo(conn, clean_rows, logger=logger)

    logger.info(f"✅ Tmp_TaxInfo 入庫完成：{inserted} 筆")

    return inserted


def validate_file_date_or_raise(source_file_date: date) -> None:
    logger = get_logger()

    strict = (
        os.getenv("STRICT_FILE_DATE", "1")
        .strip()
        .lower()
        not in {"0", "false", "no", "off"}
    )

    expect = (
        os.getenv("EXPECT_FILE_DATE")
        or os.getenv("BACKFILL_FILE_DATE")
        or ""
    ).strip()

    if not strict:
        logger.warning(
            f"⚠️ 已停用嚴格檔案日期檢查：source_file_date={source_file_date} "
            f"(STRICT_FILE_DATE=0)"
        )
        return

    if expect:
        try:
            expected_date = datetime.strptime(expect, "%Y-%m-%d").date()
        except ValueError:
            msg = f"❌ EXPECT_FILE_DATE 格式錯誤，需 YYYY-MM-DD，收到：{expect}"
            logger.error(msg)
            raise RuntimeError(msg)

        if source_file_date != expected_date:
            msg = (
                f"❌ 檔案日期不匹配：CSV第2行日期={source_file_date}，"
                f"期望日期={expected_date} (EXPECT_FILE_DATE)"
            )
            logger.error(msg)
            raise RuntimeError(msg)

        logger.info(
            f"✅ 檔案日期檢查通過：{source_file_date} "
            f"（符合 EXPECT_FILE_DATE={expected_date}）"
        )
        return

    today = datetime.now().date()

    if source_file_date != today:
        msg = f"❌ 檔案日期不匹配：CSV第2行日期={source_file_date}，批次執行日={today}"
        logger.error(msg)
        raise RuntimeError(msg)

    logger.info(f"✅ 檔案日期檢查通過：{source_file_date}（與批次執行日一致）")


def run_download_raw_etl(work_dir: str, run_id: str) -> Dict[str, Any]:
    logger = get_logger()
    logger.info("=== 開始：下載 + raw 入庫 + ETL ===")

    zip_path, csv_path, downloaded_at = download_and_extract(work_dir)

    logger.info(f"✅ 下載完成：{zip_path}")
    logger.info(f"✅ 解壓完成：{csv_path}")

    cfg = get_mysql_settings_from_env()
    conn = connect_mysql(cfg)

    try:
        file_date, raw_data_cnt = csv_to_tmp_rawdata(
            conn,
            run_id=run_id,
            source_url=SOURCE_URL,
            local_zip_path=zip_path,
            local_csv_path=csv_path,
            downloaded_at=downloaded_at,
        )

        logger.info(f"✅ META 日期解析：{file_date}")

        validate_file_date_or_raise(file_date)

        tmp_cnt = rawdata_to_legacy_tmp_taxinfo(
            conn,
            run_id=run_id,
            source_file_date=file_date,
        )

        return {
            "run_id": run_id,
            "zip_path": zip_path,
            "csv_path": csv_path,
            "downloaded_at": downloaded_at.strftime("%Y-%m-%d %H:%M:%S"),
            "file_date": file_date.strftime("%Y-%m-%d"),
            "raw_data_cnt": raw_data_cnt,
            "tmp_taxinfo_cnt": tmp_cnt,
        }

    finally:
        conn.close()
        logger.info("=== 結束：下載 + raw 入庫 + ETL ===")


def run_csv_raw_etl(csv_path: str, run_id: str) -> Dict[str, Any]:
    logger = get_logger()
    logger.info("=== 開始：CSV 模式 raw 入庫 + ETL（跳過下載） ===")

    csv_path = os.path.abspath(csv_path)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"找不到 CSV：{csv_path}")

    downloaded_at = datetime.now()
    local_zip_path = "MANUAL_CSV"

    csv_size = os.path.getsize(csv_path)
    logger.info(f"使用既有 CSV：{csv_path}")
    logger.info(f"本機 CSV 大小：{_format_bytes(csv_size)}")
    _log_csv_meta_date(csv_path, logger)

    cfg = get_mysql_settings_from_env()
    conn = connect_mysql(cfg)

    try:
        file_date, raw_data_cnt = csv_to_tmp_rawdata(
            conn,
            run_id=run_id,
            source_url="LOCAL_CSV",
            local_zip_path=local_zip_path,
            local_csv_path=csv_path,
            downloaded_at=downloaded_at,
        )

        logger.info(f"✅ META 日期解析：{file_date}")

        validate_file_date_or_raise(file_date)

        tmp_cnt = rawdata_to_legacy_tmp_taxinfo(
            conn,
            run_id=run_id,
            source_file_date=file_date,
        )

        return {
            "run_id": run_id,
            "zip_path": None,
            "csv_path": csv_path,
            "downloaded_at": downloaded_at.strftime("%Y-%m-%d %H:%M:%S"),
            "file_date": file_date.strftime("%Y-%m-%d"),
            "raw_data_cnt": raw_data_cnt,
            "tmp_taxinfo_cnt": tmp_cnt,
        }

    finally:
        conn.close()
        logger.info("=== 結束：CSV 模式 raw 入庫 + ETL ===")


def raw_to_legacy_tmp_etl(conn, run_id: str, source_file_date, logger=None) -> int:
    return rawdata_to_legacy_tmp_taxinfo(
        conn=conn,
        run_id=run_id,
        source_file_date=source_file_date,
    )