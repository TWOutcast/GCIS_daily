# -*- coding: utf-8 -*-
"""run_daily_job_v3.py

v3 目標：
- 保留「raw data 先入庫」概念（crawlerdb.tmp_rawData 新表）
- Tmp_TaxInfo / TaxInfo 走舊版 DDL
- CLI 支援兩種模式
  1) 日批：自動下載 + 解壓 + 入庫 raw + ETL + tmp->main
  2) 指定 CSV：跳過下載/解壓，直接對既有 CSV 做 raw 入庫 + ETL + tmp->main

日批的短版流程：
START -> (download/extract or use --csv) -> TRUNCATE tmp_rawData + Tmp_TaxInfo
-> load tmp_rawData -> META date -> validate -> ETL -> load Tmp_TaxInfo
-> count check (raw(DATA) vs Tmp_TaxInfo)
-> tmp->main (差異寫入 TaxInfo) -> ensure TaxRecord
-> CLEANUP (TRUNCATE tmp_rawData + Tmp_TaxInfo) -> END
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime

# 可選：支援 .env（若你習慣用 dotenv 管理環境變數）
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    # 沒裝 python-dotenv 或不使用 .env 都 OK
    pass

from db_loader_v5 import (
    connect_mysql,
    get_logger,
    get_mysql_settings_from_env,
    truncate_legacy_tmp_taxinfo,
    truncate_tmp_rawdata,
    count_rawdata_data_rows,
    count_legacy_tmp_taxinfo,
    merge_diff_tmp_to_main_taxinfo,
    ensure_taxrecord_for_new_party_ids,
)
from crawler_etl_v4 import (
    SOURCE_URL,
    download_and_extract,
    csv_to_tmp_rawdata,
    validate_file_date_or_raise,
    rawdata_to_legacy_tmp_taxinfo,
)


def _make_run_id(now: datetime) -> str:
    return f"RUN_{now.strftime('%Y%m%d_%H%M%S')}"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="GCIS 稅籍日檔 pipeline (v3)")

    p.add_argument(
        "--work-dir",
        default=os.path.join(os.path.dirname(__file__), "work"),
        help="工作目錄（下載/解壓/臨時檔）",
    )
    p.add_argument(
        "--csv",
        default=None,
        help="指定已存在的 CSV 檔案路徑（提供此參數會跳過下載/解壓）",
    )
    p.add_argument(
        "--skip-date-check",
        action="store_true",
        help="略過檔案日期檢查（等同 STRICT_FILE_DATE=0）",
    )
    p.add_argument(
        "--no-cleanup",
        action="store_true",
        help="不清空 tmp_rawData/Tmp_TaxInfo（除錯用；正式日批請勿開）",
    )

    return p.parse_args()


def main() -> None:
    args = _parse_args()
    logger = get_logger()

    now = datetime.now()
    run_id = _make_run_id(now)
    logger.info(f"START | run_id={run_id}")

    if args.skip_date_check:
        os.environ["STRICT_FILE_DATE"] = "0"

    cfg = get_mysql_settings_from_env()
    conn = connect_mysql(cfg)

    zip_path = "MANUAL"
    csv_path = None
    downloaded_at = now

    try:
        logger.info("=== 開始：raw 入庫 + ETL + tmp->main ===")

        # 0) 準備日批暫存：先清空（確保本次執行是乾淨的）
        truncate_tmp_rawdata(conn, logger=logger)
        truncate_legacy_tmp_taxinfo(conn, logger=logger)

        # 1) 取得 CSV
        if args.csv:
            zip_path = "(manual)"
            downloaded_at = datetime.now()
            csv_path = os.path.abspath(args.csv)
            if not os.path.isfile(csv_path):
                raise FileNotFoundError(f"找不到 CSV：{csv_path}")
            logger.info(f"使用既有 CSV：{csv_path} (跳過下載/解壓)")
        else:
            zip_path, csv_path, downloaded_at = download_and_extract(args.work_dir)
            logger.info(f"✅ 下載完成：{zip_path}")
            logger.info(f"✅ 解壓完成：{csv_path}")

        # 2) CSV -> tmp_rawData（新表）
        #    注意：tmp_rawData 表的 local_zip_path / local_csv_path 欄位為 NOT NULL，
        #    即使是手動指定 CSV，也要塞一個可追溯的值。
        file_date, raw_inserted = csv_to_tmp_rawdata(
            conn,
            run_id=run_id,
            source_url=SOURCE_URL,
            local_zip_path=zip_path or "(manual)",
            local_csv_path=csv_path,
            downloaded_at=downloaded_at,
        )
        logger.info(f"✅ META 日期解析：{file_date}")

        # 3) 檔案日期驗證
        validate_file_date_or_raise(file_date)

        # 4) tmp_rawData -> ETL -> Tmp_TaxInfo（舊表）
        clean_cnt = rawdata_to_legacy_tmp_taxinfo(conn, run_id=run_id, source_file_date=file_date)
        logger.info(f"✅ ETL 完成：Tmp_TaxInfo 筆數={clean_cnt}")

        # 5) 核對 raw(DATA) vs Tmp_TaxInfo
        raw_data_cnt = count_rawdata_data_rows(conn, run_id=run_id, logger=logger)
        tmp_cnt = count_legacy_tmp_taxinfo(conn, logger=logger)
        logger.info(f"[核對] tmp_rawData(DATA)={raw_data_cnt}，Tmp_TaxInfo={tmp_cnt}")

        # 6) tmp vs main：只寫入新增與異動（append 到 TaxInfo）
        merge_result = merge_diff_tmp_to_main_taxinfo(conn, logger=logger)
        logger.info(f"✅ main merge 結果：{merge_result}")

        # 7) 補 TaxRecord
        tr_inserted = ensure_taxrecord_for_new_party_ids(conn, logger=logger)
        logger.info(f"✅ TaxRecord補齊：inserted={tr_inserted}")

    finally:
        # 8) 清空暫存（正式日批需要）
        if conn:
            try:
                if not args.no_cleanup:
                    truncate_tmp_rawdata(conn, logger=logger)
                    truncate_legacy_tmp_taxinfo(conn, logger=logger)
                else:
                    logger.warning("⚠️ 已設定 --no-cleanup：暫存表不清空")
            finally:
                conn.close()

        logger.info(f"END | run_id={run_id}")


if __name__ == "__main__":
    main()
