CREATE DATABASE IF NOT EXISTS crawlerdb_v2
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE crawlerdb_v2;

DROP TABLE IF EXISTS tmp_rawData;

CREATE TABLE tmp_rawData (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    run_id VARCHAR(40) NOT NULL,
    source_url VARCHAR(500) NOT NULL,
    local_zip_path VARCHAR(1000) NOT NULL,
    local_csv_path VARCHAR(1000) NOT NULL,
    downloaded_at DATETIME NOT NULL,
    file_date DATE NULL,
    row_num INT UNSIGNED NOT NULL,
    row_type ENUM('HEADER', 'META', 'DATA') NOT NULL,

    c01 TEXT NULL,
    c02 TEXT NULL,
    c03 TEXT NULL,
    c04 TEXT NULL,
    c05 TEXT NULL,
    c06 TEXT NULL,
    c07 TEXT NULL,
    c08 TEXT NULL,
    c09 TEXT NULL,
    c10 TEXT NULL,
    c11 TEXT NULL,
    c12 TEXT NULL,
    c13 TEXT NULL,
    c14 TEXT NULL,
    c15 TEXT NULL,
    c16 TEXT NULL,

    Insert_Time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    KEY idx_tmp_raw_run_type (run_id, row_type),
    KEY idx_tmp_raw_run_row (run_id, row_num),
    KEY idx_tmp_raw_file_date (file_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


DROP TABLE IF EXISTS Tmp_TaxInfo;

CREATE TABLE Tmp_TaxInfo (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    Party_Addr VARCHAR(500) NULL,
    Party_ID VARCHAR(20) NOT NULL,
    Parent_Party_ID VARCHAR(20) NULL,
    Party_Name VARCHAR(255) NULL,
    PaidIn_Capital BIGINT NULL,
    Setup_Date DATE NULL,
    Party_Type VARCHAR(100) NULL,
    Use_Invoice VARCHAR(50) NULL,

    Ind_Code VARCHAR(20) NULL,
    Ind_Name VARCHAR(255) NULL,
    Ind_Code1 VARCHAR(20) NULL,
    Ind_Name1 VARCHAR(255) NULL,
    Ind_Code2 VARCHAR(20) NULL,
    Ind_Name2 VARCHAR(255) NULL,
    Ind_Code3 VARCHAR(20) NULL,
    Ind_Name3 VARCHAR(255) NULL,

    Update_Time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    KEY idx_tmp_taxinfo_party_id (Party_ID),
    KEY idx_tmp_taxinfo_update_time (Update_Time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


DROP TABLE IF EXISTS TaxInfo;

CREATE TABLE TaxInfo (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    Party_ID VARCHAR(20) NOT NULL,
    Party_Addr VARCHAR(500) NULL,
    Parent_Party_ID VARCHAR(20) NULL,
    Party_Name VARCHAR(255) NULL,
    PaidIn_Capital BIGINT NULL,
    Setup_Date DATE NULL,
    Party_Type VARCHAR(100) NULL,
    Use_Invoice VARCHAR(50) NULL,

    Ind_Code VARCHAR(20) NULL,
    Ind_Name VARCHAR(255) NULL,
    Ind_Code1 VARCHAR(20) NULL,
    Ind_Name1 VARCHAR(255) NULL,
    Ind_Code2 VARCHAR(20) NULL,
    Ind_Name2 VARCHAR(255) NULL,
    Ind_Code3 VARCHAR(20) NULL,
    Ind_Name3 VARCHAR(255) NULL,

    Update_Time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    KEY idx_taxinfo_party_id (Party_ID),
    KEY idx_taxinfo_party_update (Party_ID, Update_Time),
    KEY idx_taxinfo_update_time (Update_Time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


DROP TABLE IF EXISTS TaxRecord;

CREATE TABLE TaxRecord (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    Party_ID VARCHAR(20) NOT NULL,
    Insert_Time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    UNIQUE KEY uk_taxrecord_party_id (Party_ID),
    KEY idx_taxrecord_insert_time (Insert_Time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
