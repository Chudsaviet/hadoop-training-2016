-----------------------------------------------------------
---------- Hive Homework 01 - Timofei Korostelev ----------
-----------------------------------------------------------

-- Create staging fact table as an external table
DROP TABLE IF EXISTS STG_CLICK_FACT;
CREATE EXTERNAL TABLE STG_CLICK_FACT(
    BID_ID                  VARCHAR(32),
    CLICK_DTTM             VARCHAR(30),
    IPINYOU_ID              VARCHAR(30),
    USER_AGENT              VARCHAR(2000),
    IP                      VARCHAR(16),
    REGION                  BIGINT,
    CITY                    BIGINT,
    AD_EXCHANGE             BIGINT,
    DOMAIN_ID               VARCHAR(100),
    URL                     VARCHAR(2000),
    ANONYMOUS_URL_ID        VARCHAR(2000),
    AD_SLOT_ID              VARCHAR(200),
    AD_SLOT_WIDTH           BIGINT,
    AD_SLOT_HEIGHT          BIGINT,
    AD_SLOT_VISIBILITY      BIGINT,
    AD_SLOT_FORMAT          BIGINT,
    AD_SLOT_FLOOR_PRICE     BIGINT,
    CREATIVE_ID             VARCHAR(32),
    BIDDING_PRICE           BIGINT,
    ADVERTISER_ID           VARCHAR(100),
    USER_TAGS               VARCHAR(100),
    STREAM_ID               BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
location '/dataset';

-- Create view to implicitly parse timestamps
DROP VIEW IF EXISTS CLICK_FACT;
CREATE VIEW CLICK_FACT AS 
SELECT
    BID_ID,
    CAST(to_date(from_unixtime(unix_timestamp(CLICK_DTTM,'yyyyMMddHHmmssSSS'))) AS DATE) AS CLICK_DT,
    CAST(from_unixtime(unix_timestamp(CLICK_DTTM,'yyyyMMddHHmmssSSS')) AS TIMESTAMP) AS CLICK_DTTM,
    IPINYOU_ID,
    USER_AGENT,
    IP,
    REGION,
    CITY,
    AD_EXCHANGE,
    DOMAIN_ID,
    URL,
    ANONYMOUS_URL_ID,
    AD_SLOT_ID,
    AD_SLOT_WIDTH,
    AD_SLOT_HEIGHT,
    AD_SLOT_VISIBILITY,
    AD_SLOT_FORMAT,
    AD_SLOT_FLOOR_PRICE,
    CREATIVE_ID,
    BIDDING_PRICE,
    ADVERTISER_ID,
    USER_TAGS,
    STREAM_ID,
    INPUT__FILE__NAME AS INPUT_FILE_NAME
FROM STG_CLICK_FACT;

-- Calculate Bid Flow (point 01) report
CREATE TABLE BID_FLOW AS
SELECT
    CLICK_DT,
    SUM(BIDDING_PRICE) AS BIDDING_PRICE
FROM CLICK_FACT
WHERE
    STREAM_ID = 1
GROUP BY CLICK_DT
ORDER BY CLICK_DT;