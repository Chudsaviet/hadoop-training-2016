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
    PAYING_PRICE            BIGINT,
    CREATIVE_ID             VARCHAR(32),
    BIDDING_PRICE           BIGINT,
    ADVERTISER_ID           VARCHAR(100),
    USER_TAGS               VARCHAR(100),
    STREAM_ID               BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
location '/dataset'
;

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
    PAYING_PRICE,
    CREATIVE_ID,
    BIDDING_PRICE,
    ADVERTISER_ID,
    USER_TAGS,
    STREAM_ID,
    INPUT__FILE__NAME AS INPUT_FILE_NAME
FROM
    STG_CLICK_FACT
;

-- Calculate Bid Flow (point 01) report
DROP TABLE IF EXISTS BID_FLOW;
CREATE TABLE BID_FLOW AS
SELECT
    CLICK_DT,
    SUM(BIDDING_PRICE) AS BIDDING_PRICE,
    SUM(PAYING_PRICE)  AS PAYING_PRICE
FROM CLICK_FACT
WHERE
    STREAM_ID = 1
GROUP BY CLICK_DT
ORDER BY CLICK_DT
;

-- Calculate User-Date-Clicks table
DROP TABLE IF EXISTS USER_DATE_CLICKS;
CREATE TABLE USER_DATE_CLICKS AS
SELECT
    IPINYOU_ID,
    CLICK_DT,
    COUNT(*) AS CLICKS
FROM
    CLICK_FACT
GROUP BY
    IPINYOU_ID,
    CLICK_DT
;

-- Collect all dates
DROP TABLE IF EXISTS DAYS;
CREATE TABLE DAYS AS 
SELECT
    CLICK_DT AS DAY
FROM
    USER_DATE_CLICKS
GROUP BY
    CLICK_DT
-- DEBUG WHERE CLAUSE
WHERE
    CLICK_DT = CAST('2016-06-12' AS DATE)
;

-- Calculate User-Date-Category table
DROP TABLE IF EXISTS USER_DAY_CATEGORY;
CREATE TABLE USER_DAY_CATEGORY AS
SELECT
    udc.IPINYOU_ID,
    days.DAY,
    udc.CLICK_DT,
    udc.CLICKS,
    LAG(udc.CLICKS,1,0) OVER (PARTITION BY udc.IPINYOU_ID, days.DAY order by udc.CLICK_DT) AS PREV_CLICKS
    
FROM
    USER_DATE_CLICKS AS udc
INNER JOIN
    DAYS AS days
ON (
    1 = 1
)
;

SELECT * FROM USER_DAY_CATEGORY
ORDER BY IPINYOU_ID,DAY,CLICK_DT;