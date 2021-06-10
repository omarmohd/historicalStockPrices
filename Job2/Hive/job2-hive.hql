DROP TABLE IF EXISTS prices;
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS joinTable;
DROP TABLE IF EXISTS sumOfTickerVolume;
DROP TABLE IF EXISTS maxTickerVolume;
DROP TABLE IF EXISTS dateMinMax;
DROP TABLE IF EXISTS minClose;
DROP TABLE IF EXISTS minCloseTicker;
DROP TABLE IF EXISTS maxClose;
DROP TABLE IF EXISTS maxCloseTicker;
DROP TABLE IF EXISTS percentChange;
DROP TABLE IF EXISTS percentChangeTicker;
DROP TABLE IF EXISTS maxTickerPercentChange;


--create the two input table
CREATE TABLE prices(ticker STRING,open DOUBLE, close DOUBLE, adj DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, data DATE) row format delimited fields terminated by ',';
CREATE TABLE stocks(ticker STRING, exch STRING, name STRING, sector STRING, industry STRING)ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

-- load the CSV (original and modified)
LOAD DATA LOCAL INPATH 'historical_stock_prices.csv' OVERWRITE INTO TABLE prices;
LOAD DATA LOCAL INPATH 'historical_stocks.csv' OVERWRITE INTO TABLE stocks;


CREATE TABLE joinTable as
SELECT
    prices.ticker,
    prices.close,
    prices.volume,
    prices.data,
    stocks.sector
FROM prices JOIN stocks
ON prices.ticker = stocks.ticker
WHERE YEAR(prices.data)>='2009' AND YEAR(prices.data)<='2018' AND stocks.sector!='N/A';


--per ogni settore, per ogni anno, per ogni ticker ho il loro peso totale
CREATE TABLE sumOfTickerVolume AS
SELECT
    sector,
    YEAR(data) AS anno,
    ticker,
    SUM(volume) AS totTickerVolume
FROM joinTable
GROUP BY sector, YEAR(data), ticker;


--prendo solo il ticker con il peso maggiore
CREATE TABLE maxTickerVolume AS
SELECT m.*
FROM sumOfTickerVolume m
    LEFT JOIN sumOfTickerVolume b
        ON m.sector = b.sector AND m.anno = b.anno
        AND m.totTickerVolume < b.totTickerVolume
WHERE b.totTickerVolume IS NULL;


--per ogni anno, per ogni settore, per ogni ticker ho la data minima e massima
CREATE TABLE dateMinMax AS
SELECT
    sector,
    ticker,
    min(TO_DATE(data)) AS min_data,
    max(TO_DATE(data)) AS max_data
FROM joinTable
GROUP BY sector, ticker, YEAR(data);


--per ogni ogni anno, per ogni settore ho prezzo di chiusura minimo
CREATE TABLE minClose AS
SELECT
    b.sector,
    YEAR(b.min_data) AS anno,
    SUM(a.close) AS min_close
FROM joinTable AS a, dateMinMax AS b
WHERE a.sector=b.sector AND a.data=b.min_data AND b.ticker=a.ticker
GROUP BY b.sector, YEAR(b.min_data);

--per ogni ogni anno, per ogni settore, per ogni ticker ho prezzo di chiusura minimo
CREATE TABLE minCloseTicker AS
SELECT
    b.sector,
    YEAR(b.min_data) AS anno,
    b.ticker,
    SUM(a.close) AS min_close
FROM joinTable AS a, dateMinMax AS b
WHERE a.sector=b.sector AND a.data=b.min_data AND b.ticker=a.ticker
GROUP BY b.sector, YEAR(b.min_data), b.ticker;


--per ogni ogni anno, per ogni settore ho prezzo di chiusura massimo
CREATE TABLE maxClose AS
SELECT
    b.sector,
    YEAR(b.max_data) AS anno,
    SUM(a.close) AS max_close
FROM joinTable AS a, dateMinMax AS b
WHERE a.sector=b.sector AND a.data=b.max_data AND b.ticker=a.ticker
GROUP BY b.sector, YEAR(b.max_data);

--per ogni ogni anno, per ogni settore, per ogni ticker ho prezzo di chiusura massimo
CREATE TABLE maxCloseTicker AS
SELECT
    b.sector,
    YEAR(b.max_data) AS anno,
    b.ticker,
    SUM(a.close) AS max_close
FROM joinTable AS a, dateMinMax AS b
WHERE a.sector=b.sector AND a.data=b.max_data AND b.ticker=a.ticker
GROUP BY b.sector, YEAR(b.max_data), b.ticker;


CREATE TABLE percentChange AS
SELECT
    min.sector,
    min.anno,
    ROUND(((max.max_close-min.min_close)/min.min_close) * 100, 2) AS variazioneAnnuale
FROM minClose AS min, maxClose AS max
WHERE min.sector=max.sector AND min.anno=max.anno
ORDER BY sector, anno;

--per ogni anno, per ogni settore, per ogni ticker ho la variazione percentuale
CREATE TABLE percentChangeTicker AS
SELECT
    min.sector,
    min.anno,
    min.ticker,
    ROUND(((max.max_close-min.min_close)/min.min_close) * 100, 2) AS variazioneAnnualeTicker
FROM minCloseTicker AS min, maxCloseTicker AS max
WHERE min.sector=max.sector AND min.anno=max.anno AND min.ticker=max.ticker
ORDER BY sector, anno;


--prendo solo il ticker con la variazione maggiore
CREATE TABLE maxTickerPercentChange AS
SELECT m.*
FROM percentChangeTicker m
    LEFT JOIN percentChangeTicker b
        ON m.sector = b.sector AND m.anno = b.anno
        AND m.variazioneAnnualeTicker < b.variazioneAnnualeTicker
WHERE b.variazioneAnnualeTicker IS NULL;


--query finale
SELECT
    a.sector,
    a.anno,
    a.variazioneAnnuale,
    b.ticker AS tickerVariazioneAnnualeMax,
    b.variazioneAnnualeTicker,
    c.ticker AS tickerVolumeMax,
    c.totTickerVolume
FROM percentChange AS a, maxTickerPercentChange AS b, maxTickerVolume AS c
WHERE a.sector=b.sector AND b.sector=c.sector AND a.anno=b.anno AND c.anno=b.anno
ORDER BY sector, anno
LIMIT 10;
