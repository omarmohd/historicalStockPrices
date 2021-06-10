DROP TABLE hsp;
DROP TABLE stock_min_date;
DROP TABLE stock_max_date;
DROP TABLE stock_min_max_variation;
DROP TABLE stocks_min_max;


CREATE TABLE hsp (nome STRING, apertura FLOAT, chiusura FLOAT, chiusura_2 FLOAT, minimo FLOAT, massimo FLOAT, volume INTEGER, data DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'historical_stock_prices.csv' OVERWRITE INTO TABLE hsp;

CREATE TABLE stock_max_date AS
SELECT DISTINCT a.nome, a.data, a.chiusura
FROM hsp a
INNER JOIN (
    SELECT nome, max(data) as data
    FROM hsp
    GROUP BY nome
) b
ON (a.nome = b.nome
    AND a.data = b.data);


CREATE TABLE stock_min_date AS
SELECT DISTINCT a.nome, a.data, a.chiusura
FROM hsp a
INNER JOIN (
    SELECT nome, min(data) as data
    FROM hsp
    GROUP BY nome
) b
ON (a.nome = b.nome
    AND a.data = b.data);


CREATE TABLE stock_min_max_variation AS
SELECT a.nome,100*((b.chiusura - a.chiusura) / a.chiusura) as variation
FROM stock_min_date a, stock_max_date b
WHERE a.nome=b.nome;


CREATE TABLE stocks_min_max as
SELECT a.nome, max(a.data) as data_max, min(a.data) as data_min, 
max(a.massimo) as max_value, min(a.minimo) as min_value
FROM hsp a
GROUP BY a.nome
ORDER BY a.nome;


SELECT a.nome, a.data_max, a.data_min, b.variation, 
a.max_value, a.min_value
FROM stocks_min_max a, stock_min_max_variation b
WHERE a.nome=b.nome
GROUP BY a.nome, data_max, data_min, variation,
a.max_value, a.min_value
LIMIT 10;