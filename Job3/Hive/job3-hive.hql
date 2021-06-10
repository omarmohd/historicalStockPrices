DROP TABLE hsp;
DROP TABLE stock_2017;
DROP TABLE stock_2017_max_date;
DROP TABLE stock_2017_min_date;
DROP TABLE stock_2017_monthly_variations;
DROP TABLE stock_couples;
DROP TABLE stock_couples_filtered;

CREATE TABLE hsp (nome STRING, apertura FLOAT, chiusura FLOAT, chiusura_2 FLOAT, minimo FLOAT, massimo FLOAT, volume INTEGER, data DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'historical_stock_prices.csv' OVERWRITE INTO TABLE hsp;


CREATE TABLE stock_2017 AS
SELECT hsp.nome, hsp.chiusura, 
MONTH(hsp.data) as mese, DAY(hsp.data) as giorno
FROM hsp
WHERE YEAR(hsp.data) = 2017;


CREATE TABLE stock_2017_max_date AS
SELECT DISTINCT a.nome, a.mese, a.giorno, a.chiusura
FROM stock_2017 a
INNER JOIN (
    SELECT max(giorno) as giorno, mese,
        nome
    FROM stock_2017
    GROUP BY nome, mese
) b
ON (a.nome = b.nome
    AND a.giorno = b.giorno
AND b.mese = a.mese);


CREATE TABLE stock_2017_min_date AS
SELECT DISTINCT a.nome, a.mese, a.giorno, a.chiusura
FROM stock_2017 a
INNER JOIN (
    SELECT min(giorno) as giorno, mese,
        nome
    FROM stock_2017
    GROUP BY nome, mese
) b
ON (a.nome = b.nome
    AND a.giorno = b.giorno
AND b.mese = a.mese);


CREATE TABLE stock_2017_monthly_variations AS
SELECT a.nome, a.mese,
((b.chiusura-a.chiusura)*100)/a.chiusura as variation
FROM stock_2017_min_date a, stock_2017_max_date b 
WHERE a.nome = b.nome AND a.mese=b.mese;


CREATE TABLE stock_couples as
SELECT a.nome, b.nome as nome2, a.mese, 
a.variation as var, b.variation as var2
FROM stock_2017_monthly_variations a inner join
     stock_2017_monthly_variations b
     WHERE a.mese=b.mese
     AND a.nome<>b.nome
     AND (a.variation-b.variation) BETWEEN -0.5 AND 0.5
     ORDER by a.nome, nome2, a.mese;


CREATE TABLE stock_couples_filtered as
SELECT a.nome, a.nome2, count(a.mese) as tot_mesi
FROM stock_couples a
GROUP BY a.nome, a.nome2
HAVING count(a.mese)=12;


SELECT A.* 
FROM stock_couples A
    JOIN stock_couples_filtered B ON 
	((A.nome = B.nome) AND (A.nome2=B.nome2))
ORDER BY a.nome, a.nome2, a.mese
LIMIT 10;
