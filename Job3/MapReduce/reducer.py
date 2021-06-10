#!/usr/bin/env python3
import sys
# Serve per creare dizionari annidati
class AutoTree(dict):
    def __missing__(self, key):
        value = self[key] = type(self)()
        return value

months = ['GEN', 'FEB', 'MAR', 'APR', 'MAG', 'GIU',
          'LUG', 'AGO', 'SET', 'OTT', 'NOV', 'DIC']


# INIZIO REDUCER
stock_monthly_variation = AutoTree()
for line in sys.stdin:
    # Remove useless whitespaces at the beginning and at the end of the line
    line_input = line.strip().split("\t")
    for month in months:
        index = months.index(month)
        stock_monthly_variation[line_input[0]][month] = line_input[2+(index*2)]

# Mi scorro gli stock e vedo quali, per tutti i 12 mesi hanno avuto una differenza di variazione di 1,5%
similar_trend = AutoTree()
similar=0
for stock in stock_monthly_variation:
    for stock_2 in stock_monthly_variation:
        # escludo di confrontare la stessa azione
        if stock != stock_2:
            similar = 1
        for month in months:
            difference = float(stock_monthly_variation[stock][month]) - float(stock_monthly_variation[stock_2][month])
            # esco dall'analisi se anche un mese sfora la soglia dell'1,5%
            if difference < -0.5 or difference > 0.5:
                similar = 0
        if similar:
            stocks_couple = stock + ' - ' + stock_2
            # il test serve ad evitare di includere nell'output stesse coppie ma in ordine inverso
            test_stocks_couple = stock_2 + ' - ' + stock
            if test_stocks_couple not in similar_trend.keys():
                for iterator in months:
                    similar_trend[stocks_couple][iterator] = [stock_monthly_variation[stock][iterator], stock_monthly_variation[stock_2][iterator]]

print(similar_trend)
