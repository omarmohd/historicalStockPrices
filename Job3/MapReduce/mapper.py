#!/usr/bin/env python3
import sys
# Serve per creare dizionari annidati
class AutoTree(dict):
    def __missing__(self, key):
        value = self[key] = type(self)()
        return value


# VARIABILI GLOBALI:

# dizionario che immagizzina azione > mese > data iniziale, data finale, valori
stock_mese = {}

# serve da iteratore per i mesi
months = ['GEN', 'FEB', 'MAR', 'APR', 'MAG', 'GIU',
          'LUG', 'AGO', 'SET', 'OTT', 'NOV', 'DIC']


def create_stock_info(map, stock, data, valore_chiusura):
    mese = str(data[5:7])

    if stock in map:
        if mese in map[stock]:
            if data < map[stock][mese]["data_inizio"]["data"]:
                map[stock][mese]["data_inizio"]["data"] = data
                map[stock][mese]["data_inizio"]["valore"] = valore_chiusura
            if data > map[stock][mese]["data_fine"]["data"]:
                map[stock][mese]["data_fine"]["data"] = data
                map[stock][mese]["data_fine"]["valore"] = valore_chiusura

        else:
            stock_info_to_add = {
                "data_inizio": {"data": data, "valore": valore_chiusura},
                "data_fine": {"data": data, "valore": valore_chiusura},
            }
            map[stock][mese] = stock_info_to_add
    else:
        map[stock] = {}
        stock_info_to_add = {
            "data_inizio": {"data": data, "valore": valore_chiusura},
            "data_fine": {"data": data, "valore": valore_chiusura},
        }
        map[stock][mese] = stock_info_to_add
    return map

stock_monthly_variation = AutoTree()
csvIn = sys.stdin
for line in csvIn:
    line_input = line.strip().split(",")

    # solo anno 2017 è d'interesse
    if line_input[7][0:4] == "2017":
        # print('{}\t{}\t{}'.format(input[0], input[2], input[7]))
        # values = line.strip().split('\t')

        # stock, data, valore_chiusura = values[0], values[7], values[1]
        stock, data, valore_chiusura = (
            line_input[0],
            line_input[7],
            float(line_input[1]),
        )
        monthly_stock = create_stock_info(stock_mese, stock, data, valore_chiusura)

# calcolo variazione mensile per ogni azione, in base alle date
for stock in monthly_stock:
    for mese in monthly_stock[stock]:
        variazione = (((monthly_stock[stock][mese]["data_fine"]["valore"]) * 100)
                      / monthly_stock[stock][mese]["data_inizio"]["valore"]) - 100
        stock_monthly_variation[stock][mese] = round(variazione, 2)

# tengo solo azioni con 12 mesi
for stock in list(stock_monthly_variation):
    if len(stock_monthly_variation[stock].keys()) != 12:
        del stock_monthly_variation[stock]

for stock in stock_monthly_variation:
    line_to_print = []
    line_to_print.append(stock)
    for month in stock_monthly_variation[stock]:
        line_to_print.append(month)
        line_to_print.append(stock_monthly_variation[stock][month])
    print(*line_to_print, sep='\t')
