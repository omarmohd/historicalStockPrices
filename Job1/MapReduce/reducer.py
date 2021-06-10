#!/usr/bin/env python3
import sys


# Serve per creare dizionari annidati
class AutoTree(dict):
    def __missing__(self, key):
        value = self[key] = type(self)()
        return value


stocks = AutoTree()

for line in sys.stdin:
    line_input = line.strip().split(",")
    valore = float(line_input[2].replace('\'', ''))
    valore_min = float(line_input[4].replace('\'', ''))
    valore_max = float(line_input[5].replace('\'', ''))

    # se non è nel dizionario allora lo metto
    if line_input[0] not in stocks:
        stocks[line_input[0]] = [line_input[7], valore,
                                 line_input[7], valore,
                                 valore_min, valore_max, 0]
    else:
        # in caso aggiorno date e valori
        if line_input[7] < stocks[line_input[0]][0]:
            stocks[line_input[0]][0] = line_input[7]
            stocks[line_input[0]][1] = valore
        if line_input[7] > stocks[line_input[0]][2]:
            stocks[line_input[0]][2] = line_input[7]
            stocks[line_input[0]][3] = valore
    
        # in caso aggiorno valori massimi e minimi
        if valore_min < stocks[line_input[0]][4]:
            stocks[line_input[0]][4] = valore_min
        if valore_max > stocks[line_input[0]][5]:
            stocks[line_input[0]][5] = valore_max

for stock in stocks:
    stocks[stock][6] = ((stocks[stock][3] * 100) / stocks[stock][1]) - 100

print(stocks)

