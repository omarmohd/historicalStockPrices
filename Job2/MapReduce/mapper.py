#!/usr/bin/env python3
import csv
import sys

RANGE1 = 2009
RANGE2 = 2018

rangeValues = range(RANGE1, RANGE2 + 1)

tickerToSectorMap = {}

with open('historical_stocks.csv') as hs:
    csv_reader = csv.reader(hs, delimiter=',')
    firstLine = True

    for row in csv_reader:
        if not firstLine:
            ticker, _, _, sector, _ = row
            if sector != 'N/A':
                tickerToSectorMap[ticker] = sector
        else:
            firstLine = False


for line in sys.stdin:
    # CONVERT EVERY LINE INTO A LIST OF STRINGS
    input = line.strip().split(',')
    if len(input) == 8:
        ticker, _, close, _, _, _, volume, date = input
        if ticker != 'ticker':
            year = int(date[0:4])

            # CHECK YEAR BETWEEN START & END
            if year in rangeValues and ticker in tickerToSectorMap:
                sector = tickerToSectorMap[ticker]
                print('{}\t{}\t{}\t{}\t{}'.format(sector, ticker, date, close, volume))