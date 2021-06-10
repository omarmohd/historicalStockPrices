#!/usr/bin/env python3
import sys

# LOCATION OF THE FIELDS
SECTOR = 0
TICKER = 1
DATE = 2
CLOSE = 3
VOLUME = 4

# GLOBAL VARIABLES
prev_Sector = None
prev_Ticker = None
prev_Year = None
prev_Close = 0

'''
key: year, value: dictionary {
    closeStartingValue: closeStartingValue,
    closeFinalValue: closeFinalValue
}
'''
yearToChange = {}  #result


'''
key: year, value: dictionary {
    ticker: tickerVolumeValue
}
'''
yearToTickerVolume = {} #result


'''
key: year, value: dictionary {
    key: ticker, value: dictionary {
        closeStartingValue: closeStartingValue,
        closeFinalValue: closeFinalValue
    }
}
'''
yearToTickerChange = {} #result

def appendToList():
    for year in sorted(yearToChange.keys()):
        # percentage change in the price of the sector over the year
        sectorTrend = yearToChange[year]
        totPercentChange = round((sectorTrend['closeFinalValue'] - sectorTrend['closeStartingValue']) /
                              sectorTrend['closeStartingValue'] * 100, 2)

        # the action of the sector that had the greater percentage increase in the year
        tickerChangeMap = yearToTickerChange[year]
        maxTickerForChange, maxChange = getMaxTickerChange(tickerChangeMap)

        # the action of the sector that had the higher volume of transactions in the year
        tickerTrend = yearToTickerVolume[year]
        maxTickerForVolume, maxVolume = getMaxTickerVolume(tickerTrend)

        print('{},\t{},\t{} %,\t{}\t{} %,\t{}\t{}'.format(prev_Sector, year, totPercentChange, maxTickerForChange, maxChange, maxTickerForVolume, maxVolume))


def getMaxTickerChange(tickerChangeMap):
    maxChange = 0  # l'incremento percentuale è sempre positivo
    for ticker in sorted(tickerChangeMap.keys()):
        maxTickerChange = ticker
        tickerChangeTrend = tickerChangeMap[ticker]
        currTickerChange = round((tickerChangeTrend['closeFinalValue'] - tickerChangeTrend['closeStartingValue']) /
                                    tickerChangeTrend['closeStartingValue'] * 100, 2)
        if (currTickerChange > maxChange):
            maxTickerChange = ticker
            maxChange = currTickerChange

    return maxTickerChange, maxChange


def getMaxTickerVolume(tickerTrend):
    maxVolume = 0
    for tick in tickerTrend:
        vol = tickerTrend[tick]
        if(vol > maxVolume):
            maxTickerVolume = tick
            maxVolume = vol

    return maxTickerVolume, maxVolume


def updateDataStructure(dataStructure, year, key, value):
    if year in dataStructure:
        if key in dataStructure[year]:
            dataStructure[year][key] += value
        else:
            dataStructure[year][key] = value
    else:
        dataStructure[year] = {}
        dataStructure[year][key] = value


def updateYearToTickerChange(dataStructure, year, ticker, key, value):
    if year in dataStructure:
        if ticker in dataStructure[year]:
            if key in dataStructure[year][ticker]:
                dataStructure[year][ticker][key] += value
            else:
                dataStructure[year][ticker][key] = value
        else:
            dataStructure[year][ticker] = {}
            dataStructure[year][ticker][key] = value
    else:
        dataStructure[year] = {}
        dataStructure[year][ticker] = {}
        dataStructure[year][ticker][key] = value


def parseValues(valueList):
    sector = valueList[SECTOR].strip()
    ticker = valueList[TICKER].strip()
    date = valueList[DATE].strip()
    close = float(valueList[CLOSE].strip())
    volume = int(valueList[VOLUME].strip())
    return (sector, ticker, date, close, volume)


# main
for line in (sys.stdin):
    valueList = line.strip().split('\t')

    if len(valueList) == 5:
        sector, ticker, date, close, volume = parseValues(valueList)
        year = date[0:4]

        if prev_Sector and prev_Sector != sector: # different sector
            updateDataStructure(yearToChange, prev_Year, 'closeFinalValue', prev_Close)
            updateYearToTickerChange(yearToTickerChange, prev_Year, prev_Ticker, 'closeFinalValue', prev_Close)
            appendToList()

            yearToChange = {}
            yearToTickerVolume = {}
            yearToTickerChange = {}

            updateDataStructure(yearToChange, year, 'closeStartingValue', close)
            updateYearToTickerChange(yearToTickerChange, year, ticker, 'closeStartingValue', close)

            updateDataStructure(yearToTickerVolume, year, ticker, volume)

        else: # same sector
            updateDataStructure(yearToTickerVolume, year, ticker, volume)

            if prev_Ticker and prev_Ticker != ticker: # Case 1: different ticker
                updateDataStructure(yearToChange, prev_Year, 'closeFinalValue', prev_Close)
                updateYearToTickerChange(yearToTickerChange, prev_Year, prev_Ticker, 'closeFinalValue', prev_Close)
                updateDataStructure(yearToChange, year, 'closeStartingValue', close)
                updateYearToTickerChange(yearToTickerChange, year, ticker, 'closeStartingValue', close)

            else: # Case 2: same ticker
                if not prev_Ticker:
                    prev_Ticker = ticker
                    updateDataStructure(yearToChange, year, 'closeStartingValue', close)
                    updateYearToTickerChange(yearToTickerChange, year, ticker, 'closeStartingValue', close)

                if prev_Year and prev_Year != year:
                    updateDataStructure(yearToChange, prev_Year, 'closeFinalValue', prev_Close)
                    updateYearToTickerChange(yearToTickerChange, prev_Year, prev_Ticker, 'closeFinalValue', prev_Close)
                    updateDataStructure(yearToChange, year, 'closeStartingValue', close)
                    updateYearToTickerChange(yearToTickerChange, year, ticker, 'closeStartingValue', close)

        prev_Year = year
        prev_Close = close
        prev_Sector = sector
        prev_Ticker = ticker

if prev_Sector:
    updateDataStructure(yearToChange, prev_Year, 'closeFinalValue', prev_Close)
    updateYearToTickerChange(yearToTickerChange, prev_Year, prev_Ticker, 'closeFinalValue', prev_Close)
    appendToList()