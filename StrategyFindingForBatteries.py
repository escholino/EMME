import math
import numpy as np
import pandas as pd
import time
from itertools import product

time_start = time.time()

#Data input in a form of 3 rows with the prices for every market
market = pd.read_csv('beispiel2.csv')

x1 = market['15 min']
x2 = market['30 min'][np.logical_not(np.isnan(market['30 min']))]
x3 = market['60 min'][np.logical_not(np.isnan(market['60 min']))]

# t0 marks the current time, if it starts at 15,30,45 after a full hour
t0 = 3
#capacity in MWh
capacity = 1
#poweroutput in MW
poweroutput = 1
#starting load of the battery
load = 0


# structures the market in a way that vor every time the price and the traiding avialabilty can be seen
def marketstructure(t0, x1, x2, x3):
    structuredMarket = []
    for t1 in range(len(x1) - 4):  # hier wir die länge definiert, für die vorhergesagten Daten
        p = [x1[t1]]
        if int((t1 + t0) % 2) == 0 and int((t1 + t0) % 4) == 0:
            p.append(float(x2[int((t1 + t0) / 2)]))
            p.append(float(x3[int((t1 + t0) / 4)]))
        elif int((t1 + t0) % 2) == 0 and int((t1 + t0) % 4) != 0:
            p.append(float(x2[int((t1 + t0) / 2)]))
            p.append(np.nan)
        else:
            p.append(np.nan)
            p.append(np.nan)
        structuredMarket.append(p)
        t1 += 1
    return structuredMarket

#creation of the structured market data
structuredmarket = marketstructure(t0, x1, x2, x3)

# this funktion shuffels the in a way that evrey combination of regions in spezific time horizion is listed in huge list
def shuffelMarket(lst):
    for doslice in product([True, False], repeat=len(lst) - 1):
        slices = []
        start = 0
        for i, slicehere in enumerate(doslice, 1):
            if slicehere:
                slices.append(lst[start:i])
                start = i
        slices.append(lst[start:])
        yield slices


def cutSellAndBuy(marketpart):
    return marketpart[0:math.floor(len(marketpart) / 2) -1], marketpart[math.floor(len(marketpart) / 2):len(marketpart)]


# funktion die eine liste erstellt bei der jeder preis der entsprechenden handlsziet zugeordnet wird
# erster wert is der index in der liste um ihn später zu löschen, zweiter dei kapazität und 3. der preis
def priceToKapa(marketpart, capacity):
    priceToKapalist = []
    capacityList = [capacity * 0.25, capacity * 0.5, capacity * 1.0]
    for i in range(len(marketpart)):
        for j in range(len(marketpart[i])):
            pricekapaPair = [capacityList[j], marketpart[i][j]]
            priceToKapalist.append(pricekapaPair)
            j += 1
        i += 1
    return priceToKapalist


# glieche liste wie oben nur mit index für später um die richtigen werte zu löschen bei sell und buy
def priceToIndex(marketpart):
    priceToIndexlist = []
    for i in range(len(marketpart)):
        for j in range(len(marketpart[i])):
            priceIndexPair = marketpart[i][j]
            priceToIndexlist.append(priceIndexPair)
            j += 1
        i += 1
    return priceToIndexlist


# hier wird der batrag errechnent der maximal bei der gegebenen region gekauft werden kann und wo
def loadBuy(marketpart, capacity, poweroutput, startload):
    buyregion = cutSellAndBuy(marketpart)[0]
    load = startload
    # limitiert die einfuhr auf power input auf die zeit die es in der region hat
    totalPower = ((len(priceToKapa(buyregion, capacity)) / 3) * 0.25 * poweroutput)
    buyRegionInKapaPrice = priceToKapa(buyregion, capacity)
    buyRegionIndexPrice = priceToIndex(buyregion)
    listOfBuyingPoints = []
    totalreturn = 0
    for i in range(len(buyRegionInKapaPrice)):
        idx = buyRegionIndexPrice.index(min(buyRegionIndexPrice))
        kapa = buyRegionInKapaPrice[idx][0]
        if capacity == 0 or kapa + load > poweroutput:
            del buyRegionInKapaPrice[idx]
            del buyRegionIndexPrice[idx]
            i += 1
        elif (load + kapa * poweroutput <= capacity) & (kapa*(poweroutput/capacity) <= totalPower):
            listOfBuyingPoints.append(['Price:',buyRegionIndexPrice[idx] * poweroutput,
                                       'MW:',buyRegionInKapaPrice[idx][0] * poweroutput])
            totalreturn += buyRegionIndexPrice[idx]  * kapa
            del buyRegionInKapaPrice[idx]
            del buyRegionIndexPrice[idx]
            load += kapa * poweroutput
            i += 1
        else:
            del buyRegionInKapaPrice[idx]
            del buyRegionIndexPrice[idx]
            i += 1
    return listOfBuyingPoints, totalreturn, load


def loadSell(marketpart, capacity, poweroutput, startload):
    sellregion = cutSellAndBuy(marketpart)[1]
    totalkapa = capacity
    load = 0
    capacity = loadBuy(sellregion, capacity, poweroutput, startload)[2]
    # limitiert die einfuhr auf power input auf die zeit die es in der region hat
    totalPower = ((len(priceToKapa(sellregion, capacity)) / 3) * 0.25 * poweroutput )
    sellRegionInKapaPrice = priceToKapa(sellregion, capacity)
    sellRegionIndexPrice = priceToIndex(sellregion)
    listOfSellingPoints = []
    totalreturn = 0
    for i in range(len(sellRegionInKapaPrice)):
        idx = sellRegionIndexPrice.index(max(sellRegionIndexPrice))
        kapa = sellRegionInKapaPrice[idx][0]
        if capacity == 0 or kapa + load > poweroutput:
            del sellRegionInKapaPrice[idx]
            del sellRegionIndexPrice[idx]
            i += 1
        elif (load + kapa * poweroutput <= capacity) & (kapa*(poweroutput/totalkapa) <= totalPower):
            listOfSellingPoints.append(['Price:',sellRegionIndexPrice[idx] * poweroutput,
                                        'MW:',sellRegionInKapaPrice[idx][0] * poweroutput])
            totalreturn += sellRegionIndexPrice[idx] * kapa
            del sellRegionIndexPrice[idx]
            del sellRegionInKapaPrice[idx]
            load += kapa * poweroutput
            i += 1
        else:
            del sellRegionInKapaPrice[idx]
            del sellRegionIndexPrice[idx]
            i += 1
    return listOfSellingPoints, totalreturn

#eine region
def partialReturn(marketpart):
    if (loadSell(marketpart, capacity, poweroutput, load)[1] - loadBuy(marketpart, capacity, poweroutput,load)[1]) <= 0:
        result = 0
    elif loadSell(marketpart, capacity, poweroutput,load)[1] == np.nan or \
            loadBuy(marketpart, capacity, poweroutput,load)[1] == np.nan:
        result = 0
    else:
        result = loadSell(marketpart, capacity, poweroutput, load)[1] - loadBuy(marketpart, capacity, poweroutput, load)[1]
    return result


# die funktion erechent den gesmaten return  einer comnbination aus
def wholeReturn(wholemarketpartcombination):
    wholeReturnvalue = 0
    for i in range(len(wholemarketpartcombination)):
        if partialReturn(wholemarketpartcombination[i]) > 0:
            wholeReturnvalue += partialReturn(wholemarketpartcombination[i])
            i += 1
        else:
            i += 1
    return wholeReturnvalue


def maximalStrategy(wholeMarket):
    listOfMaxValue = []
    whole = list(wholeMarket)
    for i in range(len(whole)):
        region = whole[i]
        listOfMaxValue.append(float(wholeReturn(region)))
        i += 1
    maxCash = max(listOfMaxValue)
    maxCombination = listOfMaxValue.index(max(listOfMaxValue))
    lengthOfFirstRegion = len(whole[maxCombination][0])
    return maxCash, maxCombination, lengthOfFirstRegion, whole[maxCombination]

#print(len(maximalStrategy(shuffelMarket(structuredmarket[0:12]))[3][1]))
#4h beriche (16) werden berechnet
def iterationOverTime(alldata):
    profit = 0
    i = 0
    #how far into the future the calculation shoulf go (more than 4 hours takes very long due to exponential calc time)
    calculationhorizion = 4
    while i < len(alldata):
        strategy = maximalStrategy(shuffelMarket(alldata[i:i+calculationhorizion*4]))[3]
        if strategy:
            counter = 0
            if len(strategy[0]) == 1:
                counter += 1
                del strategy[0]
            if strategy:
                profitOfFirst = partialReturn(strategy[0])
                if np.isnan(profitOfFirst) or profit == profitOfFirst + profitOfFirst:
                    profit += 0
                    print('Current Profit: ', profit, '€', '|| No new transactions')
                else:
                    profit += profitOfFirst
                    print('Current Profit: ', profit, '€', '||', 'Buy at:',
                          loadBuy(strategy[0], capacity, poweroutput, load)[0], 'Sell at:',
                          loadSell(strategy[0], capacity, poweroutput, load)[0])
                i += counter + len(strategy[0])
            else:
                break
    return 'Total daily profit: ', profit, '€'


print(iterationOverTime(structuredmarket))



time_elapsed = (time.time() - time_start)
print('________________________________________________')
print('Time needed to calculate: ', time_elapsed, '.sec')