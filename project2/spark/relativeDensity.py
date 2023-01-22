# -*- coding: utf-8 -*-
"""
Spyder Editor

"""

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import col,max,min,count,sum,avg,stddev_pop,hour,countDistinct,expr,stddev,window,column
from pyspark.ml.feature import RFormula
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import Row, SparkSession, SQLContext
import math
from operator import add

#CONSTANTS
MAX_CELL = 250000
LEFT_CORNER_CELL = 249501

#FUNCTIONS
def getCellId(x,y):
    x= x-1
    y= y-1		#shift numbers from 1-20 to 0-19
    if x>=20:
        columnId= int(x/20)
    else:
        columnId= 0
    if y>=20:
        rowId = int(y/20)
    else:
        rowId = 0
	
    return rowId*500 + columnId+1

def getRelativeDensityIdx(cellId,cellCount,allCells):
    #(X) = X.count / Average (Y1.count, Y2.count, …Yn.count)
    n = neighbors(cellId)
    nSize = len(n)
    nCount = 0
    for i in range(nSize):
        nTemp = n.pop()
        tempCount = allCells.get(nTemp)
        if(not tempCount == None):
            nCount += tempCount
    if(nCount == 0):
        #all cells empty
        return 0
    else:
        avrg = nCount/nSize
        index = cellCount / avrg
        return index

def neighbors(cellID):
    neighborSet = set()
    
    if(not((cellID-1) % 500 == 0)):
        neighborSet.add(cellID-1) #add left cell
        
        if(not cellID-1-500 < 1):
            neighborSet.add(cellID-1-500) #add left upper diag
            
            
        if(not cellID-1+500 > MAX_CELL):
            neighborSet.add(cellID-1+500) #add left lower diag
            
    if(not(cellID % 500 == 0)):
        neighborSet.add(cellID+1) #add right cell
        
        if(not cellID+1-500 < 1):
            neighborSet.add(cellID+1-500) #add right upper diag
            
        if(not cellID+1+500 > MAX_CELL):
            neighborSet.add(cellID+1+500) #add right lower diag
    
    if(not(1 <= cellID <= 500)):
        neighborSet.add(cellID-500) #add top cell
    
    if(not(LEFT_CORNER_CELL <= cellID <= MAX_CELL)):
        neighborSet.add(cellID+500) #add bottom cell
    
    return neighborSet
    
##  test each case
#eight= neighbors(502)  
#three = neighbors(LEFT_CORNER_CELL)
#five = neighbors(1500)


def convert(list):
    dict = {list[i][0]: list[i][1] for i in range(0, len(list))}
    return dict



conf = SparkConf().setAppName('SparkRDD')
conf = (conf.setMaster('local[*]') 
        .set('spark.executor.memory', '16G')
        .set('spark.driver.memory', '16G'))

sc = SparkContext.getOrCreate(conf=conf)
Spark = SparkSession(sc)




#In this step, you will write spark code to manipulate the file and report the top 50 grid cells (the
#grid cell IDs not the points inside) that have the highest I index. Write the workflow that reports
#the cell IDs along with their relative-density index.
#Your code should be fully parallelizable (distributed) and scalable.

#map points to cells -> cell1, 1
#reduce -> cell1, 10

#rdd = sc.textFile("/home/twobeers/Desktop/bigData/project2/Ptest.csv")

rdd = sc.textFile("/home/twobeers/Desktop/bigData/big-data-projects/Pp.csv")
cells = rdd \
    .map(lambda line: line.split(",")) \
    .filter(lambda line: len(line)>1)  \
    .map(lambda line: (getCellId(int(line[0]),int(line[1])), 1)) \
    .reduceByKey(add) \
    .collect()
#print(cells)

cellsDict = convert(cells)  #now possible to get value by key
#distData = sc.parallelize(cellsDict)

list_of_tuples = []
for k,v in cellsDict.items():
    idx = getRelativeDensityIdx(k, v, cellsDict)
    list_of_tuples.append((k,idx))

#cellId, index

# sort list of tuples by second element (descending order)
sorted_list = sorted(
    list_of_tuples,
    key=lambda t: t[1],
    reverse=True
)

#print out first 50 entries
#print(sorted_list)
top_fifty = []
for i in range(50):
    print(sorted_list[i])
    top_fifty.append(sorted_list[i])
    
    
#Step 3 (Report the Neighbors of the TOP 50 grid) [20 Points]
#Continue over the results from Step 2, and for each of the reported top 50 grid cells, report the
#IDs and the relative-density indexes of its neighbor cells.

#neighbors of cellId: (cellId,idx), (cellId,idx)

    
#for 50 entries search neighbors
#for neighbors get index
s = ""
for i in range(50):
    cellId = sorted_list[i][0]
    nSet = neighbors(cellId)
    s += "neighbors of " + str(cellId) + ": "
    for nId in nSet:
        nCount = cellsDict.get(nId)
        idx = getRelativeDensityIdx(nId, nCount, cellsDict)
        s += "(" + str(nId) + "," + str(idx) + ")"
    s += "\n"

for i in s.splitlines():
    print(i)
