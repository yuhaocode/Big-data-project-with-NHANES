import re
import argparse
import random
import math
import collections
from decimal import Decimal
from pyspark import SparkContext, SparkConf
#import matplotlib
#matplotlib.use('Agg')
#import matplotlib.pyplot as plt

conf = SparkConf()
sc = SparkContext(conf=conf)

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

def flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
            for sub in flatten(el):
                yield sub
        else:
            yield el

def printList (results):
    for r in results:
        print ",".join (str(x) for x in flatten(r))

def parseWord(char,index):
    begin,middle,end = (0,0,0)
    atBegin = (index == 0)
    atEnd = (index == len(word) - 1)
    if not (atBegin or atEnd):
        middle = 1
    else:
        if atBegin:
            begin = 1
        if atEnd:
            end = 1
    return (char, (begin,middle,end))

def safe_cast(val, to_type, default):
    if len(val) == 0 or len(val) > 20:
        return default
    else:
        return to_type(val)


    #for a in range (0,k):
    #      cluster.append([float(results[a][1][1])/float(results[a][1][0]),float(results[a][1][2])/float(results[a][1][0])])

def processFile(filename):
 
    lines = sc.textFile ("/data/DEMO_H.csv")
    linesheader = lines.take(1)[0]
    lines = lines.filter(lambda line: line != linesheader)
    demo = lines.map(lambda x: x.split(",")).map(lambda x: (float(x[0]),[float(x[3]),safe_cast(x[4],float,float(0)),float(x[6])])) 
    counts = demo.reduceByKey(lambda a, b: [a]+[b])



    
    nutrientlines = sc.textFile ("/data/DR1IFF_H.csv")
    header = nutrientlines.take(1)[0]
    nutrientlines = nutrientlines.filter(lambda line: line != header)
    nutrientwords = nutrientlines.map(lambda cell: cell.split(",")).map(lambda cell:(float(cell[0]),
        [safe_cast(cell[18],float,float(0)),safe_cast(cell[19],float,float(0)),safe_cast(cell[20],float,float(0)),safe_cast(cell[21],float,float(0)),safe_cast(cell[22],float,float(0)),safe_cast(cell[23],float,float(0)),safe_cast(cell[24],float,float(0)),safe_cast(cell[25],float,float(0)),safe_cast(cell[26],float,float(0)),safe_cast(cell[27],float,float(0))]))
    #filteredWords = words.flatMap (lambda word: w for w in )
    #pairs  = words.map(lambda word: (word,))
    nutrientcounts = nutrientwords.reduceByKey(lambda a, b: [a[0]+b[0],a[1]+b[1],a[2]+b[2],a[3]+b[3],a[4]+b[4],a[5]+b[5],a[6]+b[6],a[7]+b[7],a[8]+b[8],a[9]+b[9]])
    

    datanutrient = counts+nutrientcounts
    dataresults = datanutrient.reduceByKey(lambda a,b: [a]+[b])
    dataresults = dataresults.collect()
    print "# {}".format(filename)
    print "########################################"
    print "# nutrient"
    finalresult = []
    for i in range(0,len(dataresults)):
        if len(dataresults[i][1]) == 2:
            finalresult.append(dataresults[i])

    printList(finalresult)
    #for i in range(0,len(finalresult)):
    #   print finalresult[i]
  #          plt.plot(finalresult[i][1][0],finalresult[i][1][1],'ro',color = 'r')
  #      else:
  #          plt.plot(finalresult[i][1][0],finalresult[i][1][1],'ro',color = 'b')
  #  plt.savefig("1.png",format = "png")
#def cluster(finalresult):
    #distData = sc.parallelize(finalresult)
    #print distData
    #mylist = []
    #datalist = []
    #for i in range(0,3):
    #    mylist.append(i)
    #for i in range(0,len(finalresult)):
    #   datalist.append([random.choice(mylist),[1,finalresult[i][1][0][0],finalresult[i][1][0][1],float(finalresult[i][1][0][2])
    #    ,float(finalresult[i][1][1][0]),float(finalresult[i][1][1][1]),float(finalresult[i][1][1][2]),float(finalresult[i][1][1][3]),float(finalresult[i][1][1][4]),float(finalresult[i][1][1][5]),float(finalresult[i][1][1][6]),float(finalresult[i][1][1][7]),float(finalresult[i][1][1][8]),float(finalresult[i][1][1][9])]])
    


 

    
def main():
    quiet_logs(sc)

    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help="filename of input text", nargs="+")
    args = parser.parse_args()

    for filename in args.filename:
        processFile (filename)

if __name__ == "__main__":
    main()

