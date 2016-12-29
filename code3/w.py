import re
import argparse
import collections
import random
import string
import math





from pyspark import SparkContext, SparkConf
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

def distance(point,cluster,k):
    dis = []
    for i in range(0,k):
        dis.append([abs(point[0] - cluster[i][0]) + abs(point[1] - cluster[i][1])])
    #print(dis)
    #distance1 = abs(point[0] - cluster1[0]) + abs(point[1] - cluster1[1])
    #distance2 = abs(point[0] - cluster2[0]) + abs(point[1] - cluster2[1])
    #distance3 = abs(point[0] - cluster3[0]) + abs(point[1] - cluster3[1])
    minimum = min(dis)
    
    for x in range(0,k):
        if minimum == dis[x]:
            
            return x
       


def recursive(fileName,cluster,k):
    cluster4 = cluster[0][0]
    cluster5 = cluster[1][0]
    cluster6 = cluster[2][0]

    lines = sc.textFile (fileName)
    words = lines.flatMap (lambda line: line.split('\n'))
    pairs = words.map(lambda word:(distance([float(word.split(" ")[0]),float(word.split(" ")[1])],cluster,k),[1,float(word.split(" ")[0]),float(word.split(" ")[1])]))
    
    counts = pairs.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1],a[2]+b[2],a[3]+b[3],a[4]+b[4],a[5]+b[5],a[6]+b[6],a[7]+b[7],a[8]+b[8],a[9]+b[9],a[10]+b[10],a[11]+b[11],a[12]+b[12],a[13]+b[13]))
    results = counts.collect()
    cluster = []
    for i in range(0,k):
        cluster.append([float(results[i][1][1])/float(results[i][1][0]),float(results[i][1][2])/float(results[i][1][0])])
    #cluster2 = [float(results[1][1][1])/float(results[1][1][0]),float(results[1][1][2])/float(results[1][1][0])]
    #cluster3 = [float(results[2][1][1])/float(results[2][1][0]),float(results[2][1][2])/float(results[2][1][0])]
    
    if cluster4 == cluster[0][0]:
        pairs.foreach(display)
        print(cluster)
        newcounts = pairs.reduceByKey(lambda a,b: a + b)
        newresults = newcounts.collect()
        printList(newresults)
            
    else: 
        print (cluster)
        recursive(fileName,cluster,k)
        
   
   


def display(word):
    print(word) 

def processFile(fileName,k):
    lines = sc.textFile (fileName)
    mylist = []
    for i in range (0,k):
        mylist.append(i)
    
    
    
    words = lines.flatMap (lambda line: line.split('\n'))

    pairs  = words.map(lambda word: (random.choice(mylist),[1,float(word.split(",")[1]),float(word.split(",")[2]),float(word.split(",")[3]),float(word.split(",")[4]),float(word.split(",")[5]),float(word.split(",")[6]),float(word.split(",")[7]),float(word.split(",")[8]),float(word.split(",")[9]),float(word.split(",")[10]),float(word.split(",")[11]),float(word.split(",")[12]),float(word.split(",")[13])]))
    pairs.foreach(display)
    counts = pairs.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1],a[2]+b[2],a[3]+b[3],a[4]+b[4],a[5]+b[5],a[6]+b[6],a[7]+b[7],a[8]+b[8],a[9]+b[9],a[10]+b[10],a[11]+b[11],a[12]+b[12]))
    results = counts.collect()
    cluster = []
    printList (results)
    #for a in range (0,k):
    #      cluster.append([float(results[a][1][1])/float(results[a][1][0]),float(results[a][1][2])/float(results[a][1][0])])
    #cluster2 = [float(results[1][1][1])/float(results[1][1][0]),float(results[1][1][2])/float(results[1][1][0])]
    #cluster3 = [float(results[2][1][1])/float(results[2][1][0]),float(results[2][1][2])/float(results[2][1][0])]
    #print(cluster)
    #print(cluster2)
    #print(cluster3)
    
    #recursive(fileName,cluster,k)


        



def main():
    quiet_logs(sc)

    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help="filename of input text")
    parser.add_argument('k',type = int)
    args = parser.parse_args()

    processFile (args.filename,args.k)

if __name__ == "__main__":
    main()

