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

def distance(point,cluster,k,n):
    dis = []
    distance = 0
    for i in range(0,k):
        for s in range(0,n):
            distance = distance + abs(point[s]-cluster[i][s])
        dis.append(distance)
        distance = 0
    #print(dis)
    #distance1 = abs(point[0] - cluster1[0]) + abs(point[1] - cluster1[1])
    #distance2 = abs(point[0] - cluster2[0]) + abs(point[1] - cluster2[1])
    #distance3 = abs(point[0] - cluster3[0]) + abs(point[1] - cluster3[1])
    minimum = min(dis)
    
    for x in range(0,k):
        if minimum == dis[x]:
            return x



def recursive(fileName,cluster,k,n):
	
    cluster1 = cluster[0]
    lines = sc.textFile (fileName)
    words = lines.flatMap (lambda line: line.split('\n'))
    pairs = words.map(lambda word:(distance(mapperagain(n,word),cluster,k,n),mapper(n,word)))
    #pairs.foreach(display)
    counts = pairs.reduceByKey(lambda a,b: (papper(n,a,b)))
    results = counts.collect()
    cluster = []
    cluster2 = []
    
    for i in range(0,k):
        for s in range(1,n+1):
                cluster2.append(float(results[i][1][s])/float(results[i][1][0]))
        cluster.append(cluster2)
        cluster2=[]

    #cluster2 = [float(results[1][1][1])/float(results[1][1][0]),float(results[1][1][2])/float(results[1][1][0])]
    #cluster3 = [float(results[2][1][1])/float(results[2][1][0]),float(results[2][1][2])/float(results[2][1][0])]
        

    if cluster1 == cluster[0]:
        pairs.foreach(display)
        print(cluster)
        print("diameter")
        print pairs[1]
        print("distance")

    else: 
        print (cluster)
        recursive(fileName,cluster,k,n)
        
def mapperagain(n,word):
    maplist = []
    for i in range(1,n+1):
            maplist.append(float(word.split(",")[i]))
    return maplist

def mappera(n,word):
    maplist = [1]
    for i in range(1,n+1):
            maplist.append(float(word.split(",")[i]))
    return maplist

def mapper(n,word):
    maplist = [1]
    for i in range(1,n+1):
            maplist.append(float(word.split(",")[i]))
    return maplist
    
def pappera(n,a,b):
    countslist = []
    for  i in range(0,n+1):
            countslist.append(a[i]+b[i])
    return countslist
def papper(n,a,b):
    countslist = []
    for  i in range(0,n+1):
            countslist.append(a[i]+b[i])
    return countslist

def display(word):
    print(word) 

def processFile(fileName,k,n):
    lines = sc.textFile (fileName)
    mylist = []
    for i in range (0,k):
        mylist.append(i)
    
    
    
    words = lines.flatMap (lambda line: line.split('\n'))
    pairs  = words.map(lambda word: (random.choice(mylist),mappera(n,word)))
    pairs.foreach(display)
    counts = pairs.reduceByKey(lambda a,b: (pappera(n,a,b)))

    results = counts.collect()
    printList(results)
    cluster = []
    cluster1 = []
    
    for a in range (0,k):
        for i in range(1,n+1):
          cluster1.append(float(results[a][1][i])/float(results[a][1][0]))
        cluster.append(cluster1)
        cluster1=[]
    #cluster2 = [float(results[1][1][1])/float(results[1][1][0]),float(results[1][1][2])/float(results[1][1][0])]
    #cluster3 = [float(results[2][1][1])/float(results[2][1][0]),float(results[2][1][2])/float(results[2][1][0])]
    #print(cluster)
    #print(cluster2)
    #print(cluster3)

    recursive(fileName,cluster,k,n)


        



def main():
    quiet_logs(sc)

    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help="filename of input text")
    parser.add_argument('k',type = int)
    parser.add_argument('n',type = int)
    args = parser.parse_args()
    print(args.k)
    print(args.n)
    processFile (args.filename,args.k,args.n)

if __name__ == "__main__":
    main()

