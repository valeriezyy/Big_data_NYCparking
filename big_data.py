from pyspark import SparkContext
import sys


def boro_code(i):
    boro=None
    if i in ['MAN','MH','MN','NEWY','NEW Y','NY']:
        boro=1
    if i in ['BRONX','BX']:
        boro=2
    if i in ['BK','K','KING','KINGS']:
        boro=3
    if i in ['Q','QN','QNS','QU','QUEEN']:
        boro=4
    if i in ['R','RICHMOND']:
        boro=5
    return boro
def match(boro,st,house_num):
    V_id=None
    import csv
    with open('nyc_cscl.csv','r')as file:
        reader=csv.reader(file)
        next(reader)
        for row in reader:
            
            if type(house_num) is tuple:
                if int(house_num[1])%2==0:#(0,3)

                    if ("-" in row[4]) and ("-" in row[5]):
#                             house_num=(house_num.split('-')[0],house_num.split('-')[1])
                        rangenum_l=(row[4].split("-")[0],row[4].split("-")[1])#(0,1) 0-1
                        rangenum_h=(row[5].split("-")[0],row[4].split("-")[1])#(5,7)
                        if (house_num<=rangenum_h) and (house_num>=rangenum_l):
                            if (st==row[10].lower())or (st==row[28].lower()):
                                if int(row[13])==boro:
                                    V_id=row[0]
                                    return V_id
                    else:
                        continue

                elif int(house_num[1])%2==1:
                    if ("-" in row[3]) and ("-" in row[2]):
#                             house_num=(house_num.split('-')[0],house_num.split('-')[1])
                        rangenum_l=(row[2].split("-")[0],row[2].split("-")[1])
                        rangenum_h=(row[3].split("-")[0],row[3].split("-")[1])
                        if (house_num<=rangenum_h) and (house_num>=rangenum_l):
                            if (st==row[10].lower())or (st==row[28].lower()):
                                if int(row[13])==boro:
                                    V_id=row[0]
                                    return V_id
                    else:
                        continue

            elif type(house_num) is int:
                if (row[4].isdigit()) and (row[5].isdigit()):
                    if house_num%2==0:
                        if (house_num<=int(row[5])) and (house_num>=int(row[4])):
                            if (st==row[10].lower())or (st==row[28].lower()):
                                if int(row[13])==boro:
                                    V_id=row[0]
                                    return V_id
                else:
                    continue
                    
                
                if (row[2].isdigit()) and (row[3].isdigit()):
                    if house_num%2==1:
                        if (house_num<=int(row[3])) and (house_num>=int(row[2])):
                            if (st==row[10].lower())or (st==row[28].lower()):
                                if int(row[13])==boro:
                                    V_id=row[0]
                                    return V_id
                else:
                    continue
                        
                                
            else:
                V_id=None
                            
    return None
                
                
                
def process(V_id, records): 
    import csv
    # Skip the header
    if V_id==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    for row in reader:
        if len(row)<31:
            continue
        if (row[23]=="")or (row[24]=="") or (row[21]==""):
            continue
        
        Street_Name = row[24].lower()
        Violation_County=row[21].upper()
        boro=boro_code(Violation_County)
        if row[23].isdigit():
#         if (type(row[23])is int) or (type(row[23])is float):
            housenum=int(row[23])
        elif ("-" in row[23]) and (row[23].split("-")[0].isdigit()) and (row[23].split("-")[1].isdigit()):
            one=row[23].split("-")[0]
#                 if row[23].split("-")[1].isdigit():
            two=row[23].split("-")[1]
            #tuple the house_num
            housenum=(one,two)
            
            
        else:
            continue
            
        if type(housenum) is tuple:
            if housenum[1].isdigit()== False:
                continue
        matched_id=match(boro,Street_Name,housenum)
        
        if matched_id:
            #4, 'Issue Date'
            year_id=(matched_id,row[4].split('/')[2])#keep yr
            counts[year_id] = counts.get(year_id, 0) + 1
#             yield (row[4].split('/')[2],housenum,boro,Street_Name),matched_id
    
           


            
    return counts.items()
            
           


            
def year(rdd):
    for row in rdd:
        if row[0] == "2015":
            yield (row[1][0],row[1][1],0,0,0,0)
        if row[0] == "2016":
            yield (row[1][0],0,row[1][1],0,0,0)
        if row[0] == "2017":
            yield (row[1][0],0,0,row[1][1],0,0)
        if row[0] == "2018":
            yield (row[1][0],0,0,0,row[1][1],0)
        if row[0] == "2019":
            yield (row[1][0],0,0,0,0,row[1][1])

def ols1(rdd):
    import numpy as np
    from sklearn.linear_model import LinearRegression
    for row in rdd:
        x=np.arange(2015,2020)
        y=list(row[1])
        x1=x.reshape(-1,1)
        reg = LinearRegression().fit(x1,y)
        coef=reg.coef_[0]
        yield((row[0]),(row[1],coef))
        
        
        
if __name__ == "__main__":
    sc = SparkContext()
    rdd = sc.textFile("hdfs:///tmp/bdm/nyc_parking_violation/")
    counts2 = rdd.mapPartitionsWithIndex(process)\
            .reduceByKey(lambda x,y: x+y).sortByKey(False)
    before_ols=counts2.map(lambda x:((x[0][0]),(x[0][1],x[1])))\
    .reduceByKey(lambda x,y: x+y).map(lambda x: ((x[1][0]),(x[0],x[1][1])))\
    .mapPartitions(year).map(lambda x: ((x[0]),(x[1],x[2],x[3],x[4],x[5])))
    
    before_ols.mapPartitions(ols1).saveAsTextFile(sys.argv[1])
    