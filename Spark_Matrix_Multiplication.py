from pyspark import SparkContext
import sys
def mult(key,values):
    print key
    print values
    map_reducer={"A":None,"B":None}
    info={"rowColumn":None,"matVal":None}
    map_reducer["A"]=[]
    map_reducer["B"]=[]
    reducerOut={}
    for val in values:
       # temp=val.split(',')
        print "processing"
        info["rowColumn"]=val[0]
        info["matVal"]=val[1]
        if(val[2]=='A'):
            map_reducer["A"].append(info.copy())
        else:
            map_reducer["B"].append(info.copy())
    matA=map_reducer.get("A")
    matB=map_reducer.get("B")
    result=[]
    for valA in matA:
        row=valA["rowColumn"]
        mValA=valA["matVal"]
        for valB in matB:
            col=valB["rowColumn"]
            mValB=valB["matVal"]
            rowcol=(row,col)
            print rowcol
            c_val=mValA*mValB
            print c_val
            tempResult=((row,col),c_val)
            result.append(tempResult)
    print result
    return result
s=sys.argv
out_file=open(s[3],"w")
sc = SparkContext(appName="inf553")
linesA=sc.textFile(s[1],2)
linesB=sc.textFile(s[2],2)
phase1MapA=linesA.map(lambda x:(int(x.split(',')[1]),(int(x.split(',')[0]),int(x.split(',')[2]),'A'))).collect()
print phase1MapA
phase1MapB=linesB.map(lambda x:(int(x.split(',')[0]),(int(x.split(',')[1]),int(x.split(',')[2]),'B'))).collect()
print phase1MapB
phase1Map=phase1MapA+phase1MapB
print phase1Map
phase1MapOut=sc.parallelize(phase1Map)
phase1ReducerInput=phase1MapOut.groupByKey().map(lambda x : (x[0],list(x[1]))).collect()
print phase1ReducerInput
phase2MapperInput=[]
### output here which i am trying to pass to reduce [(1, [(1, 1, 'A'), (2, 1, 'A'), (1, 1, 'B')]), (2, [(1, 2, 'A'), (2, 1, 'B')]), (3, [(2, 1, 'A'), (1, 2, 'B')])]
for kv in phase1ReducerInput:
    phase1ReducerInp=sc.parallelize(kv,1)
    phase1ReducerOutput=phase1ReducerInp.reduce(lambda x,y:mult(x,y))
    phase2MapperInput=phase2MapperInput+phase1ReducerOutput
    print "inside main "
    print phase2MapperInput
phase2MapperInp=sc.parallelize(phase2MapperInput)
phase2ReducerOutput=phase2MapperInp.reduceByKey(lambda num,n : num+n).collect()
print phase2ReducerOutput
for res in phase2ReducerOutput:
    rowcol= res[0]
    out_file.write(str(rowcol[0])+","+str(rowcol[1])+"\t "+str(res[1])+"\n")
        

