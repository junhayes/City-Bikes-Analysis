#This is the ETL script we built to move our bikes and weather data from our AWS hosted MongoDb Atlas
#cloud database to our AWS hosted Postgresql database. This version is built to be run on our AWS
#Amazo Linux instance.

#In this script we had to consider both the initiation of the transfer, as well as resuming the
#transfer due to unexpected interruptions. This script is designed to initially pick 1000 Mongo
#documents from each of our 4 collections, and transform them before transferring them - we then
#take the document ID (index) of the last of the previous 1000 documents loaded and transfer the
#next set of documents after this ID in a constant loop. To enable us to resume loads, we can input
#document IDs where the script should resume transfers from.

import psycopg2
from pymongo import MongoClient
from bson import ObjectId
import pandas
import time
from pandas.io.json import json_normalize
import csv

#This function takes the last documentID from the previously run batch of transfers
#and selects the next set of documents to be transferred.

def Find_from_ID(doc_id, collection_name):
    cur=collection_name.find({"_id": {"$gt" : ObjectId(doc_id)}}).limit(1000)   #find documents after the last document in the previous collection
    return cur

#This function creates a pandas dataframe out of the pointer to the Mongo bikes documents that have been 
#passed to it. It performs a few transformations to the data, removing unwanted columns and junk data

def Continuous_Get_Bikes(cur,string):
    mongo_docs = list(cur)
    mongo_docs = mongo_docs[:2000]
    docs = pandas.DataFrame(columns=[])

    for num, doc in enumerate(mongo_docs):
        doc["_id"] = str(doc["_id"])
        doc_id = doc["_id"]
        doc["number"] = str(string+str(doc["number"]))
        try:doc['available_bike_stands']=doc['available_bike_stands'].fillna(0).astype('int64')
        except:continue
        try:doc['available_bikes']=doc['available_bikes'].fillna(0).astype('int64')
        except:continue
        try:doc['last_update']=doc['last_update'].fillna(0).astype('int64')
        except:continue
        series_obj = pandas.Series(doc, name=doc_id )
        docs = docs.append(series_obj )
        docs = docs.drop(["position","address","banking","bonus","name","contract_name","bike_stands"],axis=1) 

    return docs, doc_id

#This function creates a pandas dataframe out of the pointer to the Mongo weather documents that have been 
#passed to it. This is a complex dataset to handle due to the varying degrees of nesting that exists in the
#json output. A couple of transformations take place, with a couple of fields intermittently appearing in
#our MongoDb dataset that could cause issues if passed to the relational tables of postgres

def Continuous_Get_Weather(cur):
    mongo_docs = list(cur)
    mongo_docs = mongo_docs[:2000]
    
    df = pandas.DataFrame(columns=[])   #holds all weather info
    df_temp = pandas.DataFrame(columns=[])  #holds temperature weather info
    df_wind = pandas.DataFrame(columns=[])  #holds wind weather info

    for num, doc in enumerate(mongo_docs):
        doc["_id"] = str(doc["_id"])
        doc_id = doc["_id"]
        series_obj1 = pandas.Series(doc, name=doc_id )
        df = df.append(series_obj1)
        series_obj_temp = pandas.Series(doc['main'], name=doc_id)
        df_temp = df_temp.append(series_obj_temp)
        df_temp.index = range(1,len(df_temp)+1)
        series_obj_wind = pandas.Series(doc['wind'], name=doc_id)
        df_wind = df_wind.append(series_obj_wind)
        df_wind.index = range(1,len(df_wind)+1)
        
    df_cate = json_normalize(df.to_dict('list'), ['weather']).unstack().apply(pandas.Series)
    df_cate.index=(range(1,len(df_cate)+1))
    df_time = json_normalize(df.to_dict('list'), ['dt']).unstack().apply(pandas.Series)
    df_time.index=(range(1,len(df_time)+1))

    result=pandas.concat([df_cate, df_temp, df_wind, df_time], axis=1, sort=False)
    result=result.filter(items=["main","humidity","temp","speed",0])
    result[0]=result[0].fillna(0).astype('int64')
    result=result.rename(columns={"main":"weather",0:"time"})

    length=result['temp'].isnull().sum()
    results=result.head(length)
        
    return results, doc_id

#This function takes in the formatted bikes or weather dataframe and saves it to a CSV file at a
#defined file path
    
def Save_To_CSV(docs,path):
    docs.to_csv(path, ",", index=False)  
    print("Export ended on Documents")

#This function reads in a saved CSV file and transfers it to the inputted table in Postgres.

def Transfer_To_Postgres(connection,cursor,path,table):
    with open(path, 'r') as f:
        reader=csv.reader(f)
        next(reader) # Skip the header row.
        for record in reader:
            try:cursor.copy_from(f, table, sep=',')
            except:continue
    connection.commit()

#The main function defines all necessary variables and dictates the flow of action of the
#program.

def main():
    #Connection details for MongoDB Atlas connection
    DublinBikes = MongoClient('mongodb+srv://jhayes:DublinBikes@cluster0-dkeas.mongodb.net/test?retryWrites=true&w=majority')
    db = DublinBikes['Bikes']
    collection_Goteborg = db['GoteborgBikes']
    collection_Brussels = db['BrusselsBikes']
    collection_Dublin = db['DubBikes']
    collection_DubWeather=db['DubWeather']
   
    #Connection details for Postgres cloud
    connection = psycopg2.connect(
    host = 'dublinbikes.chpkrcuhpnzi.eu-west-1.rds.amazonaws.com',
    port = 5432,
    user = 'dublinbikes',
    password = 'dublinbikes',
    database='dbDublinBikes'
    )
    cursor=connection.cursor()     #postgres connection string
   
    sleeptime=300
   
    global doc_id_Brus,doc_id_Dub,doc_id_Gote,doc_id_Weather
    
    Gote_String='g'
    Brus_String='b'
    Dub_String='d'
   
    #path_Gote="C:\\Users\\johnh\\Documents\\MongoOut_Goteborg.csv"
    #path_Brus="C:\\Users\\johnh\\Documents\\MongoOut_Brussels.csv"
    #path_Dub="C:\\Users\\johnh\\Documents\\MongoOut_Dublin.csv"
    #path_Weather="C:\\Users\\johnh\\Documents\\MongoOut_DubWeather.csv"
    
    path_Gote="/home/ec2-user/MongoOut_Goteborg.csv"
    path_Brus="/home/ec2-user/MongoOut_Brussels.csv"
    path_Dub="/home/ec2-user/MongoOut_Dublin.csv"
    path_Weather="/home/ec2-user/MongoOut_Dublin.csv"

    table_Gote='bikes.goteborg_v2'
    table_Brus='bikes.brussels_v2'
    table_Dub='bikes.dublin_v2'
    #table_Weather='bikes.weather_v2'

    #cur_Gote=collection_Goteborg.find().limit(1000)
    #cur_Brus=collection_Brussels.find().limit(1000)
    #cur_Dub=collection_Dublin.find().limit(1000)
    #cur_Weather=collection_DubWeather.find().limit(1000)  
    #docs_Gote, doc_id_Gote=Continuous_Get_Bikes(cur_Gote,Gote_String)
    #docs_Brus, doc_id_Brus=Continuous_Get_Bikes(cur_Brus,Brus_String)
    #docs_Dub, doc_id_Dub=Continuous_Get_Bikes(cur_Dub,Dub_String) 
    #docs_Weather, doc_id_Weather=Continuous_Get_Weather(cur_Weather)
    #Save_To_CSV(docs_Gote,path_Gote)
    #Save_To_CSV(docs_Brus,path_Brus)
    #Save_To_CSV(docs_Dub,path_Dub)
    #Save_To_CSV(docs_Weather,path_Weather)
    #Transfer_To_Postgres(connection,cursor,path_Gote,table_Gote)
    #Transfer_To_Postgres(connection,cursor,path_Brus,table_Brus)
    #Transfer_To_Postgres(connection,cursor,path_Dub,table_Dub)
    #Transfer_To_Postgres(connection,cursor,path_Weather,table_Weather)
    doc_id_Brus = '5df65073de863ce0c2077777'
    doc_id_Gote = '5df679a3de863ce0c207c1f1'
    doc_id_Dub = '5df673c5de863ce0c207b5d5'
    doc_id_Weather = '5de38071942506cfbbe05731'
   
    while 1:
        cur_Brus=Find_from_ID(doc_id_Brus, collection_Brussels)
        cur_Gote=Find_from_ID(doc_id_Gote, collection_Goteborg)
        cur_Dub=Find_from_ID(doc_id_Dub, collection_Dublin)
        cur_Weather=Find_from_ID(doc_id_Weather, collection_DubWeather)
        docs_Brus, doc_id_Brus=Continuous_Get_Bikes(cur_Brus,Brus_String)
        docs_Gote, doc_id_Gote=Continuous_Get_Bikes(cur_Gote,Gote_String)
        docs_Dub, doc_id_Dub=Continuous_Get_Bikes(cur_Dub,Dub_String)
        docs_Weather, doc_id_Weather=Continuous_Get_Weather(cur_Weather)
        Save_To_CSV(docs_Brus,path_Brus)
        Save_To_CSV(docs_Gote,path_Gote)
        Save_To_CSV(docs_Dub,path_Dub)
        Save_To_CSV(docs_Weather,path_Weather)
        Transfer_To_Postgres(connection,cursor,path_Dub,table_Dub)
        Transfer_To_Postgres(connection,cursor,path_Brus,table_Brus)
        Transfer_To_Postgres(connection,cursor,path_Gote,table_Gote)
        Transfer_To_Postgres(connection,cursor,path_Weather,table_Weather)
if __name__ == '__main__':
   main()