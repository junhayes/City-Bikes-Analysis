from pymongo import MongoClient
import time

def Delete_from_ID(cutoff, collection_name):
    collection_name.delete_many({'last_update':{'$lt':cutoff}})
    print("Documents deleted successfully")

def main():
    #Connection details for MongoDB Atlas connection
    DublinBikes = MongoClient('mongodb+srv://[username]:[database]@cluster0-dkeas.mongodb.net/test?retryWrites=true&w=majority')
    db = DublinBikes['Bikes']
    collection_Goteborg = db['GoteborgBikes']
    collection_Brussels = db['BrusselsBikes']
    collection_Dublin = db['DubBikes']
       
    sleeptime=43200
    
    while 1:
        now=round(time.time()*1000,0)
        number_days=7
        removal_time=number_days*60*60*24*1000
        cutoff=now-removal_time
        Delete_from_ID(cutoff, collection_Goteborg)
        Delete_from_ID(cutoff, collection_Brussels)
        Delete_from_ID(cutoff, collection_Dublin)
        time.sleep(sleeptime)
        
if __name__ == '__main__':
   main()