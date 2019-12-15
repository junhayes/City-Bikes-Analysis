# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 20:20:33 2019

@author: johnh
"""

import requests
import json
import time
from pymongo import MongoClient

#Details for JCDecaux API call
city1='dublin'
city2='bruxelles'
city3='goteborg'
dbikes_apikey='63563e548cffe8bf37f4246538a2c2ee490b8637'
DublinBikes = MongoClient('mongodb+srv://jhayes:DublinBikes@cluster0-dkeas.mongodb.net/test?retryWrites=true&w=majority')
db = DublinBikes['Bikes']
collection_dubbikeData = db['DubBikes']
collection_brubikeData = db['BrusselsBikes']
collection_gotebikeData = db['GoteborgBikes']
collection_weatherData = db['DubWeather']

lat=53.347313
lon=-6.259015

sleeptime=300   #defined delay of 5 minutes between each file download

Dub_bikes_url='https://api.jcdecaux.com/vls/v1/stations?contract={}&apiKey={}'.format(city1,dbikes_apikey)
Bru_bikes_url='https://api.jcdecaux.com/vls/v1/stations?contract={}&apiKey={}'.format(city2,dbikes_apikey)
Gote_bikes_url='https://api.jcdecaux.com/vls/v1/stations?contract={}&apiKey={}'.format(city3,dbikes_apikey)
Weather_url='http://api.openweathermap.org/data/2.5/weather?lat={}&lon={}'.format(lat,lon)+'&appid=b35975e18dc93725acb092f7272cc6b8&units=metric'

while 1:
    res_Dub_Bikes=requests.get(Dub_bikes_url)
    res_Bru_Bikes=requests.get(Bru_bikes_url)
    res_Gote_Bikes=requests.get(Gote_bikes_url)
    res_Weather=requests.get(Weather_url)
    form_Dub_Bikes=res_Dub_Bikes.json()
    form_Bru_Bikes=res_Bru_Bikes.json()
    form_Gote_Bikes=res_Gote_Bikes.json()
    form_Weather=res_Weather.json()
    collection_dubbikeData.insert_many(form_Dub_Bikes)
    collection_brubikeData.insert_many(form_Bru_Bikes)
    collection_gotebikeData.insert_many(form_Gote_Bikes)
    collection_weatherData.insert_one(form_Weather)
    DublinBikes.close()
    time.sleep(sleeptime)