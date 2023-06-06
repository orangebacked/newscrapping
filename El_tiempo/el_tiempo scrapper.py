#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 10:23:04 2019

@author: orangebacked
"""

import requests 

from lxml import html

import pandas as pd

import datetime

import google.cloud.storage

import pandas_gbq

from functools import reduce


def scrapping(article):
    
    dict_spanish_months = {"enero": "01",
                       "febrero" :"02", 
                       "marzo" :"03", 
                       "abril" :"04", 
                       "mayo" :"05", 
                       "junio" :"06", 
                       "julio ":"07", 
                       "agosto":"08", 
                       "septiembre":"09", 
                       "octubre":"10", 
                       "noviembre":"11", 
                       "diciembre":"12"
                       }
    
    r = requests.get('https://www.eltiempo.com{}'.format(article))
    
    tree = html.fromstring(r.content)
    
    content_text = tree.xpath('.//p[@class="contenido"]/text()')
    
    ## iterate may be a lambda function
    content = reduce((lambda x, y: x + y + "\n"), content_text)
    
    date = tree.xpath('.//span[@class="fecha"]/text()')[0].strip()
    
    day = date.strip().split(" ")[0]
    
    year = date.strip().split(" ")[3]
    
    month = dict_spanish_months[date.strip().split(" ")[2].lower()]
    
    if date.split(" ")[-1].lower() == "p.m":
        hour = str((int(date.strip().split(" ")[-2].split(":")[0]) + 12)) + ":" + date.strip().split(" ")[5].split(":")[1]
    else:
        hour = date.strip().split(" ")[-2]
        
    date = str(year) +  "-" + str(month) + "-" + str(day)
    ## may be serveral iterate through the tags
    
    tags_l = tree.xpath('.//h2[@class="tags-en-articulo-tag"]/a/text()')
    
    tags =  str([str(x) for x in tags_l])
    
    title = tree.xpath('.//h1[@itemprop="headline"]/text()')[0]
    
    item_id = int(r.url.split("-")[-1])
    
    row = [str(title), str(date), str(hour), str(tags), str(content), item_id]
    
    print(row)
    return row 


def loop_req():
    
    r = requests.get('https://www.eltiempo.com/')
    
    tree = html.fromstring(r.content)
    
    list_articles = tree.xpath('.//a[@class="title page-link"]/@href')
        
        
    ## in order to create the df
    dflist = []
    for n,article in enumerate(list_articles):
        try:
            dflist.append(scrapping(article))
            print(n)
        except:
            print(n,'fail')
        
            pass
        
        df = pd.DataFrame(dflist,columns=["title", "date", "hour", "tag", "text_content", "item_id"])
#        now = datetime.datetime.now()
#        y = now.year
#        m = now.month
#        d = now.day
    return df

def upload_bucket(csv):
    
    client = google.cloud.storage.Client()
    bucket = client.get_bucket('newscrapp')
    now = datetime.datetime.now()
    y = now.year
    m = now.month
    d = now.day
    blob = bucket.blob('el_tiempo/{}-{}-{}.csv'.format(y, m, d))
    blob.upload_from_string(csv)
    
   
def scrapper(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        df = loop_req()
        csv = df.to_csv()
        upload_bucket(csv)
        pandas_gbq.to_gbq(df, 'news_scrapping.el_tiempo', project_id="servisentimen-servipolitics", if_exists='append')
    return csv