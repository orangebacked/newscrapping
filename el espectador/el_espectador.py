#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 10:28:49 2019

@author: orangebacked
"""

import requests 

from lxml import html

import pandas as pd

import datetime

from lxml import etree

import google.cloud.storage

from lxml import etree

from functools import reduce

import re 

#############################################
### filtering function bloody dificult ######
#############################################

def filtering(x):
    
    a = lambda x: len(str(x).split("/") )>= 3
    
    b = lambda x: str(x).split(":").count("https") == 0
    
    c = bool(re.search(r'-[0-9]', x))
    
    e = not bool(re.search(r'columna', x))
    
    return (a(x) and b(x) and e and c )
    

def scrapping(article):
    
    r = requests.get('https://www.elespectador.com{}'.format(article))
    
    tree = html.fromstring(r.content)
    
    content_text = tree.xpath('.//script[@type="application/ld+json"]')
    
    content = content_text[1]
    
    content_r = etree.tostring(content)
     
    conn = str(content_r)

    m = re.search(r"description(.*)keyword" , conn)

    dict_weird_enc = {r"\\\\u00e1": "á",
                  r"\\\\u00e9": "é",
                  r"\\\\u00ed": "í",
                  r"\\\\u00f3": "ó",
                  r"\\\\u00fa": "ú",
                  r"\\\\u00c1": "Á",
                  r"\\\\u00c9": "É",
                  r"\\\\u00cd": "Í",
                  r"\\\\u00d3": "Ó",
                  r"\\\\u00da": "Ú",
                  r"\\\\u00f1": "ñ",
                  r"\\\\u00d1": "Ñ"
                  }

    t = m.group()
    
    for i in dict_weird_enc :
       t =  re.sub(i, dict_weird_enc[i], t)
    t = re.sub("\\\\r", "", t)
        
    t = re.sub("\\\\n", "", t)
    
    t = re.sub("\\\\\\\\", "", t)
    
    t = re.sub(r"\\u.{4}", "", t)
    
    t = re.sub(r"\\", "",t) 
    
    t = re.sub(r"u00/n/n", "",t)
    
    t = re.sub(r"u00/n/n", "",t)
    
    t = re.sub(r"description", "",t)
    
    t = re.sub(r"keyword", "",t)
    
    name = str(tree.xpath('.//title/text()')[0])
    
    name.encode('ascii', errors='ignore').decode('utf8', errors='ignore')
    
    date = str(tree.xpath('.//meta[@property="article:published_time"]/@content')[0])
    
    item_id = int(tree.xpath('.//meta[@name = "cXenseParse:articleid"]/@content')[0])
    
    tag = r.url.split("/")[4]
    
    row = [name, date, tag, t, item_id]
    
    return row 

def upload_bucket(csv):
    
    client = google.cloud.storage.Client()
    bucket = client.get_bucket('newscrapping')
    now = datetime.datetime.now()
    y = now.year
    m = now.month
    d = now.day
    blob = bucket.blob('el-espectador/{}-{}-{}.csv'.format(y, m, d))
    blob.upload_from_string(csv)


def loop_req():
    
    r = requests.get('https://www.elespectador.com')
    
    tree = html.fromstring(r.content)
    
    list_articles_ele = tree.xpath(".//a/@href")
    
    list_articles = list(filter(filtering, list_articles_ele))
    
    
    dflist = []
    for n,article in enumerate(list_articles):
        try:
            dflist.append(scrapping(article))
            print(n)
        except:
            print(n,'fail')
        
            pass
        
        df=pd.DataFrame(dflist,columns=["title", "date", "tag", "text_content", "item_id"])
#        now = datetime.datetime.now()
#        y = now.year
#        m = now.month
#        d = now.day
    return df
    
	    
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
        df.drop_duplicates(inplace = True)
        csv = df.to_csv()
        upload_bucket(csv)
        pandas_gbq.to_gbq(df, 'news_scrapping.el_espectador', project_id="servisentimen-servipolitics", if_exists='append')
        return csv
