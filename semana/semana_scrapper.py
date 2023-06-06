#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 10:26:48 2019

@author: orangebacked
"""

import requests 

from lxml import html

import pandas as pd

import datetime

from lxml import etree

import google.cloud.storage

import re

import pandas_gbq

import html as hhhh


def scrapping(article):
    
    r1 = requests.get('https://www.semana.com{}'.format(article))

    tree = html.fromstring(r1.content)

    content = tree.xpath('.//div[@id="contentItem"]')
    
    text_content = etree.tostring(content[0], pretty_print=True)
    
    text_content = hhhh.unescape(str(text_content))

    text_content = re.sub(r"<.{0,200}>","",text_content)
    
    text_content = text_content.replace("\\n", "").replace("\r", "").replace("b'", "")

    date = tree.xpath('.//span[@class="date"]/text()')[0]
    
    date = date.replace("|", "")
    
    date = date.strip()
    
    list_date = date.split(" ")
    
    list_date[1]
    
    if list_date[2] == "PM":
        hour = str(int(list_date[1].split(":")[0]) + 12) + ":" + list_date[1].split(":")[1] + ":" + list_date[1].split(":")[2]
    else:
        hour = list_date[1]
    
    date = list_date[0].replace("/", "-")

    tag = tree.xpath('.//a[@itemprop="articleSection"]/text()')[0]
    
    tag= hhhh.unescape(str(tag))
    
    title = tree.xpath('.//h1[@class="tittleArticuloOpinion"]/text()')[0]
    
    title = title.strip()
    
    title = hhhh.unescape(str(title))
    
    item_id = int(tree.xpath('.//input[@id="itemId"]/@value')[0])
    
    row = [str(title), str(date), str(hour), str(tag), str(text_content), item_id]
    
    return row   


def loop_req():
    
    r = requests.get('https://www.semana.com')
    
    tree = html.fromstring(r.content)
    
    list_articles = tree.xpath('.//a[contains(@class,"article-h-link")]/@href')
    
    
    ## in order to create the df
    dflist = []
    for n,article in enumerate(list_articles):
        try:
            dflist.append(scrapping(article))
            print(n)
        except:
            print(n,'fail')
        
            pass
        
        df=pd.DataFrame(dflist,columns=["title", "date", "hour", "tag", "text_content", "item_id"])
    return df

def upload_bucket(csv):
    
    client = google.cloud.storage.Client()
    bucket = client.get_bucket('newscrapp')
    now = datetime.datetime.now()
    y = now.year
    m = now.month
    d = now.day
    blob = bucket.blob('semana/{}-{}-{}.csv'.format(y, m, d))
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
        df.drop_duplicates(inplace = True)
        csv = df.to_csv()
        upload_bucket(csv)
        pandas_gbq.to_gbq(df, 'news_scrapping.semana', project_id="servisentimen-servipolitics", if_exists='append')
        return csv
