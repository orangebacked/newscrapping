#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 10 14:04:55 2019

@author: orangebacked
"""


### import libraries
import requests 

from lxml import html

import pandas as pd

import datetime

from lxml import etree


### scrapping for one article 
r = requests.get('https://www.semana.com')

tree = html.fromstring(r.content)

list_articles = tree.xpath('.//a[contains(@class,"article-h-link")]/@href')

article = list_articles[3]

r1 = requests.get('https://www.semana.com{}'.format(article))

tree = html.fromstring(r1.content)

content = tree.xpath('.//div[@id="contentItem"]')

text_content = etree.tostring(content[0], pretty_print=True)

date = tree.xpath('.//span[@class="date"]/text()')[0]

tag = tree.xpath('.//a[@itemprop="articleSection"]/text()')[0]

title = tree.xpath('.//h1[@class="tittleArticuloOpinion"]/text()')[0]

row = [str(title), str(date), str(tag), str(text_content)]

df=pd.DataFrame([row],columns=["title", "date", "tag", "text_content"])




#### define scrapping function

def scrapping(article):
    
    r1 = requests.get('https://www.semana.com{}'.format(article))

    tree = html.fromstring(r1.content)

    content = tree.xpath('.//div[@id="contentItem"]')
    
    text_content = etree.tostring(content[0], pretty_print=True)
    
    date = tree.xpath('.//span[@class="date"]/text()')[0]
    
    tag = tree.xpath('.//a[@itemprop="articleSection"]/text()')[0]
    
    title = tree.xpath('.//h1[@class="tittleArticuloOpinion"]/text()')[0]
    
    row = [str(title), str(date), str(tag), str(text_content)]
    
    return row     



#### initial request of the page 
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

## dataframe
df=pd.DataFrame(dflist,columns=['price','name', 'brand'])


now = datetime.datetime.now()
y = now.year
m = now.month
d = now.day

# write csv
df.to_csv(f'semana-{y}-{m}-{d}.csv')

    