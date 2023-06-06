#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  9 14:52:59 2019

@author: orangebacked
"""

import requests
from lxml import html
import pandas as pd
import datetime
import re
from functools import reduce

def filtering(x):
    
    element=str(x)
    n=element.split("-")[-1]
    return (bool(re.search("^\d\d\d\d.",n)) and (not bool(re.search("https", element))))




r = requests.get("https://lasillavacia.com")

tree=html.fromstring(r.content)

list_elements = tree.xpath("//a/@href")

list_articles=list(set(filter(filtering, list_elements)))

r = requests.get("https://lasillavacia.com{}".format(list_articles[9]))

tree=html.fromstring(r.content)

title = str(tree.xpath("//h1[@class='article-title']/text()")[0])

date =  str(tree.xpath(".//meta[@name='created']/@content")[0])

content =  tree.xpath(".//p/span/text()")

content = reduce((lambda x,y: x+y), content)

content = re.sub(r"    .+?0", "", content)

content = re.sub(r"\\n08.+?.*", "", content)

reduce((lambda x,y: x+y), content)

element=str(list_elements[0])

n=element.split("-")[-1]
