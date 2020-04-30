# -*- coding: utf-8 -*-

import json
import pandas as pd

import urllib.request as re

product_json=[]
with re.urlopen('http://greendeck-datasets-2.s3.amazonaws.com/netaporter_gb_similar.json') as fp:
    for product in fp.readlines():
        product_json.append(json.loads(product))

# Dataframe to store data for products
ids = []
name = []
brand = []
regular_price = []
offer_price = []
basket_price = []

for values in product_json:
  ids.append(values['_id']['$oid'])
  name.append(values['name'])
  brand.append(values['brand']['name'])
  regular_price.append(values['price']['regular_price']['value'])
  offer_price.append(values['price']['offer_price']['value'])
  basket_price.append(values['price']['basket_price']['value'])

df = pd.DataFrame()
df['id'] = ids
df['name'] = name
df['brand.name'] = brand
df['regular_price'] = regular_price
df['offer_price'] = offer_price
df['basket_price'] = basket_price
df['discount'] = round((df['regular_price'] - df['offer_price'])/df['regular_price'] * 100,2)

# Dataframe to store data for products and its competition
id = []
brand = []
bask_price = []
website_id = []
comp_name = []
comp_bask_price = []
name = []
for values in product_json:
  prod_name = []
  prod_value = []
  for key,value in values['similar_products']['website_results'].items():
    
    if len(value['knn_items']) != 0:
      for items in value['knn_items']:
        id.append(values['_id']['$oid'])
        name.append(values['name'])
        brand.append(values['brand']['name'])
        bask_price.append(values['price']['basket_price']['value'])
        website_id.append(key)
        comp_name.append(items['_source']['name'])
        comp_bask_price.append(items['_source']['price']['basket_price']['value'])

df_similar = pd.DataFrame({'id':id,'name':name,'brand.name':brand,'prod_basket_price':bask_price,'competition':website_id,'comp_name':comp_name,'competition_basket_price':comp_bask_price})

#!pip install flask-ngrok

import operator
def operation(inp, relate, cut):
    ops = {'>': operator.gt,
           '<': operator.lt,
           '>=': operator.ge,
           '<=': operator.le,
           '==': operator.eq,
           '!=': operator.ne}
    return ops[relate](inp, cut)

# Takes the query and returns query_type,and operations which are in filter
def structure_query(data):
    query_type = data['query_type']
    op1 = []
    opr = []
    op2 = []
    if 'filters' in data.keys():
      for val in data['filters']:
        op1.append(val['operand1'])
        opr.append(val['operator'])
        op2.append(val['operand2'])
    else:
      op1 = ['id']
      opr = ['!=']
      op2 = [None]


    return query_type,op1,opr,op2

# Working with data frames to get results for the queries
def answer_query(query):
  query_type,operand1,operator,operand2 = structure_query(query)
  result = []
  inp = operand1[0]
  rel = operator[0]
  cut = operand2[0]
  if query_type == 'discounted_products_list':
    
    result = list(df[operation(df[inp] , rel, cut)]['id'])
    return {query_type:result}

  if query_type == 'discounted_products_count|avg_discount':
    result = (df[operation(df[inp] , rel, cut)]['discount'])
    count = result.shape[0]
    avg_discount = result.mean()
    queries = query_type.split('|')
    return {queries[0] : count , queries[1]:avg_discount}

  if query_type == 'expensive_list':
    df_max = df_similar.sort_values('competition_basket_price', ascending=False).drop_duplicates(['id'])
    result = list((df_max[(operation(df_max[inp] , rel, cut)) 
              & (df_max['prod_basket_price'] > df_max['competition_basket_price'])]['id']))
    
    return {query_type:result}
  
  if query_type == 'competition_discount_diff_list':
    inp2 = operand1[1]
    rel2= operator[1]
    cut2 = operand2[1] 
    df_max = df_similar.sort_values('competition_basket_price', ascending=False).drop_duplicates(['id'])
    df_max['discount_diff'] = round((df_max['prod_basket_price'] - df_max['competition_basket_price'])/df_max['prod_basket_price']*100,2)
    result = list(df_max[(operation(df_max[inp] , rel, cut)) & (operation(df_max[inp2] , rel2, cut2)) ]['id'])  
    return {query_type:result}




# Flask Application
from flask import Flask,request,jsonify
app = Flask(__name__)
@app.route("/",methods=['POST'])
def home():
    data = request.get_json()
    result = answer_query(data)
    return jsonify(result)
  
app.run()

