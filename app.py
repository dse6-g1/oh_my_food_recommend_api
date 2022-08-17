import requests
import json
import pandas as pd
from pandas import json_normalize 
from scipy.spatial.distance import cosine
from datetime import datetime
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class request_body(BaseModel): # Declare Input
    customer_id: str

def findDoc(mongokey, collection, database, datasource, dataendpoint, query) :
  url = "https://data.mongodb-api.com/app/%s/endpoint/data/v1/action/findOne"%dataendpoint
  payload = json.dumps({
      "collection" : collection,
      "database" : database,
      "dataSource" : datasource,
      "filter" : query
  })
  headers = {
      'Content-Type' : 'application/json',
      'Access-Control-Request-Headers' : '*',
      'api-key': mongokey
  }
  response = requests.request('POST', url, headers=headers, data=payload)
  return response.json()

def findAll(mongokey, collection, database, datasource, dataendpoint, sort) :
  url = "https://data.mongodb-api.com/app/%s/endpoint/data/v1/action/find"%dataendpoint
  payload = json.dumps({
      "collection" : collection,
      "database" : database,
      "dataSource" : datasource,
      "sort" : sort
  })
  headers = {
      'Content-Type' : 'application/json',
      'Access-Control-Request-Headers' : '*',
      'api-key': mongokey
  }
  response = requests.request('POST', url, headers=headers, data=payload)
  return response.json()

def findAllCustOrder() :
  mongokey = 'KPFD6nHURX1vkF7fugJsBhUSkEawEY0ntdoXPmglbiq35IZ3OQwnaXSU22A8mcPK'
  database = 'dse6g1customer'
  collection = 'customerorder'
  datasource = 'Cluster0'
  dataendpoint = 'data-quohf'
  sort = {
      'customer_id' : 1
  }
  return findAll(mongokey, collection, database, datasource, dataendpoint, sort)

def findMany(mongokey, collection, database, datasource, dataendpoint, sort, query) :
  url = "https://data.mongodb-api.com/app/%s/endpoint/data/v1/action/find"%dataendpoint
  payload = json.dumps({
      "collection" : collection,
      "database" : database,
      "dataSource" : datasource,
      "filter" : query,
      "sort" : sort
  })
  headers = {
      'Content-Type' : 'application/json',
      'Access-Control-Request-Headers' : '*',
      'api-key': mongokey
  }
  response = requests.request('POST', url, headers=headers, data=payload)
  return response.json()

def findOrderByCustomerId(temp_customer_id) :
  mongokey = 'KPFD6nHURX1vkF7fugJsBhUSkEawEY0ntdoXPmglbiq35IZ3OQwnaXSU22A8mcPK'
  database = 'dse6g1customer'
  collection = 'customerorder'
  datasource = 'Cluster0'
  dataendpoint = 'data-quohf'
  sort = {
      'customer_id' : 1
  }
  query = {
      'customer_id' : temp_customer_id
  }
  return findMany(mongokey, collection, database, datasource, dataendpoint, sort, query)

def findFoodNameById(temp_food_id) :
  mongokey = 'KPFD6nHURX1vkF7fugJsBhUSkEawEY0ntdoXPmglbiq35IZ3OQwnaXSU22A8mcPK'
  database = 'dse6g1customer'
  collection = 'foods'
  datasource = 'Cluster0'
  dataendpoint = 'data-quohf'
  query = {
      'food_id' : temp_food_id
  }
  return findDoc(mongokey, collection, database, datasource, dataendpoint, query)

def recommend_by_customer_order(in_customer_id) :
  personal_order_doc = findOrderByCustomerId(in_customer_id)
  personalFoodFreq = {}

  for doc in personal_order_doc.get("documents") :
    temp_food_id = doc.get("food_id")
    if None == personalFoodFreq.get(temp_food_id) :
      personalFoodFreq[temp_food_id] = doc.get("quantity")
    else :
      personalFoodFreq[temp_food_id] = personalFoodFreq.get(temp_food_id) + doc.get("quantity")

  maxFreq = 0
  for temp_food_id in personalFoodFreq.keys() :
    if maxFreq < personalFoodFreq.get(temp_food_id) :
      maxFreq = personalFoodFreq.get(temp_food_id)

  #print(personalFoodFreq)
  #print(maxFreq)

  custorder_doc = findAllCustOrder()
  temp_currentCust = "#startcustid#"
  orderbyuserDoc = {}
  temp_orderDoc = {}
  allFoodList = []
  foodNotPurchaseList = []
  for doc in custorder_doc.get("documents") :
    if (temp_currentCust != doc.get("customer_id")) :
      if (temp_currentCust != "#startcustid#") :
        orderbyuserDoc[temp_currentCust] = temp_orderDoc
        temp_orderDoc = {}
      temp_currentCust = doc.get("customer_id")
    temp_food_id = doc.get("food_id")

    if None == temp_orderDoc.get(temp_food_id) :
      temp_orderDoc[temp_food_id] = doc.get("quantity")
    else :
      temp_orderDoc[temp_food_id] = temp_orderDoc.get(temp_food_id) + doc.get("quantity")
    if temp_food_id not in allFoodList :
      allFoodList.append(temp_food_id)

  if len(temp_orderDoc) != 0 :
    orderbyuserDoc[temp_currentCust] = temp_orderDoc

  for food in allFoodList :
    if None == personalFoodFreq.get(food) :
      foodNotPurchaseList.append(food)

  #print(orderbyuserDoc)
  #print(allFoodList)
  #print(foodNotPurchaseList)

  df_custXfood = pd.DataFrame(index=orderbyuserDoc.keys(),columns=allFoodList)
  temp_row = 0

  for temp_customer_id in orderbyuserDoc.keys() :
    for j in range(0, len(allFoodList)) :
      temp_orderDoc = orderbyuserDoc.get(temp_customer_id)
      if None == temp_orderDoc.get(allFoodList[j]) :
        df_custXfood.iloc[temp_row,j] = 0
      else :
        df_custXfood.iloc[temp_row,j] = temp_orderDoc.get(allFoodList[j])
    temp_row += 1

  data_ibs = pd.DataFrame(index=allFoodList,columns=allFoodList)
  for i in range(0,len(data_ibs.columns)) :
    # Loop through the columns for each column
    for j in range(0,len(data_ibs.columns)) :
      # Fill in placeholder with cosine similarities
      data_ibs.iloc[i,j] = 1-cosine(df_custXfood.iloc[:,i],df_custXfood.iloc[:,j])

  maxScore = 0
  resultFoodId = "#None#"
  for temp_food_id in personalFoodFreq.keys() :
    for temp_food_for_score in foodNotPurchaseList :
      if maxScore < data_ibs._get_value(temp_food_id,temp_food_for_score) :
        maxScore = data_ibs._get_value(temp_food_id,temp_food_for_score)
        resultFoodId = temp_food_for_score

  foodDoc = findFoodNameById(resultFoodId)
  #print(foodDoc)
  resultFoodName = foodDoc.get("document").get("food_name")

  return resultFoodName

@app.get('/') # index of API
def index(): 
    return {'message': 'This API for recommend food for customer by order history'}

@app.post("/recommend-by-cust-order") # Service API
def predict(data : request_body):
    pred = recommend_by_customer_order(data.customer_id)
    return {f'{pred}'}

if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000)