#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 15 16:48:28 2018

@author: Maurice
"""
from elasticsearch import Elasticsearch
from elasticsearch import helpers


def create_entitie_index(es_client, index_name):
    index_body = {
  "settings": {
    "analysis": {
      "analyzer": {
        "analyzer_keyword": {
          "type": "custom",
          "tokenizer": "keyword",
          "filter": [
            "asciifolding",
            "lowercase"
          ]
        },
        "analyzer_shingle": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "asciifolding",
            "lowercase",
            "shingle"
          ]
        }
      }
    }
  },
  "mappings": {
    "your_type": {
      "properties": {
        "keyword": {
          "type": "text",
          "analyzer": "analyzer_keyword",
          "search_analyzer": "analyzer_shingle"
        }
      }
    }
  }
}
        
    
    es_client.indices.create(index_name, body = index_body)

def update_index_test(df,connection_info):
    es = Elasticsearch(
                        )

    ids = []
    for i in range(2*len(df)):
        ids.append(i)
    actions = []
    count = len(df)-1
    for i in df.country.unique():
        actions.append({'_op_type': 'update', '_index': 'test_keyword', '_type': 'word', '_id': ids[count],
                            'doc': {"id":ids[count],"keyword":i}})
        count+=1
    helpers.bulk(es, actions)
    es.indices.refresh(index="test_keyword")
