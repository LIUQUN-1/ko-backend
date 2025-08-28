import re
import codecs, sys
from collections import Counter
import json
import sys
import time
from urllib.parse import quote, unquote
import random
from neo4j import GraphDatabase

import argparse

import json


def main(request):
    name=request.GET["name"]

    print(name)
    driver = GraphDatabase.driver("bolt://114.213.233.177:8687", auth=("neo4j", "DMiChao"))
    session = driver.session()

    query_word='''
        MATCH (h:Wikipedia) where h.name ={arg_1}  OPTIONAL match (h)-[r:Contain]-(t:Wikipedia) return h, r, t limit 50 union 
        MATCH (h:Wikipedia) where h.name = {arg_1}  OPTIONAL match (h)-[r:belong]-(t:Category) return h, r, t limit 100 union  
        MATCH (h:Wikipedia)-[:belong]-(t:Category) 
        where h.name = {arg_1}  
        OPTIONAL match p=allShortestPaths((t)-[:Sub|belong*..10]-())  
        with relationships(p) as rels  
        unwind rels as rel  
        return startNode(rel) as h ,rel as r, endNode(rel) as t limit 200
        '''
    results_place = session.run(query_word, parameters={"arg_1": name})

    sum_data=[]
    for res in results_place:

        Triple={}
        Triple["keys"]=["h","r","t"]
        Triple["length"]=3
        Triple["_fieldLookup"]={
            "h": 0,
            "r": 1,
            "t": 2
        }
        Triple["_fields"]=[]

        if res["h"] is not None:
            h_field = {}
            h_field["identity"]={}
            h_field["identity"]["high"]=0
            h_field["identity"]["low"]=res['h'].id
            h_field["labels"]=list(set(res['h'].labels))
            h_field["properties"]=res['h']._properties
            Triple["_fields"].append(h_field)
        else:
            Triple["_fields"].append(None)

        if res["r"] is not None:
            print(res["r"])
            r_field = {}
            r_field["identity"]={}
            r_field["identity"]["high"]=0
            r_field["identity"]["low"]=res['t'].id

            r_field["start"]={}
            r_field["start"]["high"]=0
            r_field["start"]["low"]=res["r"].nodes[0].id

            r_field["end"]={}
            r_field["end"]["high"]=0
            r_field["end"]["low"]=res["r"].nodes[1].id

            r_field["type"]=res["r"].type
            r_field["properties"]=res['r']._properties
            Triple["_fields"].append(r_field)
        else:
            Triple["_fields"].append(None)

        if res["t"] is not None:
            t_field = {}
            t_field["identity"]={}
            t_field["identity"]["high"]=0
            t_field["identity"]["low"]=res['t'].id
            t_field["labels"]=list(set(res['t'].labels))
            t_field["properties"]=res['t']._properties
            Triple["_fields"].append(t_field)
        else:
            Triple["_fields"].append(None)

        sum_data.append(Triple)
    session.close()
    test_data = [
        {
            "keys": [
                "h",
                "r",
                "t"
            ],
            "length": 3,
            "_fields": [
                {
                    "identity": {
                        "low": 1070441,
                        "high": 0
                    },
                    "labels": [
                        "Wikipedia"
                    ],
                    "properties": {
                        "name": "图论",
                        "url": "https://zh.wikipedia.org/wiki/%E5%9B%BE%E8%AE%BA"
                    }
                },
                None,
                None
            ],
            "_fieldLookup": {
                "h": 0,
                "r": 1,
                "t": 2
            }
        },
        {
            "keys": [
                "h",
                "r",
                "t"
            ],
            "length": 3,
            "_fields": [
                {
                    "identity": {
                        "low": 2999661,
                        "high": 0
                    },
                    "labels": [
                        "Wikipedia"
                    ],
                    "properties": {
                        "name": "图论",
                        "url": "https://zh.wikipedia.org/wiki/图论"
                    }
                },
                {
                    "identity": {
                        "low": 79275770,
                        "high": 0
                    },
                    "start": {
                        "low": 2999661,
                        "high": 0
                    },
                    "end": {
                        "low": 1758165,
                        "high": 0
                    },
                    "type": "Contain",
                    "properties": {}
                },
                {
                    "identity": {
                        "low": 1758165,
                        "high": 0
                    },
                    "labels": [
                        "Wikipedia"
                    ],
                    "properties": {
                        "name": "最小生成树",
                        "url": "https://zh.wikipedia.org/wiki/最小生成树"
                    }
                }
            ],
            "_fieldLookup": {
                "h": 0,
                "r": 1,
                "t": 2
            }
        }
    ]
    return test_data