# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 16:02:23 2019

@author: jshekhh
"""

import sys
import pymongo
import datetime
import re
import pandas as pd
import json


database = 'exl_tag' 
conn_url = 'mongodb://{}:{}@{}/{}?authMechanism=SCRAM-SHA-1' 
conn_url = conn_url.format('st_exl_tag', 'm8bPV5NuQN04', 'tgl-mongodb11.rctanalytics.com,tgl-mongodb12.rctanalytics.com,tgl-mongodb13.rctanalytics.com','exl_tag') 
client = MongoClient(conn_url) 
db = client.exl_tag

#print(db.ExcludedChains.find_one())
print("Connection Successful\n\n")


# Joining chaon categories and categories
def join_chain_catg_seg():
    db.temp_chain_categories.drop()
    db.chain_categories.aggregate([
        {
            "$lookup":
            {
                "from":"categories",
                "localField": "CATEGORY_ID",
                "foreignField": "CATEGORY_ID",
                "as":"tmp"
            }
        },
        {
            "$project":
            {
                "CHAIN_ID":1, "CATEGORY_ID":1, "ACTIVE_IND":1, "tmp.CATEGORY_ID":1, "tmp.SEGMENT_ID":1, "tmp.SEG_TYPE_ID":1, "tmp.CATEGORY_NAME":1, "tmp.SEGMENT_NAME":1
            }                                  
        }, 
        {
            "$unwind":"$CHAIN_ID"
        },
        {
            "$out": "temp_chain_categories"
        }
    ])
# flag = join_chain_catg_seg()
# print('''Collection - exl_tag.temp_chain_categories CREATED\n\n''')


# Removing documents having ACTIVE_IND = N and SEgment_ID > 7
def temp_chain_categories_filter():
    #db.temp_chain_categories_1.drop()
    db.temp_chain_categories.aggregate([
        {
            "$match": {
                "tmp.SEGMENT_ID": {"$in": [1,2,3,4,5,6,7]},
                "ACTIVE_IND": "Y"
            }
        },
        {"$out": "temp_chain_categories_1"}
    ])
# flag = temp_chain_categories_filter()
# print('''Collection - exl_tag.temp_chain_categories_1 CREATED\n\n''')

# Adding indicators for special Chains
def temp_chain_categories_special_ind():
    #db.temp_chain_categories_2.drop()
    db.temp_chain_categories_1.aggregate([
    {
        "$lookup":
        {
            "from":"chain_seg",
            "localField": "CHAIN_ID",
            "foreignField": "CHAIN_ID",
            "as":"ind"
        }
    },
    {
        "$project":
        {
            "CHAIN_ID":1, "CATEGORY_ID":1, "ACTIVE_IND":1, "tmp.SEGMENT_ID":1, "tmp.SEG_TYPE_ID":1, "tmp.CATEGORY_NAME":1, "tmp.SEGMENT_NAME":1, "ind.LUXURY_IND":1, "ind.EVERYDAY_IND":1, "ind.VALUE_IND":1, "ind.CLOSEOUT_IND":1, "ind.CHAIN_ID":1
        }                                  
    },
    {"$out": "temp_chain_categories_2"}
    ])
# flag = temp_chain_categories_special_ind()
# print('''Collection - exl_tag.temp_chain_categories_2 CREATED\n\n''')


# Removing special chains {Chain_id with any indicator as '1' is treated as special chain}
def temp_chain_categories_special_ind_filter():
    #db.temp_chain_categories_3.drop()
    db.temp_chain_categories_2.aggregate([
    { 
     "$match": {
          "$and": [ 
              {"ind.LUXURY_IND": {"$in": [0]}}, 
              {"ind.EVERYDAY_IND": {"$in": [0]}}, 
              {"ind.VALUE_IND": {"$in": [0]}}, 
              {"ind.CLOSEOUT_IND": {"$in": [0]}}
          ]
     }
   },
   {"$out": "temp_chain_categories_3"}
  ])
# flag = temp_chain_categories_special_ind_filter()
# print('''Collection - exl_tag.temp_chain_categories_3 CREATED\n\n''')

------------------------------------------------------------Start executing from here------------------------------------------------
# Adding chain exclusion flags
def temp_chain_categories_chain_excl():
    #db.temp_chain_categories_4.drop()
    db.temp_chain_categories_3.aggregate([
    {
     "$lookup":
     {
         "from":"ExcludedChains",
         "localField": "CHAIN_ID",
         "foreignField": "CHAIN_ID",
         "as":"excl"
     }
    },
    {
     "$project":
     {
        "CHAIN_ID":1, "CATEGORY_ID":1, "ACTIVE_IND":1, "tmp.SEGMENT_ID":1, "tmp.SEG_TYPE_ID":1, "tmp.CATEGORY_NAME":1, "tmp.SEGMENT_NAME":1,
        "excl_flag":{ "$cond":{"if": {"$eq": ["$excl",[]]}, "then":0, "else":1} }}
    },
    {"$out": "temp_chain_categories_4"}
    ])
    # flag = temp_chain_categories_chain_excl()
    # print('''Collection - exl_tag.temp_chain_categories_4 CREATED\n\n''')


# Adding chain segment flags
def temp_chain_categories_seg_flag():
    #db.temp_chain_categories_5.drop()
    db.temp_chain_categories_4.aggregate([
    {
        "$lookup":
        {
            "from":"chain_seg",
            "localField": "CHAIN_ID",
            "foreignField": "CHAIN_ID",
            "as":"seg"
        }
    },
    {
        "$project":
        {
            "CHAIN_ID":1, "CATEGORY_ID":1, "ACTIVE_IND":1, "tmp.SEGMENT_ID":1, "tmp.SEG_TYPE_ID":1, "tmp.CATEGORY_NAME":1, "tmp.SEGMENT_NAME":1,"excl_flag":1,
            "chain_seg_flag": { "$cond":{"if": {"$eq": ["$seg",[]]}, "then":0, "else":1}}
        }                                  
    },
    {"$out": "temp_chain_categories_5"}
    ])
# flag = temp_chain_categories_seg_flag()
# print('''Collection - exl_tag.temp_chain_categories_5 CREATED\n\n''')
------------------------------------------------------------New code------------------------------------------------

# Mapping sitedata details to the chain details
def add_sitedata_to_chain_details():
    # Unwind chain sites aggregate documents
    db.chain_sites.aggregate([
        {
            "$unwind": "$siteIds"
        },
        {
            "$project": {"CHAIN_ID":1,"CHAIN_NAME":1, "SITE_ID":"$siteIds"
            }
        },
        {
            "$out": "temp_chain_sites_unwound"
        }
    ])
    
    # Creating index in chain_sites_unwound based on SITE_ID. This will lead to faster lookups on SITE_ID field.
    db.tmp_chain_sites_unwound.create_index([("SITE_ID", pymongo.DESCENDING)])
    db.sitedata.create_index([("siteId", pymongo.DESCENDING)])
    
    
    
    db.sitedata.aggregate([
        {
            "$lookup": {
                "from": "temp_chain_sites_unwound",
                "localField": "siteId",
                "foreignField": "SITE_ID",
                "as": "chain_info"
            }
        },
        {
            "$unwind": { "path": "$chain_info", "preserveNullAndEmptyArrays": True }
        },
        {
            "$project": {
                "SITE_ID": "$siteId",
                "SITE_NAME": "$name",
                "CHAIN_ID": { "$ifNull": [ "$chain_info.CHAIN_ID", 0]},
                "CHAIN_NAME": { "$ifNull": [ "$chain_info.CHAIN_NAME", "Undefined"]},
                "ADDRESS": "$addrs",
                "SITE_TYPE": "remedy.type" 
        }
        },
        {
            "$out": "temp_sitedata_1"
        }
    ])


# Adding exclusion flag to the sitedata table
def select_useable_sites():
    db.temp_sitedata_1.aggregate([
    {
      "$match": {
        "excl_flag": 1,
        "chain_seg_flag": 1,
        "ACTIVE_IND":"Y",
        "SITE_TYPE":"Retail",
        "ADDRESS.ctry":"United States",
        "ADDRESS.zip":{'$ne': ''}
      }
    },
    {
     "$project":{
        "SITE_ID":1
        "SITE_NAME":1
        "CHAIN_ID":1
        "CHAIN_NAME":1
        "ADDRESS":1
        "SITE_TYPE":1
     }  
    },
    {
        "$out": "temp_useable_sites_1"
    }
])

# Adding segment to chain sites
def add_segment_to_sitedata:
    db.temp_useable_sites_1.aggregate([
    {
        "$lookup":
        {
            "from":"temp_chain_categories_5",
            "localField": "CHAIN_ID",
            "foreignField": "CHAIN_ID",
            "as":"site_agg"
        }
    },
    {
        "$project":{
        "CHAIN_ID":1, "CATEGORY_ID":1, "ACTIVE_IND":1, "tmp.SEGMENT_ID":1, "tmp.SEG_TYPE_ID":1, "tmp.CATEGORY_NAME":1, "tmp.SEGMENT_NAME":1,"excl_flag":1,
            "chain_seg_flag":1, "site_agg.SITE_ID":1, "site_agg.SITE_NAME":1, "site_agg.CHAIN_ID":1, "site_agg.CHAIN_NAME":1, "site_agg.ADDRESS":1, "site_agg.SITE_TYPE":1,
        }
    },
    {
        "$out":"temp_useable_sites_2"
    }
    ])





client.close()


