# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 16:02:23 2019

@author: jshekhh
"""

import sys
import pymongo
from pymongo import MongoClient
import datetime
import re
import pandas as pd
import json


database = 'exl_tag' 
conn_url = 'mongodb://{}:{}@{}/{}?authMechanism=SCRAM-SHA-1' 
exl_conn_url = conn_url.format('st_exl_tag', 'm8bPV5NuQN04', 'tgl-mongodb11.rctanalytics.com,tgl-mongodb12.rctanalytics.com,tgl-mongodb13.rctanalytics.com','exl_tag') 
exl_client = MongoClient(exl_conn_url) 
exl_db = exl_client.exl_tag

# conn_url_siteref = conn_url.format('st_market_int', 'k59cPkRd8xk2', 'tgl-mongodb21.rctanalytics.com,tgl-mongodb22.rctanalytics.com,tgl-mongodb23.rctanalytics.com','siteref_copyofprod') 
# sref_client = MongoClient(conn_url_siteref) 
# sref_db = client.siteref_copyofprod

mi_conn_url = conn_url.format('st_market_int', 'DbMGP0GBiBw2', 'tgl-mongodb11.rctanalytics.com,tgl-mongodb12.rctanalytics.com,tgl-mongodb13.rctanalytics.com','market_intelligence') 
mi_client = MongoClient(mi_conn_url) 
mi_db = mi_client.market_intelligence

# print(exl_db.ExcludedChains.find_one())
print("Connection Successful\n\n")


# Joining chain categories and categories
def join_chain_catg_seg():
    exl_db.chain_categories.aggregate([
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
                "CHAIN_ID":"$CHAIN_ID",
                "CATEGORY_ID":"$CATEGORY_ID",
                "ACTIVE_IND":"$ACTIVE_IND",
                "CATEGORY_ID_2":"$tmp.CATEGORY_ID",
                "SEGMENT_ID":"$tmp.SEGMENT_ID",
                "SEG_TYPE_ID":"$tmp.SEG_TYPE_ID",
                "CATEGORY_NAME":"$tmp.CATEGORY_NAME",
                "SEGMENT_NAME":"$tmp.SEGMENT_NAME"
            }                                  
        }, 
        {
            "$unwind":"$CHAIN_ID"
        },
        {
            "$out": "temp_chain_categories_0"
        }
    ])
# flag = join_chain_catg_seg()
# print('''Collection 1 - exl_tag.temp_chain_categories_0 CREATED\n\n''')

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Removing documents having ACTIVE_IND = N and SEgment_ID > 7
def temp_chain_categories_filter():
    exl_db.temp_chain_categories_0.aggregate([
        {
            "$match": {
                "SEGMENT_ID": {"$in": [1,2,3,4,5,6,7]},
                "ACTIVE_IND": "Y"
            }
        },
        {"$out": "temp_chain_categories_1"}
    ])
# flag = temp_chain_categories_filter()
# print('''Collection 2 - exl_tag.temp_chain_categories_1 CREATED\n\n''')

# Adding indicators for special Chains
def temp_chain_categories_special_ind():
    exl_db.temp_chain_categories_1.aggregate([
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
            "CHAIN_ID":"$CHAIN_ID",
            "CATEGORY_ID":"$CATEGORY_ID",
            "ACTIVE_IND":"$ACTIVE_IND",
            "SEGMENT_ID":"$SEGMENT_ID",
            "SEG_TYPE_ID":"$SEG_TYPE_ID",
            "CATEGORY_NAME":"$CATEGORY_NAME",
            "SEGMENT_NAME":"$SEGMENT_NAME",
            "LUXURY_IND":"$ind.LUXURY_IND",
            "EVERYDAY_IND":"$ind.EVERYDAY_IND",
            "VALUE_IND":"$ind.VALUE_IND",
            "CLOSEOUT_IND":"$ind.CLOSEOUT_IND",
            "CHAIN_ID":"$ind.CHAIN_ID"
        }                                  
    },
    {"$out": "temp_chain_categories_2"}
    ])
# flag = temp_chain_categories_special_ind()
# print('''Collection 3 - exl_tag.temp_chain_categories_2 CREATED\n\n''')


# Removing special chains {Chain_id with any indicator as '1' is treated as special chain}
def temp_chain_categories_special_ind_filter():
    exl_db.temp_chain_categories_2.aggregate([
    { 
     "$match": {
          "$and": [ 
              {"LUXURY_IND": {"$in": [0]}}, 
              {"EVERYDAY_IND": {"$in": [0]}}, 
              {"VALUE_IND": {"$in": [0]}}, 
              {"CLOSEOUT_IND": {"$in": [0]}}
          ]
     }
   },
   {"$out": "temp_chain_categories_3"}
  ])
# flag = temp_chain_categories_special_ind_filter()
# print('''Collection 4 - exl_tag.temp_chain_categories_3 CREATED\n\n''')


# Adding chain exclusion flags
def temp_chain_categories_chain_excl():
    exl_db.temp_chain_categories_3.aggregate([
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
        "CHAIN_ID":1, "CATEGORY_ID":1, "ACTIVE_IND":1, "SEGMENT_ID":1, "SEG_TYPE_ID":1, "CATEGORY_NAME":1, "SEGMENT_NAME":1,
        "excl_flag":{ "$cond":{"if": {"$eq": ["$excl",[]]}, "then":0, "else":1} }}
    },
    {"$out": "temp_chain_categories_4"}
    ])
# flag = temp_chain_categories_chain_excl()
# print('''Collection 5 - exl_tag.temp_chain_categories_4 CREATED\n\n''')


# Adding chain segment flags
def temp_chain_categories_seg_flag():
    exl_db.temp_chain_categories_4.aggregate([
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
            "CHAIN_ID":1, "CATEGORY_ID":1, "ACTIVE_IND":1, "SEGMENT_ID":1, "SEG_TYPE_ID":1, "CATEGORY_NAME":1, "SEGMENT_NAME":1,"excl_flag":1,
            "chain_seg_flag": { "$cond":{"if": {"$eq": ["$seg",[]]}, "then":0, "else":1}}
        }                                  
    },
    {"$out": "temp_chain_categories_5"}
    ])
# flag = temp_chain_categories_seg_flag()
# print('''Collection 6 - exl_tag.temp_chain_categories_5 CREATED\n\n''')


# Mapping sitedata details to the chain details
def add_sitedata_to_chain_details():
    # Unwind chain sites aggregate documents
    exl_db.chain_sites.aggregate([
        {
            "$unwind": "$siteIds"
        },
        {
            "$project": {"CHAIN_ID":"$chainId","CHAIN_NAME":"$chainName", "SITE_ID":"$siteIds","_id":0
            }
        },
        {
            "$out": "temp_chain_sites_unwound"
        }
    ])
    
    print('''Collection 7 - exl_tag.temp_chain_sites_unwound CREATED\n\n''')
    # Creating index in chain_sites_unwound based on SITE_ID. This will lead to faster lookups on SITE_ID field.
    exl_db.temp_chain_sites_unwound.create_index([("SITE_ID", pymongo.DESCENDING)])
    print('''Collection 7 - exl_tag.temp_chain_sites_unwound INDEXED\n\n''')
    exl_db.sitedata.create_index([("siteId", pymongo.DESCENDING)])
    print('''Collection 8 - exl_tag.sitedata INDEXED\n\n''')
    
    
    
    exl_db.sitedata.aggregate([
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
                "_id":0,
                "SITE_ID": "$siteId",
                "SITE_NAME": "$name",
                "CHAIN_ID": { "$ifNull": [ "$chain_info.CHAIN_ID", 0]},
                "CHAIN_NAME": { "$ifNull": [ "$chain_info.CHAIN_NAME", "Undefined"]},
                "ADDRESS": "$addrs",
                "SITE_TYPE": "$remedy.type" 
        }
        },
        {
            "$out": "temp_sitedata_1"
        }
    ])

# flag = add_sitedata_to_chain_details()
# print('''Collection 7 - exl_tag.temp_chain_sites_unwound CREATED\n\n''')
# print('''Collection 8 - exl_tag.temp_sitedata_1 CREATED\n\n''')



# Add chainId and other details to temp_sitedata_1 [Join with exl_tag.temp_chain_categories_5 ]
def add_chain_id_to_siteId():
    exl_db.temp_sitedata_1.aggregate([
        {
        "$lookup":{
            "from":"temp_chain_categories_5",
            "localField":"CHAIN_ID",
            "foreignField":"CHAIN_ID",
            "as":"ste"
            }
        },
        {
        "$project":{
            "SITE_ID":"$SITE_ID",
            "SITE_NAME":"$SITE_NAME",
            "CHAIN_ID":"$CHAIN_ID",
            "CHAIN_NAME":"$CHAIN_NAME",
            "ADDRESS":"$ADDRESS",
            "SITE_TYPE":"$SITE_TYPE",
            "CHAIN_ID_2":"$ste.CHAIN_ID",
            "CATEGORY_ID":"$ste.CATEGORY_ID",
            "ACTIVE_IND":"$ste.ACTIVE_IND",
            "SEGMENT_ID":"$ste.SEGMENT_ID",
            "SEG_TYPE_ID":"$ste.SEG_TYPE_ID",
            "CATEGORY_NAME":"$ste.CATEGORY_NAME",
            "SEGMENT_NAME":"$ste.SEGMENT_NAME",
            "excl_flag":"$ste.excl_flag",
            "chain_seg_flag":"$ste.chain_seg_flag"
            }
        },
        {
            "$out":"temp_sitedata_2"
        }
        ])


flag = add_chain_id_to_siteId()
print('''Collection 9 - exl_tag.temp_sitedata_2 CREATED\n\n''')
#----------------------------------------------------------------------------------------------------------------------------------------------------------


## Adding exclusion flag to the sitedata table
def select_useable_sites():
    exl_db.temp_sitedata_2.aggregate([
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
        "SITE_ID":"$SITE_ID",
        "SITE_NAME":"$SITE_NAME",
        "CHAIN_ID":"$CHAIN_ID",
        "CHAIN_NAME":"$CHAIN_NAME",
        "ADDRESS":"$ADDRESS",
        "SITE_TYPE":"$SITE_TYPE",
        "CHAIN_ID":"$CHAIN_ID",
        "CATEGORY_ID":"$CATEGORY_ID",
        "ACTIVE_IND":"$ACTIVE_IND",
        "SEGMENT_ID":"$SEGMENT_ID",
        "SEG_TYPE_ID":"$SEG_TYPE_ID",
        "CATEGORY_NAME":"$CATEGORY_NAME",
        "SEGMENT_NAME":"$SEGMENT_NAME",
        "excl_flag":"$excl_flag",
        "chain_seg_flag":"$chain_seg_flag"
     }  
    },
    {
        "$out": "temp_useable_sites_1"
    }
])

flag = select_useable_sites()
print('''Collection 10 - exl_tag.temp_useable_sites_1 CREATED\n\n''')


# # Adding segment to chain sites
# def add_segment_to_sitedata():
#     exl_db.temp_useable_sites_1.aggregate([
#     {
#         "$lookup":
#         {
#             "from":"temp_chain_categories_5",
#             "localField": "CHAIN_ID",
#             "foreignField": "CHAIN_ID",
#             "as":"site_agg"
#         }
#     },
#     {
#         "$project":{
#         "CHAIN_ID":"$site_agg.CHAIN_ID",
#         "CATEGORY_ID":"$site_agg.CATEGORY_ID",
#         "ACTIVE_IND":"$site_agg.ACTIVE_IND",
#         "SEGMENT_ID":"$site_agg.SEGMENT_ID",
#         "SEG_TYPE_ID":"$site_agg.SEG_TYPE_ID",
#         "CATEGORY_NAME":"$site_agg.CATEGORY_NAME",
#         "SEGMENT_NAME":"$site_agg.SEGMENT_NAME",
#         "excl_flag":"$site_agg.excl_flag",
#         "chain_seg_flag":"$site_agg.chain_seg_flag",
#         "SITE_ID":"$SITE_ID",
#         "SITE_NAME":"$SITE_NAME",
#         "CHAIN_ID":"$CHAIN_ID",
#         "CHAIN_NAME":"$CHAIN_NAME",
#         "ADDRESS":"$ADDRESS",
#         "SITE_TYPE":"$SITE_TYPE"
#         }
#     },
#     {
#         "$out":"temp_useable_sites_2"
#     }
#     ])
    # useable_sites_df = DataFrame(list(db.temp_useable_sites_2.find({})))
    # return useable_sites_df

# flag = add_segment_to_sitedata()
# print('''Collection 11 - exl_tag.temp_useable_sites_2 CREATED\n\n''')


def move_to_midb():
    useable_sites = exl_db.temp_useable_sites_1.find()
    temp_list = list(useable_sites)
    mi_db.temp_useable_sites_1.insert_many(temp_list)

flag = move_to_midb()
print('''Collection 11 - exl_tag.temp_useable_sites_1 MOVED to mi_db.temp_useable_sites_1 \n\n''')



# def get_traffic_details(useable_sites_df):
#     for data in useable_sites_df:
#         data['traffic'] = list(midb.site_traffic.find_one({"siteId":"data['SITE_ID']"}))

def get_traffic_details():
    mi_db.temp_useable_sites_1.aggregate([
    {
        "$lookup":
        {
            "from":"site_traffic",
            "localField": "SITE_ID",
            "foreignField": "siteId",
            "as":"trf"
        }
    },
    {
        "$project":{
        "CHAIN_ID":1,
        "CATEGORY_ID":1,
        "ACTIVE_IND":1,
        "SEGMENT_ID":1,
        "SEG_TYPE_ID":1,
        "CATEGORY_NAME":1,
        "SEGMENT_NAME":1,
        "excl_flag":1,
        "chain_seg_flag":1,
        "SITE_ID":1,
        "SITE_NAME":1,
        "CHAIN_ID":1,
        "CHAIN_NAME":1,
        "ADDRESS":1,
        "SITE_TYPE":1,
        "trf.siteID":"$trf.siteId",
        "trf.addresses":"$trf.addresses",
        "FIRST_TRAFFIC_DATE":"$trf.firstTrafficDate",
        "TRAFFIC":"$traffic"
        }
    },
    {
        "$out":"temp_sites_with_traffic"
    }
    ])


# flag = get_traffic_details()
# print('''Collection 12 - mi_db.temp_sites_with_traffic CREATED\n\n''')


# sites_traffic_df = mi_db.temp_sites_with_traffic.find()
# print(sites_traffic_df.head(20))
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
# get evrything in a df and start aggregating as per the sheet




exl_client.close()
mi_client.close()