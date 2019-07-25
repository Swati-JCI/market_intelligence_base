# creating branch and testing pull request
import requests
import json
import pandas as pd
import datetime as dt
report = pd.read_csv("C:/Users/Monish128025/Downloads/Shoppertrak/FULL AUTOMATION/Report Formats/LLBean.csv")
breakdown = len(report)
date = dt.datetime.now().date()
final_rep = pd.DataFrame()
for i in range(breakdown):
    # Assign columns to each variable
    data_pt = report.iloc[i]
    date_e = date - dt.timedelta(days = 1) #Change delta change to column 0 of sheet 
    date_end = str(date_e)+"T23:59:59Z"
    cat_name = data_pt['Segment/Category']
    cat_uuid = data_pt['Cat_Uuid']
    if(data_pt['Cat_Child'] != '0') :   
        cat_child = list(data_pt['Cat_Child'].split(","))
    else:
        cat_child = []
    geo_name = data_pt['GeographyName']
    geo_uuid = data_pt['Uuid']
    geo_type = data_pt['GeographyLevel']
    geo_parent = data_pt['ParentUuid']
    if(data_pt['ChildrenUuid']!='0'):
        geo_child = list(data_pt['ChildrenUuid'].split(","))
    else:
        geo_child = []
    Period = data_pt['PeriodType']
    
    #Write code for Date start selection based on WTD, YTD, QTD:
    if(Period == "WTD"):
        date_st = date_e - dt.timedelta(days = 6)
        date_start = str(date_st) + "T00:00:00Z"
    elif(Period == "YTD"):
        date_start = "2019-02-25T00:00:00Z"

    request = {
            "dateStart": date_start,
            "dateEnd": date_end,
            "subscriptions": [
                    {
                            "category": {
                                    "name": cat_name,
                                    "childrenUuids": cat_child,
                                    "uuid": cat_uuid
                                                      },
                                    "geography": {                                              #(the object is taken from GET /geographies)
                                            "uuid": geo_uuid,
                                            "name": geo_name,
                                            "geoType": geo_type,
                                            "parentUuid": geo_parent,
                                            "childrenUuids": geo_child
                                            }
                                    }
                                    ],
                            "indexCalculatorHints":{
                                            "outputSiteIds": True,
                                            },
                                    }
                            
    header = {"Content-Type" : 'application/json'}
    response = requests.post("https://marketintelligence.shoppertrak.com/index", data = json.dumps(request), headers = header)
    #print("--------------------------------------------------------------------------")
# Print the status code of the response.
    #print(response.status_code)
    data = response.json()
    final_rep = final_rep.append({"Geography Level": geo_type,"Geography Name" : geo_name, "Segment" : cat_name, "Period Type": Period,"Period Start" : date_start, "Period End" : date_e, "%YoY" : data[0]['value']}, ignore_index = True)
print(final_rep)
final_rep.to_csv("C:/Users/Monish128025/Downloads/Shoppertrak/FULL AUTOMATION/Final Report/LLBean_"+str(date_e)+".csv")
