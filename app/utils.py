from app import mongo
from config import BASE_DIR
import os
import datetime
import csv
import io
import pymongo
import configparser
from bson.objectid import ObjectId
import pandas as pd
from push2s3.push2s3 import upload_doc
from app.db  import sub_partner_emp , create_csv,show_subs

import traceback

config = configparser.ConfigParser()
config.read('configmsg.ini')
dsh = config['DASH']
MONGO_HOST = dsh['MONGO_HOST']
MONGO_PORT = int(dsh['MONGO_PORT'])
myclient = pymongo.MongoClient(MONGO_HOST,MONGO_PORT,maxPoolSize=10)
mydb = myclient[(dsh['database_name'])]

dash_usr_tbl=mydb[(dsh['dash_users'])]





def upload_data(csvreader):
    updf = pd.concat([pd.Series(x) for x in csvreader], axis=1)
    updf = updf.T
    updf.columns = ['address_proof_type', 'age', 'currently_employed',
        'department', 'doj', 'earnings', 'emp_id', 'gender',
        'highest_qualification', 'highest_qualifocation', 'id_proof_type',
        'name', 'pan_no', 'payout_date', 'registered_phone_number',
        'total_years_of_experience', 'work_email', 'work_location',
        'work_rating']
    _ , _ , df = create_csv()  
    if(data_verif(df,updf)):
        response = upload_doc(updf,'employee_details') 
    return response.acknowledged


def data_verif(df,updf):
    orgdf = df.iloc[0:0]

    column_chk = (set(updf.columns) == set(orgdf.columns))



    if column_chk == True and len(updf) != 0 :

        return True 
        print('correct')
    else:
        return False
        print('wrong')



def get_filters():
    # return(None)
    pids = ['5d4421722b3dc03043dead1e']
    dfsss =[]
    emps = pd.DataFrame()
    try:
        dfss = []
        dfss.append(sub_partner_emp(pids))
        partners_tbl=mydb['partners'].find({'parent_partner_id': ObjectId('5d4421722b3dc03043dead1e')})
        part = pd.DataFrame(list(partners_tbl))
        pids.append((part['_id'].astype(str)).tolist()) 
        dfss.append(sub_partner_emp(pids[1]))
        dfss = (dfss[0]+dfss[1])
        emps = pd.concat(dfss, ignore_index=True)

    except Exception:
        print(traceback.format_exc())
    # emps = sub_partner_emp(pids)
    # print(emps)
    
    cities = emps['Curr_City'].unique()
    depts = emps['Department'].unique()
    vendors = emps['Vendor'].unique()
    return {
        "Curr_City": cities,
        "Department": depts,
        "Vendor": vendors
    }

def pmt_upd_query(form):
    options = ["type", "receipt_number"]
    query_dict = {}
    for opt in options:
        if form[opt] != "Select option":
            query_dict[opt] = form[opt]
    query_dict["value"] = form["amount"]
    return {"$set": query_dict}




def build_query(arguments):
    query = {}
    
    applied_filters = {"Curr_City": [], "Vendor": [], "Department": []}
    if len(arguments) > 0:
        keys = list(arguments.keys())
        
        for key in keys:
            query[key] = {arguments.get(key)}
            applied_filters[key] = arguments.getlist(key)
        print(query,applied_filters)

    return query, applied_filters