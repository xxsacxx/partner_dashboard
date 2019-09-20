import io

from app import bcrypt
from flask import render_template, redirect, url_for, request, Response, send_file
from flask_login import login_required, login_user, logout_user

from app import app, mongo
from app import login_manager
from app.forms import LoginForm
from app.models import User
import csv
import pymongo
from werkzeug.security import check_password_hash
from app.utils import upload_data, create_csv, get_filters, build_query
import configparser
from bson.objectid import ObjectId
import pandas as pd
import numpy as np
from app.db import sub_partner_emp , create_csv,show_subs
import traceback
pid = ['5d4421722b3dc03043dead1e']
#pid = '5d4421722b3dc03043dead1e'

config = configparser.ConfigParser()
config.read('configmsg.ini')
dsh = config['DASH']
MONGO_HOST = dsh['MONGO_HOST']
MONGO_PORT = int(dsh['MONGO_PORT'])
myclient = pymongo.MongoClient(MONGO_HOST,MONGO_PORT,maxPoolSize=10)
mydb = myclient[(dsh['database_name'])]

dash_usr_tbl=mydb[(dsh['dash_users'])]

         
dff = pd.DataFrame()
applied_filters=[]

def get_filtered(app_filters,ddf):
    ddf = (ddf[ddf[list(app_filters.keys())].isin(app_filters).all(1)])
    return(ddf)


@app.route('/employee/<int:page>', methods=["GET"])
@login_required
def employee_view(page):
    global dff
    global applied_filters
    global filters
    print(page)
    

    dfss = []
    filters = []
    pids = []
    
    global query_string 
    if(page ==1):

        
        try:
            dfss.append(sub_partner_emp(pid))
            partners_tbl=mydb['partners'].find({'parent_partner_id': ObjectId('5d4421722b3dc03043dead1e')})
            part = pd.DataFrame(list(partners_tbl))
            pids.append((part['_id'].astype(str)).tolist()) 
            dfss.append(sub_partner_emp(pids[0]))
            dfss = (dfss[0]+dfss[1])
            dff = pd.concat(dfss, ignore_index=True)
        except Exception:
            print(traceback.format_exc())

        filters = get_filters()
        _, applied_filters = build_query(request.args)
        app_filters = applied_filters
        app_filters = {k: v for k, v in app_filters.items() if v}

        print(app_filters)
        if(app_filters):
            dff = get_filtered(app_filters,dff)
            print(app_filters)
            
        
        query_string = request.query_string.decode("utf-8")
        if query_string != "":
            query_string = "?" + query_string
        items_per_page = 15
        skip=items_per_page * (page - 1)
        limit=items_per_page
        employees = dff.iloc[skip:(skip+15)]
        employee_list = list()
        for i, employee in employees.iterrows():
            employee_list.append(dict(employee))
    else:
        _, applied_filters = build_query(request.args)
        app_filters = applied_filters
        app_filters = {k: v for k, v in app_filters.items() if v}

        
        if(app_filters):
            dff = get_filtered(app_filters,dff)
            
        query_string = request.query_string.decode("utf-8")
        if query_string != "":
            query_string = "?" + query_string
        
        items_per_page = 15
        skip=items_per_page * (page - 1)
        limit=items_per_page
        employees = dff.iloc[skip:(skip+15)]
        employee_list = list()
        for i, employee in employees.iterrows():
            employee_list.append(dict(employee))

   
    # applied_filters

    
    
    

    
    # return render_template("employees.html", emps=employee_list, page=page)

    return render_template("employees.html", emps=employee_list, page=page, filters=filters,
app_filters=applied_filters, query_str='')

@app.route('/employee/upload', methods=["POST"])
@login_required
def upload_employee_data():
    file = request.files["csv"]
    print(file)
    stream = io.StringIO(file.read().decode("UTF8"), newline=None)
    csvreader = csv.reader(stream)
    csvreader = list(csvreader)[1:]
   
    for row in csvreader:
        print(row)
    try:
        is_uploaded = upload_data(csvreader)
        if is_uploaded:
            return Response(status=200, )
        else:
            return Response(status=400)
    except:
        return Response(status=400)


@app.route('/employee/download', methods=["GET"])
@login_required
def download_csv():
    file_path, file_name , _ = create_csv()
    return send_file(file_path, attachment_filename=file_name, as_attachment=True)



@app.route('/employee/change_emp_status', methods=["PUT"])
@login_required
def change_emp_status():
    response = request
    result = mongo.db.Employee.find_one_and_update({"_id": ObjectId(response.form["emp_id"])},
                                                   {"$set": {"emp": response.form["value"]}})
    if result:
        return Response(status=200)
    else:
        return Response(status=500)