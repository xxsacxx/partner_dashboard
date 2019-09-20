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
pids = ['5d4421722b3dc03043dead1e']

config = configparser.ConfigParser()
config.read('configmsg.ini')
dsh = config['DASH']
MONGO_HOST = dsh['MONGO_HOST']
MONGO_PORT = int(dsh['MONGO_PORT'])
myclient = pymongo.MongoClient(MONGO_HOST,MONGO_PORT,maxPoolSize=10)
mydb = myclient[(dsh['database_name'])]

dash_usr_tbl=mydb[(dsh['dash_users'])]

@app.route('/payments/', methods=["GET"])
@login_required
def payments_view():
    payments_list = list()
    
    result = mongo.db.Payments.find({}).sort("_id")
    result = result.limit(limit=10)
    for payment in result:
        payment["_id"] = str(payment["_id"])
        payments_list.append(payment)
    return render_template("payments.html", payments=payments_list)


@app.route('/update_payments', methods=["POST"])
@login_required
def update_payments_view():
    form = dict(request.form)
    query = pass_to_kafka(form)
    ##ifs3 also
    # return Response(status=200)
    # response = mongo.db.Payments.update({"tnx":form["tnx_id"]},query)
    # if response["nModified"] >0:
    #     return Response(status=200)
    # else:
    #     return Response(status=400)