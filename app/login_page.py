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




@login_manager.user_loader
def user_loader(user_id):
    user_mon = dash_usr_tbl.find_one({"email": user_id})
    if not user_mon:
        return
    user = User(email=user_mon["email"], password=user_mon["password"])
    return user


@app.route("/", methods=["GET"])
def reroute_view():
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    """For GET requests, display the login form.
    For POSTS, login the current user by processing the form.
    dash_usr_tbl.find_one({"email": input1})
    """
    form = LoginForm()
    if form.validate_on_submit():
        user_mon = dash_usr_tbl.find_one({"email": form.email.data})
        if user_mon:
            if check_password_hash(user_mon["password"], form.password.data):
                user_obj=User(user_mon['email'],user_mon["password"])
                login_user(user_obj,remember=True)

                return redirect(url_for("dashboard_view"))
    return render_template("login.html", form=form)


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))








