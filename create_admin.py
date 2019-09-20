from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash
# from config import engine
import pandas as pd
import bson
from bson.objectid import ObjectId
import pymongo
#from data import (df, st_active, st_applied, st_verif_fail, st_verif_success)
import configparser
import logging
import sys

config = configparser.ConfigParser()
config.read('configmsg.ini')
dsh = config['DASH']
MONGO_HOST = dsh['MONGO_HOST']
MONGO_PORT = int(dsh['MONGO_PORT'])
myclient = pymongo.MongoClient(MONGO_HOST,MONGO_PORT,maxPoolSize=10)
mydb = myclient[(dsh['database_name'])]





def create_user_table():
    User.metadata.create_all(engine)


def add_user(password_ip, email_ip, phone_ip, partner_id_ip):
    hashed_password = generate_password_hash(password_ip, method='sha256')
    mydb[(dsh['dash_users'])].save( { 'password': hashed_password, 'email': email_ip ,'phone':phone_ip ,'partner_id':ObjectId(partner_id_ip)  } )



def del_user(email_ip):
    
    mydb[(dsh['dash_users'])].remove( { 'email': email_ip } )



def show_users():
    documents=(mydb[(dsh['dash_users'])].find() )
    for document in documents:
          print(document)
    
