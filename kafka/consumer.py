from setup import topic
import json
import ast
from pykafka.common import OffsetType
import sys, os
sys.path.append(os.path.abspath(os.path.join('..')))
import configparser
import traceback
import pymongo
pid = ['5d4421722b3dc03043dead1e']


config = configparser.ConfigParser()
config.read('configmsg.ini')
dsh = config['DASH']
MONGO_HOST = dsh['MONGO_HOST']
MONGO_PORT = int(dsh['MONGO_PORT'])
myclient = pymongo.MongoClient(MONGO_HOST,MONGO_PORT,maxPoolSize=10)
mydb = myclient[(dsh['database_name'])]

dash_usr_tbl=mydb[(dsh['dash_users'])]

# from app.db import push_subs_reconcile
# from comm
# import pykafka.common.OffsetType
def push_subs_reconcile(number,status):
    subs_inv = mydb[('partner_subscription_invoice')].update_one({'number':number},{'$set':{'status':status}})
    

consumer = topic.get_simple_consumer(
    auto_offset_reset=OffsetType.LATEST,
    reset_offset_on_start=True
)
for m in consumer:
    if m is not None:
        # print(type(str(m.value)))
        
        msg = ast.literal_eval(m.value.decode('utf-8'))
        # print(type(dict(msg)))
        print(msg.keys())
        number = msg['NUMBER']
        status = msg['STATUS']
        print(number,status)
        push_subs_reconcile(number,status)

        
    #    print(msg['NUMBER'])


        
