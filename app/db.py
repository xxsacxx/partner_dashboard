import configparser
from bson.objectid import ObjectId
import pandas as pd
import numpy as np
import csv
import pymongo
import io
from push2s3.push2s3 import upload_doc


pids = ['5d4421722b3dc03043dead1e']
pid = '5d4421722b3dc03043dead1e'

# try:
    
#     dfs.append(sub_partner_emp(pids))
#     partners_tbl=mydb['partners'].find({'parent_partner_id': ObjectId('5d4421722b3dc03043dead1e')})
#     part = pd.DataFrame(list(partners_tbl))
#     pids.append((part['_id'].astype(str)).tolist())
#     dfs.append(sub_partner_emp(pids[1]))
# except Exception:
#     print(traceback.format_exc())
# finally:
#     finalqueryresult = pd.concat(dfs, ignore_index=True)
#     print(finalqueryresult)

config = configparser.ConfigParser()
config.read('configmsg.ini')
dsh = config['DASH']
MONGO_HOST = dsh['MONGO_HOST']
MONGO_PORT = int(dsh['MONGO_PORT'])
myclient = pymongo.MongoClient(MONGO_HOST,MONGO_PORT,maxPoolSize=10)
mydb = myclient[(dsh['database_name'])]

dash_usr_tbl=mydb[(dsh['dash_users'])]



def sub_partner_emp(pids):
    dfs=[]
    for pid in pids:

        part_emp_tbl=mydb['partner_employees'].find({'partner_id': ObjectId(pid)})
        loanapp_tbl=mydb['loan_applications'].find({'partner_id': ObjectId(pid)})
        user_emp_tbl=mydb['user_employments'].find({'partner_id': ObjectId(pid)})





        part_emp = pd.DataFrame(list(part_emp_tbl))
        loanapp = pd.DataFrame(list(loanapp_tbl))
        user_emp = pd.DataFrame(list(user_emp_tbl))

        part_emp['partner_id'] = part_emp['partner_id'].astype(str)
        user_emp['partner_id'] = user_emp['partner_id'].astype(str)
        loanapp['partner_id'] = loanapp['partner_id'].astype(str)
        loanapp['user_id'] = loanapp['user_id'].astype(str)

        user_emp = (user_emp[['employee_id','user_id','current','employer_name']])
        part_emp = (part_emp[['emp_id','currently_employed','department','name','registered_phone_number','work_location']])
        part_emp= part_emp.rename(columns = {'emp_id':'employee_id'})
        loanapp = loanapp[['user_id','status']]


        df = (pd.merge(user_emp, part_emp, on='employee_id'))

        df['user_id'] = df['user_id'].astype(str)
        df = (pd.merge(loanapp, df, on='user_id'))
        df = df[['name','employee_id','work_location','department','status','currently_employed','registered_phone_number','employer_name']]

        df.columns = ['Name','Emp_id','Curr_City','Department','Status','Currently_Employed','Phone_No','Vendor']
        df['Status'] = df['Status'].map({'CREDIT_DECISION_SUCCESS': 'Active','APPLIED': 'Applied'})

        df['Currently_Employed'] = df['Currently_Employed'].map({True: 'Working',False: 'Not Working'})
        dfs.append(df)
    return(dfs)



def create_csv():
########################

########################
    #part_emp_tbl=mydb['partner_employees']
    part_emp_tbl = mydb.partner_employees
    new_records_tbl = mydb.partner_reconcile_status 
    #new_records_tbl=mydb['partner_reconcile_status']

    part_emp = pd.DataFrame(list(part_emp_tbl.find()))
    new_records = pd.DataFrame(list(new_records_tbl.find()))

    """changing download data"""


    part_emp = pd.DataFrame(list(part_emp_tbl.find()))
    part_emp['partner_id']= part_emp['partner_id'].astype(str)
    part_emp = part_emp[part_emp['partner_id'] == pid]
    new_records = pd.DataFrame(list(new_records_tbl.find()))
    new_records['partner_id']= new_records['partner_id'].astype(str)
    new_records = new_records[new_records['partner_id'] == pid]  

    """changing download data"""
    part_emp=part_emp.drop(['_id','partner_id','created_at','updated_at'],axis =1)

    bool_cols = part_emp.columns[part_emp.dtypes == 'bool']
    part_emp[bool_cols] = part_emp[bool_cols].replace({True: 'Y', False: 'N'})
    part_emp['doj']=(part_emp['doj']).dt.date
    part_emp['payout_date']=(part_emp['payout_date'])
    part_emp['registered_phone_number'] = part_emp['registered_phone_number'].str[2:]
    if 'unnamed:0' in new_records.columns:
        new_records.drop(['unnamed:0'],axis =1,inplace=True)
    #part_emp.to_csv('/home/showrya/testinh.csv',index=False)


    #df.to_csv('empo.csv')
    #filename = "em po.csv"
    #filename = "employee-{}.csv".format(str(datetime))
    
    part_emp.to_csv('emps.csv',index=False)
    #employees = mydb.partner_employees
    #filename = "employee-{}.csv".format(str(datetime.date.today()))
    #file_path = os.path.join(BASE_DIR,"app","static",filename)
    file_name = 'emps.csv'
    file_path = "/home/showrya/Downloads/dash_final_19/dash_flask-dash_final_19/emps.csv"
    return file_path,file_name,part_emp



    with open(file_path,'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        #csvwriter.writerow(["name","emp_id","city","dept","vendor","status","emp"])
        csvwriter.writerow(['address_proof_type', 'age', 'currently_employed', 'department', 'doj',
       'earnings', 'emp_id', 'gender', 'highest_qualification',
       'highest_qualifocation', 'id_proof_type', 'name', 'pan_no',
       'payout_date', 'registered_phone_number', 'total_years_of_experience',
       'work_email', 'work_location', 'work_rating'])
        for emp in employees:
            li = [emp['address_proof_type'], emp['age'], emp['currently_employed'], emp['department'], emp['doj'],
            emp['earnings'], emp['emp_id'], emp['gender'], emp['highest_qualification'],
            emp['highest_qualifocation'], emp['id_proof_type'], emp['name'], emp['pan_no'],
            emp['payout_date'], emp['registered_phone_number'], emp['total_years_of_experience'],
            emp['work_email'], emp['work_location'], emp['work_rating']]
            #li = [emp["name"] , emp["emp_id"] , emp["city"] , emp["dept"], emp["vendor"] , emp["status"] , emp["emp"]]
            csvwriter.writerow(li)
    return file_path,filename



def show_subs():
    subs_invoice = mydb[('partner_subscription_invoice')].find({"partner_id": ObjectId(pid)})
    subs_invoicedf = pd.DataFrame(list(subs_invoice))
    print(subs_invoicedf)
    return(subs_invoicedf)


def show_reps():
    subs_invoice = mydb[('partner_subscription_invoice')].find({"partner_id": ObjectId(pid)})
    subs_invoicedf = pd.DataFrame(list(subs_invoice))
    print(subs_invoicedf)
    return(subs_invoicedf)


def push_subs_reconcile(number,status):
    subs_inv = mydb[('partner_subscription_invoice')].update_one({'number':number},{'$set':{'status':status}})
    print('hiii')
#     db.ProductData.update_one({
#   '_id': p['_id']
# },{
#   '$inc': {
#     'd.a': 1
#   }
# }







