import grpc

# import document_upload_pb2
# import document_upload_pb2_grpc
import logging

import pandas as pd
import numpy as np
import onionclient
#from onionlife_client_python import onionclient
from onionclient import document_upload_pb2
#from onionclient import UploadDocuments
#np.fromstring(s, rec.dtype)

# emp_df=(pd.read_csv('../data/user_employemnts.csv')).to_csv(None).encode()
# #dd=dd.to_csv(None).encode()
# #emp_df=np.fromstring((((pd.read_csv('../data/user_employemnts.csv')).to_records(index=False)).tostring()), dd.dtype)

# subs_df=(pd.read_csv('../data/loanApplications.csv')).to_csv(None).encode()
# #subs_df=(((pd.read_csv('../data/loanApplications.csv')).to_records(index=False)).tostring())

# repay_df=(pd.read_csv('../data/loanApplications.csv')).to_csv(None).encode()
# #repay_df=(((pd.read_csv('../data/loanApplications.csv')).to_records(index=False)).tostring())


def upload_doc(doc,type3):
    client = onionclient.OnionLife(address='api.develop.onionlife.in:443', app_key='spark-consumer-key', app_secret='dfdf89475n87843&&^3410',plain_text=False)
    doc = doc.to_csv(None).encode()
    
    docUploadRequest = document_upload_pb2.DocUploadRequest(
        content = doc,
        partner_id="5d4421722b3dc03043dead1e",
        doc_for=type3
    )
    client.UploadDocuments(iter([docUploadRequest]))





# def run():
#     # NOTE(gRPC Python Team): .close() is possible on a channel and should be
#     # used in circumstances in which the with statement does not fit the needs
#     # of the code.
#     print('pushing employee_details')
#     #print(emp_df)
#     print('h')
#     #print(emp_df.to_records(index=False).tostring())
#     print(upload_doc(emp_df,'employee_details'))
#     #print(emp_df)
#     print('pushing subs_details')
#     upload_doc(subs_df,'subs_details')
#     print('pushing repayments_details')
#     upload_doc(repay_df,'repay_details')

# if __name__ == '__main__':
#     logging.basicConfig()
#     run()