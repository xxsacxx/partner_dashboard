from test_producer import pass_to_kafka
import numpy as np
import pandas as pd




data ={'NUMBER': '20190917185258','STATUS': 'paid'}
                
# df = pd.DataFrame.from_dict(data)
print(data)

def passdf(df):
    try:
        pass_to_kafka(df)
    except Exception:
        print("Message sent")

passdf(data)
# for i in range(5):
#     passdf(data)