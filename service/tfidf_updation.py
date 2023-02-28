import boto3
import pandas as pd
import numpy as np
import time
import os
import gc
import re
from datetime import datetime
from pytz import timezone
import json
import mysql.connector
import warnings
warnings.filterwarnings("ignore")
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import urllib
import pickle
import bson
from service import similarity_score

def warehouse_address_tfidf(app_config):
    cnx = mysql.connector.connect(
                        host=app_config['HAPPY_OFFER_HOST'],
                        port = '3306',
                        user=app_config['HAPPY_OFFER_USERNAME'],
                        passwd=app_config['HAPPY_OFFER_PASSWORD'],
                        database = "happy_offer" )

    query_warehouse = "select distinct id from warehouse"

    df_warehouse = pd.read_sql(query_warehouse, cnx)

    query = """select o.user_address_id,ua.address as address
    from `order` o
    inner join user_address ua on o.user_address_id = ua.id
    inner join order_product_details opd on o.id = opd.order_id
    where order_date >= current_date - interval 6 month
    and o.status not in ('cancel','discarded')
    and opd.warehouse_id = {0}
    """

    i = 0
    for w_id in df_warehouse['id']:
        i += 1
        df = pd.read_sql(query.format(w_id), cnx)
        if df.shape[0] == 0:
            continue
        print(str(i) + " " + str(w_id))
        address_list=list(df['address'])
        user_address_list = list(df['user_address_id'])
        vectorizer = TfidfVectorizer(stop_words="english", ngram_range=(1,3), token_pattern=r'[A-Za-z]{2,}')
        tf_idf = vectorizer.fit(address_list)
        tf_idf_matrix = tf_idf.transform(address_list)

        s3 = boto3.resource('s3')
        BUCKET = app_config['S3_BUCKET_ANALYTICS']
        key = 'warehouse-tfidf/' + str(w_id) + '/'

        s3.Object(BUCKET,key+"tf-idf").put(Body=pickle.dumps(tf_idf))
        s3.Object(BUCKET,key+"tf-idf-matrix").put(Body=bson.binary.Binary( pickle.dumps(tf_idf_matrix, protocol=2)))
        s3.Object(BUCKET,key+"user-address-ids").put(Body=pickle.dumps(user_address_list))

        similarity_score.clear_warehouse_tfidf_dict(w_id)
