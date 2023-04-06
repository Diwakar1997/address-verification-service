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
import sqlalchemy as sa
from sqlalchemy.engine.url import URL

import logging
logg=logging.getLogger("werkzeug")


def warehouse_address_tfidf(app_config):
    cnx = mysql.connector.connect(
                        host=app_config['HAPPY_OFFER_HOST'],
                        port = '3306',
                        user=app_config['HAPPY_OFFER_USERNAME'],
                        passwd=app_config['HAPPY_OFFER_PASSWORD'],
                        database = "happy_offer" )

    query_warehouse = "select distinct id from warehouse order by id"

    df_warehouse = pd.read_sql(query_warehouse, cnx)

    redshift_query = """select distinct o.user_address_id,ua.address as address
    from order_analytics o
    inner join dim_user_address ua on o.user_address_id = ua.id
    inner join dim_order_product_details opd on o.order_id = opd.order_id
    where order_date >= dateadd(month,-6,current_date) 
    and o.status not in ('cancel','discarded')
    and opd.warehouse_id = {0}
    """

    redshift_url = URL.create(
    drivername='redshift+redshift_connector',
    host=app_config['REDSHIFT_HOST'],
    port=5439, 
    database=app_config['REDSHIFT_DATABASE'],
    username=app_config['REDSHIFT_USERNAME'],
    password=app_config['REDSHIFT_PASSWORD']
    )
    redshift_engine = sa.create_engine(redshift_url)
    redshift_conn = redshift_engine.connect()

    i = 0
    try:
        for w_id in df_warehouse['id']:
            try:
                i += 1
                logg.info("update warehouse id "+ str(w_id))
                df = pd.read_sql(redshift_query.format(w_id), redshift_conn)
                if df.shape[0] == 0:
                    continue
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

                #similarity_score.clear_warehouse_tfidf_dict(w_id)
            except Exception as e:
                logg.info(e)
    except Exception as e:
        logg.info(e)