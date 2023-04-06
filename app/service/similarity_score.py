import boto3
import pandas as pd
import numpy as np
from datetime import datetime
import os
import gc
import re
import json
import mysql.connector
import warnings
warnings.filterwarnings("ignore")
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import urllib
import pickle
import bson

warehouse_tfidf = {}


def compute_siilarity_score(app_config,address_id,warehouse_id,address):
    global warehouse_tfidf
    s3 = boto3.resource('s3')
    BUCKET = app_config['S3_BUCKET_ANALYTICS']

    tf = None
    matrix = None
    user_addresses = None
    last_used = None
    last_updated = None

    if warehouse_id in warehouse_tfidf:
        warehouse_dict =  warehouse_tfidf.get(warehouse_id)
        tf = warehouse_dict.get("tf_idf")
        matrix = warehouse_dict.get("tf_idf_matrix")
        user_addresses = warehouse_dict.get("user_address")
        last_used = int(round(datetime.now().timestamp()))
        last_updated_cur = warehouse_dict.get("last_updated")
        if diff(last_used,last_updated_cur) < 86400:
            warehouse_tfidf.update({warehouse_id:{"tf_idf":tf,"tf_idf_matrix":matrix,"user_address":user_addresses,"last_used":last_used,"last_updated":last_updated_cur}})
        else:
            key = 'warehouse-tfidf/' + str(int(warehouse_id)) + '/'
            tf = pickle.loads(s3.Object(BUCKET,key+"tf-idf").get()['Body'].read())
            matrix = pickle.loads(s3.Object(BUCKET,key+"tf-idf-matrix").get()['Body'].read())
            user_addresses = pickle.loads(s3.Object(BUCKET,key+"user-address-ids").get()['Body'].read())
            last_used = int(round(datetime.now().timestamp()))
            last_updated = int(round(datetime.now().timestamp()))
            warehouse_tfidf.update({warehouse_id:{"tf_idf":tf,"tf_idf_matrix":matrix,"user_address":user_addresses,"last_used":last_used,"last_updated":last_updated}})

    else:
        if(len(warehouse_tfidf) >= 200):
            remove_lru()
        key = 'warehouse-tfidf/' + str(int(warehouse_id)) + '/'
        tf = pickle.loads(s3.Object(BUCKET,key+"tf-idf").get()['Body'].read())
        matrix = pickle.loads(s3.Object(BUCKET,key+"tf-idf-matrix").get()['Body'].read())
        user_addresses = pickle.loads(s3.Object(BUCKET,key+"user-address-ids").get()['Body'].read())
        last_used = int(round(datetime.now().timestamp()))
        last_updated = int(round(datetime.now().timestamp()))
        warehouse_tfidf.update({warehouse_id:{"tf_idf":tf,"tf_idf_matrix":matrix,"user_address":user_addresses,"last_used":last_used,"last_updated":last_updated}})

    tf_mat_add = tf.transform([address])
    cosine_sim = cosine_similarity(tf_mat_add,matrix).flatten()
    similarAddressIdx = cosine_sim[cosine_sim < 1.0].argsort()[::-1][:1]

    return {'user_address_id' : address_id, 'similar_user_id': user_addresses[similarAddressIdx[0]],'similarity_score':cosine_sim[similarAddressIdx][0]}
def diff(end_time,start_time):
    return end_time-start_time
def remove_lru():
    global warehouse_tfidf
    last_used = int(round(datetime.now().timestamp()))
    delete_key = None
    for key in warehouse_tfidf.keys():
        ls = warehouse_tfidf.get(key).get("last_used")
        if ls < last_used:
            delete_key = key
            last_used = ls
    if delete_key != None:
        warehouse_tfidf.pop(delete_key)


def clear_warehouse_tfidf_dict(warehouse_id):
    global warehouse_tfidf
    warehouse_tfidf.pop(warehouse_id)

