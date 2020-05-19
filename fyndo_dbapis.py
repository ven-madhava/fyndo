
# coding: utf-8

# # 0. Imports and Settings

# In[1]:


# Necessary imports
# -----------------
import psycopg2 as psql
import numpy as np
import random
import copy
import math
import csv
import collections
from collections import Counter
import time
import sqlalchemy
from sqlalchemy import create_engine
from tqdm import tqdm
from datetime import datetime


# Necessary Flask imports
# -----------------------
from flask import Flask, request, send_file
from flask_restful import Resource, Api, reqparse
from json import dumps
from flask_jsonpify import jsonify
import requests
import json


# gcloud storage related imports
# ------------------------------
import os
from google.cloud import storage



# ## 0. Local / VM set up

# In[2]:


'SWITCH BETWEEN LOCAL AND VM HERE'

# make sure the json files are intact in the remote VM
# ----------------------------------------------------
global vm_or_local
vm_or_local = 'vm'

# ops
# ---
if vm_or_local == 'local':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/venkateshmadhava/Documents/pmate2/pmate2_env/notebooks/ven-ml-project-387fdf3f596f.json"
else:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/venkateshmadhava/fyndo/ven-ml-project-387fdf3f596f.json"


# ## 1. Codes

# In[3]:


def get_api_key():

    # 1. Initialising bucket details
    # ------------------------------
    bucket_name = 'ven-ml-project.appspot.com'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    destination_blob_name = 'admin/secret_key_vm_apis.txt'
    blob = bucket.blob(destination_blob_name)

    # 2. Getting content and processing
    # ---------------------------------
    key = blob.download_as_string().decode()

    return key


# In[4]:


# we now need specific generice functions for various db ops
# API function
# ----------------------------------------------------------
def run_insert_update_delete_query_api_function(query):
    
    '''
    
    1. a single function that would run queryy
    2. the query needs to be constructed at client end
    
    ONLY FOR GENERIC QUERIES LIST INSERT, UPDATE & DELETE
    *****************************************************
    
    returns 'ok' or 'not_ok'
    along with error statement
    
    
    '''
    
    # 0. initialisations
    # ------------------
    sanity_flag = 0
    if 'INSERT' in query or 'DELETE' in query or 'UPDATE' in query:
        sanity_flag = 1
    
    # running query
    # use this to raise exceptions and return status to api calls            
    # -----------------------------------------------------------
    if sanity_flag == 1:
        
        try:

            # going to include run query statements here itself
            # -------------------------------------------------

            # 1. creating connection
            # ----------------------
            
            # 1. creating connection
            # DO NOT CHANGE THESE
            # ----------------------
            host = '127.0.0.1'
            
            # db setups
            # ---------
            if vm_or_local == 'local':                
                con = psql.connect(host = host, database = 'fyndodb')
            else:
                con = psql.connect(host = host, database = 'fyndodb', user = 'fyndodbuser', password='fyndodbuserpass')
            
            
            # creating cursor
            # ---------------
            cur = con.cursor()

            # 2. executing query
            # ------------------
            cur.execute(query)

            # 3. final closures
            # -----------------
            con.commit()
            cur.close()
            con.close()

            # 4. setting stattus
            # ------------------
            query_out = 'ok'
            query_status = 'ok'

        except Exception as e:
            query_out = 'error: ' + str(e)
            query_status = 'not_ok'


        # return status
        # return 'ok' if query executed weel
        # else an error
        # use for api further process
        # -----------------------------------
        out_d = {}
        out_d['output'] = query_out
        out_d['status'] = query_status
        
    
    # incorrect statement
    # ------------------
    else:
        out_d = {}
        out_d['output'] = 'error: wrong statement. this function only supports insert,delete or update.'
        out_d['status'] = 'not_ok'
        
        
    
    return out_d


# In[5]:


## 1.
## API FUNCTION
## MAIN function code to execute a query
## -------------------------------------
class exAPI_run_insert_update_delete_query(Resource):
    
    # overriding post function
    # ------------------------
    def post(self):

        ## Authenticating request
        ## ----------------------
        try:

            # Get stored key
            # --------------
            vm_api_key = get_api_key()

            try:
                
                # retrieveing api key
                # -------------------
                api_key = request.args['api_key']
                
                # checking api correctness
                # ------------------------
                if api_key == vm_api_key:

                    # Authorized request
                    ####################
                    
                    # 1.
                    # Setting up key values to accept
                    # -------------------------------
                    parser = reqparse.RequestParser()
                    parser.add_argument('query')
                    args = parser.parse_args()

                    # 2.
                    # Getting params
                    # --------------
                    query = args['query']
                    
                    # 3.
                    # sanity for internal
                    # -------------------
                    print('API exAPI_run_insert_update_delete_query firing: ' + str(query))
                    
                    
                    try:
                        # Using get task id function
                        # --------------------------
                        d = run_insert_update_delete_query_api_function(query)
                        return jsonify(d)

                    except Exception as e:

                        return "Something went wrong. error: " + str(e), 500

                else:

                    # Incorrect credentials
                    # ---------------------
                    return 'Incorrect credentials', 401
            
            except Exception as e:

                # Invalid headers
                # ---------------
                return 'Invalid credentails. error: ' + str(e), 400

        except Exception as e:

            # Secret key not set in storage
            # -----------------------------
            return 'API keys not initialsed. error: ' + str(e), 401


# In[6]:


# we now need specific generice functions for various db ops
# API function
# ----------------------------------------------------------
def run_select_query_api_function(table_name,query,chunk_size):
    
    '''
    
    1. a single function that would run all select queryies
    2. the query needs to be constructed at client end
    
    ONLY FOR SELECT STATEMENT
    *************************
    
    returns 'ok' or 'not_ok'
    along with error statement or output d
    
    
    '''
    
    # 0. initialisations
    # ------------------
    sanity_flag = 0
    if 'SELECT' in query:
        sanity_flag = 1
    
    
    # 1. if else
    # ----------
    if table_name in query and sanity_flag == 1:
    
        # running query
        # use this to raise exceptions and return status to api calls            
        # -----------------------------------------------------------
        try:

            # going to include run query statements here itself
            # -------------------------------------------------

            # 1. creating connection
            # DO NOT CHANGE THESE
            # ----------------------
            host = '127.0.0.1'
            
            # db setups
            # ---------
            if vm_or_local == 'local':                
                con = psql.connect(host = host, database = 'fyndodb')
            else:
                con = psql.connect(host = host, database = 'fyndodb', user = 'fyndodbuser', password='fyndodbuserpass')
            
            # creating cursor
            # ---------------
            cur = con.cursor()

            # 2. executing query
            # ------------------
            cur.execute(query)
            cols = [desc[0] for desc in cur.description]

            # 3. fetching results
            # -------------------
            if chunk_size == 'all':
                query_out = cur.fetchall()
            else:
                query_out = cur.fetchmany(int(chunk_size))


            # 3. final closures
            # -----------------
            con.commit()
            cur.close()
            con.close()

            # 4. setting stattus
            # ------------------
            query_status = 'ok'

        except Exception as e:
            query_out = 'error: ' + str(e)
            query_status = 'not_ok'


        # return status
        # return 'ok' if query executed weel
        # else an error
        # use for api further process
        # -----------------------------------
        if query_status == 'ok':
            
            # contructing out dict
            # --------------------
            temp_d = {}
            
            # iterting query out --
            # this is list of records
            # -----------------------
            for i in range(len(query_out)):
                
                # ops
                # ---
                temp_d[i] = {}

                # populating d
                # ------------
                for i_in in range(len(query_out[i])):
                    temp_d[i][cols[i_in]] = query_out[i][i_in]
                    
            
            # done and final assignments
            # --------------------------
            out_d = {}
            out_d['output'] = temp_d
            out_d['status'] = 'ok'
            
        else:
            out_d = {}
            out_d['output'] = query_out
            out_d['status'] = query_status

    
    # if table name  not in query
    # ---------------------------
    else:
        
        out_d = {}
        out_d['output'] = 'error: either wrong statement or table name and query are not consistent.'
        out_d['status'] = 'not_ok'
        
    
    # final return
    # ------------
    return out_d


# In[7]:


## 1.
## API FUNCTION
## MAIN function code to execute a query
## -------------------------------------
class exAPI_run_select_query(Resource):
    
    # overriding post function
    # ------------------------
    def post(self):

        ## Authenticating request
        ## ----------------------
        try:

            # Get stored key
            # --------------
            vm_api_key = get_api_key()

            try:
                
                # retrieveing api key
                # -------------------
                api_key = request.args['api_key']
                
                # checking api correctness
                # ------------------------
                if api_key == vm_api_key:

                    # Authorized request
                    ####################
                    
                    # 1.
                    # Setting up key values to accept
                    # -------------------------------
                    parser = reqparse.RequestParser()
                    parser.add_argument('query')
                    parser.add_argument('table_name')
                    parser.add_argument('chunk_size')
                    args = parser.parse_args()

                    # 2.
                    # Getting params
                    # --------------
                    query = args['query']
                    table_name = args['table_name']
                    chunk_size = args['chunk_size']
                    
                    # 3.
                    # sanity for internal
                    # -------------------
                    print('API exAPI_run_select_query firing: ' + str(query))
                    
                    
                    try:
                        # Using get task id function
                        # --------------------------
                        d = run_select_query_api_function(table_name,query,chunk_size)
                        return jsonify(d)

                    except Exception as e:

                        return "Something went wrong. error: " + str(e), 500

                else:

                    # Incorrect credentials
                    # ---------------------
                    return 'Incorrect credentials', 401
            
            except Exception as e:

                # Invalid headers
                # ---------------
                return 'Invalid credentails. error: ' + str(e), 400

        except Exception as e:

            # Secret key not set in storage
            # -----------------------------
            return 'API keys not initialsed. error: ' + str(e) , 401


# ## 2. running apis

# In[ ]:


# necessary set ups
# -----------------
app = Flask(__name__)
api = Api(app)


# In[ ]:


# Adding resource
# ---------------
api.add_resource(exAPI_run_insert_update_delete_query, '/run_insert_update_delete_query') # Route
api.add_resource(exAPI_run_select_query, '/run_select_query') # Route
 


# In[ ]:


# final step to run
# -----------------
global vm_or_local
if __name__ == '__main__':
    if vm_or_local == 'local':
        app.run(port='5002') # For local
    else:
        app.run(host='0.0.0.0', port=8000) # VM


# In[ ]:


### END OF CODE ###
### DELETE FROM HERE ####
#########################
