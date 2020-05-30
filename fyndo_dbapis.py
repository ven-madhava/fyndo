
# coding: utf-8

# # 0. Imports and Settings

# In[1]:


# Necessary imports
# -----------------
import psycopg2 as psql
#import numpy as np
import random
import copy
import math
import csv
import collections
from collections import Counter
import time
from tqdm import tqdm
from datetime import datetime
from difflib import SequenceMatcher
from itertools import product
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool



# Necessary Flask imports
# -----------------------
from flask import Flask, request, send_file
from flask_restful import Resource, Api, reqparse
from json import dumps
from flask_jsonpify import jsonify
import requests
import json


# remembering commands
# 1. cd Documents/pmate2/pmate2_env/notebooks/fyndo
# 2. jupyter nbconvert --to script fyndo_dbapis.ipynb
# 3. clean up py file
# 4. 


# ## 0. Local / VM set up

# In[2]:


'SWITCH BETWEEN LOCAL AND VM HERE'

# make sure the json files are intact in the remote VM
# ----------------------------------------------------
global vm_or_local
vm_or_local = 'vm'

# setting api text file path
# --------------------------
global api_file_path
if vm_or_local == 'local':
    api_file_path = '/Users/venkateshmadhava/Documents/fyndo_20/fyndo_api_key.txt'
else:
    api_file_path = '/home/venkateshmadhava/files/fyndo_api_key.txt'
    


# In[3]:


# this is required for search
# ---------------------------

global stopwords
stopwords = ['all', 'just', 'being', 'over', 'both', 'through', 'yourselves', 'its', 'before', 'herself', 'had', 'should', 'to', 'only', 'under', 'ours', 'has', 'do', 'them', 'his', 'very', 'they', 'not', 'during', 'now', 'him', 'nor', 'did', 'this', 'she', 'each', 'further', 'where', 'few', 'because', 'doing', 'some', 'are', 'our', 'ourselves', 'out', 'what', 'for', 'while', 'does', 'above', 'between', 't', 'be', 'we', 'who', 'were', 'here', 'hers', 'by', 'on', 'about', 'of', 'against', 's', 'or', 'own', 'into', 'yourself', 'down', 'your', 'from', 'her', 'their', 'there', 'been', 'whom', 'too', 'themselves', 'was', 'until', 'more', 'himself', 'that', 'but', 'don', 'with', 'than', 'those', 'he', 'me', 'myself', 'these', 'up', 'will', 'below', 'can', 'theirs', 'my', 'and', 'then', 'is', 'am', 'it', 'an', 'as', 'itself', 'at', 'have', 'in', 'any', 'if', 'again', 'no', 'when', 'same', 'how', 'other', 'which', 'you', 'after', 'most', 'such', 'why', 'a', 'off', 'i', 'yours', 'so', 'the', 'having', 'once']


# ## 1. Codes

# In[4]:


# getting api key form local
# -------------------------
def get_api_key():
    
    
    # getting key from local file
    # ---------------------------
    f = open(api_file_path, 'r')
    key = f.read()

    return key


# In[5]:


# simle function to argsort list
# ------------------------------
def argsort(seq):
    
    # http://stackoverflow.com/questions/3071415/efficient-method-to-calculate-the-rank-vector-of-a-list-in-python
    return sorted(range(len(seq)), key=seq.__getitem__)


# In[6]:


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


# In[7]:


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


# In[8]:


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


# In[9]:


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


# In[10]:


def return_search_score(input_q_list):
    
    '''
    
    1. we need 2 metrics - order & word overlap match
    2. we will use simple search score to return
    
    FOR NOW NOT WORRYING ABOUT THE ORDER OF SEARCH QUERY
    
    '''
    
    # 0. initis & set ups
    # -------------------
    assert 'list' in str(type(input_q_list)).lower(),'Error: input here is not a list.'
    q_in = input_q_list[0] # is a word -- string
    srch_str_in = input_q_list[1] # is a statement -- string
    
    
    global stopwords
    srch_str = srch_str_in.lower().split(' ')
    q = q_in.lower().split(' ')
    d = {}
    counter = 0
    
    # 1. looping thru each word in q
    # ------------------------------
    for each_q_word in q:
        
        # initialising dict
        # -----------------
        d[counter] = {}
        d[counter]['word'] = each_q_word
        d[counter]['seq'] = []
        
            
        # main iter
        # ---------
        for each_s_word in srch_str:
            
            # 1.1 sim check - processing only if not stopword
            # -----------------------------------------------
            if each_s_word not in stopwords and each_q_word not in stopwords:
                
                # sequence matching
                # -----------------
                match = SequenceMatcher(None, each_s_word, each_q_word).find_longest_match(0, len(each_s_word), 0, len(each_q_word))
                d[counter]['seq'].append(match.size/max(len(each_s_word),len(each_q_word)))
                
            else:

                # just appending 0
                # ----------------
                d[counter]['seq'].append(0.0)
        
        
        # sanity counter incremenet
        # -------------------------
        counter += 1
    
    
    # building final score -- which will be an avg
    # --------------------------------------------
    temp_s = 0
    for keys in d:
        temp_s += sum(d[keys]['seq'])
    
    
    # 2. final return
    # ----------------
    return temp_s
            


# In[17]:


# the main function has to have 
# -----------------------------

# 1. raw query returned by client
# 2. the fields to concat in search
# 3. must be a pooled function


def run_search_query_api_function(table_name,query,chunk_size,feild_names,search_query):
    
    '''
    1. the input to these functions are what the api function will  in its args
    2. this must return a jsonable dict with same feilds - output, status
    3. must be a pooled function
    
    ''' 
    
    # 0. inits
    # --------
    out_dict = {}
    feild_names = feild_names.split(',')
    
    
    # 1. calling generic query api
    # the output here is a dict with status and output as keys
    # ---------------------------------------------------------
    raw_results_out = run_select_query_api_function(table_name,query,chunk_size)
    
    
    # 2. proceeding further based on raw_results status
    # -------------------------------------------------
    try:
        if raw_results_out['status'] == 'ok' and len(raw_results_out['output'].keys()) > 0:
            
            # ok to proceed as there are reults to parse
            # 2.1 local inits
            # ----------------
            new_sorted_d = {}
            raw_results = raw_results_out['output']
            
            
            # 2.1 building args
            # -----------------
            args = [[search_query, ' '.join([raw_results[each_ind][each_f] for each_f in feild_names])] for each_ind in range(len(raw_results))]
            
            
            # 2.2 calling pooling function
            # ----------------------------
            pool = ThreadPool(10)

            # starmap accepts a sequence of argument tuples
            # ---------------------------------------------
            results = pool.starmap(return_search_score, product(args, repeat = 1))
            
            # closing pools
            # -------------
            pool.terminate()
            pool.join()
            
            # 3. sorting results based on highest score at top
            # ------------------------------------------------
            argsort_results = list(reversed(argsort(results)))
            
            # 3.3 looping & assigning
            # will use looping here
            # -----------------------
            for i in range(len(argsort_results)):
                new_sorted_d[i] = raw_results[argsort_results[i]]
                
                
            
            # 4. done and final output assignment
            # -----------------------------------
            out_dict['output'] = new_sorted_d
            out_dict['status'] = 'ok'
            
        
    except:
        
        # something was not right / or no results to parse
        # -------------------------------------------------
        out_dict['output'] = 'no results'
        out_dict['status'] = 'not_ok'
        
        
        
    
    # final return
    # ------------
    return out_dict
    
    
    


# In[18]:


## 3.
## API FUNCTION
## MAIN function code to execute a search
## --------------------------------------
class exAPI_run_search_query(Resource):
    
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
                    parser.add_argument('feild_names')
                    parser.add_argument('search_query')
                    
                    args = parser.parse_args()

                    # 2.
                    # Getting params
                    # --------------
                    query = args['query']
                    table_name = args['table_name']
                    chunk_size = args['chunk_size']
                    feild_names = args['feild_names']
                    search_query = args['search_query']
                    
                    
                    # 3.
                    # sanity for internal
                    # -------------------
                    print('API exAPI_run_search_query firing: ' + str(feild_names) + ' -- ' + str(search_query) + ' -- ' + str(query))
                    
                    
                    try:
                        # Using get task id function
                        # --------------------------
                        d = run_search_query_api_function(table_name,query,chunk_size,feild_names,search_query)
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

# In[8]:


# necessary set ups
# -----------------
app = Flask(__name__)
api = Api(app)


# In[ ]:


# Adding resource
# ---------------
api.add_resource(exAPI_run_insert_update_delete_query, '/run_insert_update_delete_query') # Route
api.add_resource(exAPI_run_select_query, '/run_select_query') # Route
api.add_resource(exAPI_run_search_query, '/run_search_query') # Route
 


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