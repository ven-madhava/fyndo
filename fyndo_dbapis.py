
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
vm_or_local = 'local'

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


# ## 2. local notebook psql ops -- delete from this point on before uploading

# ## 2.1 Internal table ops

# In[9]:


# list of feilds buidler 
# ---------------------
def cols_feilds_builder(table_name):
    
    # 1. getting cols
    # ---------------
    f,m = return_psql_query(table_name,8,10)
    out = run_psql_queries(f,1000,'all',False)


    # 2. building col string
    # ----------------------
    cols = '('
    for each in out:
        cols += str(each) +', '
    cols = cols[:-2]
    cols += ')'
    
    # final return
    # ------------
    return cols


# In[10]:


# Super helpful chunker function that returns seq chunks correctly sized even at ends
# -----------------------------------------------------------------------------------

def chunker(seq, size):
    
    # from http://stackoverflow.com/a/434328
    # not touch this code
    # -------------------
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


# In[11]:


# a new simpler return_psql_query function
# ----------------------------------------



def return_psql_query(table_name,mode,data):
    
    
    '''
    
    1. will take in params and data in specific format
    2. will return a psql query statement
    
    mode legend
    -----------
    0 - create table
    401 - remane table
    404 - drop table if exists
    
    1 - insert rows
    2 - update rows
    3 - delete rows
    4 - delete all rows
    
    5 - add column
    6 - delete column
    
    7 - select all records
    8 - select records with limit
    9 - select just count of records
    
    10 - select records with conditions
    
    '''
    
    # 0. inits
    # --------
    psql_dtypes = ['varchar','float8','int']
    
    
    # 1. main if statement
    # --------------------
    
    # create table
    # -----------
    if mode == 0:
        
        # create table
        # CREATE TABLE test_projx_profiles (user_id varchar, user_name varchar, 
        # user_title varchar, user_brief varchar, user_industry varchar);
        
        # 1.1 data format
        # data = string in format 
        # -- '(field_name dtype PRIMARY KEY, feild_name dtype,...)'
        
        # building final query
        # --------------------
        final_queries = 'CREATE TABLE ' + str(table_name) + ' ' + data + ';'
    
    
    # rename table
    # ------------
    elif mode == 401:
        
        # here data is just new table name
        # --------------------------------
        final_queries = 'ALTER TABLE ' + table_name + ' RENAME TO ' + data + ';'
        
    # delete table
    # ------------
    elif mode == 404:
        
        # drop table
        # -----------
        final_queries = 'DROP TABLE ' + table_name + ';'
    
    
    # insert rows
    # -----------
    elif mode == 1:
        
        
        # insert rows
        # -----------
        
        '''
        
        INSERT INTO table_name (field_name_1, field_name_2)
        VALUES
        
        ('http://www.google.com','Google'),
        ('http://www.yahoo.com','Yahoo'),
        ('http://www.bing.com','Bing');

        '''
        # inserts recods
        # data to be dict
        # format d['cols'] = [field_1,feild_2]
        # d['rows'] = [(data01,data02),(data11,data12),(data21,data22),.....]
        # ----------------------------------------------------------------------
        final_queries = 'INSERT INTO ' + table_name + ' ' + str(data['cols']) + ' VALUES '
        
        
        # 2. appending rows
        # ----------------
        for i in range(len(data['rows'])):
            
            final_queries += str(data['rows'][i])
            
            # ops
            # ---
            if i == len(data['rows']) - 1:
                final_queries += ';'
            else:
                final_queries += ','
    
    
    # update rows
    # -----------
    elif mode == 2:
        
        '''
        
        UPDATE table
        SET column1 = value1,
            column2 = value2 ,...
        WHERE
        condition;
    
        UPDATE link
        SET last_update = DEFAULT
        WHERE last_update IS NULL;
    
        '''
        
        # data to be in format
        # data['condition'] = ('feild','value')
        # data['rows'] = [('column1','value1'),('column2','value2'),..]
        # --------------------------------------------------------------
        final_queries = 'UPDATE ' + table_name + ' SET '
        for each in data['rows']:
            final_queries += str(each[0] + ' = ' + each[1]) + ', '
        final_queries = final_queries[:-2]
        
        # appending where
        # ---------------
        final_queries += ' WHERE ' + str(data['condition'][0]) + ' IS ' + str(data['condition'][1]) + ';'
    
    
    
    # delete row with condition
    # -------------------------
    elif mode == 3:
        
        # delete row with condition
        # data in format LIST
        # ['id','value]
        # ------------------------
        
        '''
        
        DELETE FROM link
        WHERE id = 8;

        '''
        
        final_queries = 'DELETE FROM ' + table_name + ' WHERE ' + str(data[0]) + ' = ' + str(data[1]) + ';'
        
    
    
    # delete all rows
    # ---------------
    elif mode == 4:
        
        ''' DELETE FROM link; '''
        
        final_queries = 'DELETE FROM ' + table_name + ';'
    
    
    
    # adding new col
    # --------------
    elif mode == 5:
        
        '''
        
        ALTER TABLE customer 
        ADD COLUMN fax VARCHAR,
        ADD COLUMN email VARCHAR;
        
        ALTER TABLE customers
        ADD COLUMN contact_name NOT NULL DEFAULT 'foo';
        
        '''
        
        # addind a col to db
        # date to be in list
        # [('name','varchar'),('age','int')]
        # NOT ACCOMODATING DEFAULT VALUE FOR NOW
        # --------------------------------------s
        final_queries = 'ALTER TABLE ' + table_name
        
        # itering thru data list
        # ----------------------
        for each in data:
            final_queries += ' ADD COLUMN ' + str(each[0]) + ' ' + str(each[1]) + ', '
        final_queries = final_queries[:-2]
        final_queries += ';'
    
    
    # delete column
    # -------------
    elif mode == 6:
        
        '''
        
        ALTER TABLE table_name DROP COLUMN column_name;
        
        '''
        
        # data in format list
        # ['col_name', 'colname',...]
        # ----------------------------
        final_queries = 'DROP COLUMN IF EXISTS ' + ', '.join(data) + ';'
    
    
    
    # select all records
    # ------------------
    elif mode == 7:
        
        # selects all rows
        # ----------------
        final_queries = 'SELECT * FROM ' + table_name + ';'
        
    
    
    # select all records 
    # WITH LIMIT
    # ------------------
    elif mode == 8:
        
        # data is just a value like 50
        # select all rows with limit
        # --------------------------
        final_queries = 'SELECT * FROM ' + table_name + ' LIMIT ' + str(data) + ';'
    
    
    # select just count of records
    # ----------------------------
    elif mode == 9:
        
        # returns count only
        # ------------------
        final_queries = 'SELECT COUNT(*) FROM ' + table_name + ';'
    
    
    
    # sleect rows with conditions
    # ---------------------------
    elif mode == 10:
        
        
        
        '''
        # https://www.postgresqltutorial.com/postgresql-where/
        
        WHERE OPERATOR CLAUSES
        -----------------------
        Operator  Description
        = Equal
        > Greater than
        < Less than
        >= Greater than or equal
        <= Less than or equal
        <> or != Not equal
        AND Logical operator AND
        OR Logical operator OR
        
        
        SELECT select_list
        FROM table_name
        WHERE condition;
        
        SELECT
            last_name,
            first_name
        FROM
            customer
        WHERE
            first_name = 'Jamie';
        
        
        SELECT
            first_name,
            last_name
        FROM
            customer
        WHERE
            last_name = 'Rodriguez' OR 
            first_name = 'Adam';
    
    
        '''
        
        # data must be in format
        # data['cols'] = [col1', 'col2'] or ['*']
        
        # data['ops'] = '=' or '<=' etc
        # data['condition'] = 'AND', 'OR'
        # ---------------------------------------
        final_queries = 'SELECT ' + ', '.join(data['cols'] ) + ' FROM ' + table_name + ';'# + ' WHERE '
    
    
    
    # final return
    # ------------
    return final_queries, mode
    


# In[12]:


# GENERIC - function to execute psql queries
# -------------------------------------------

def run_psql_queries(final_queries,mode,chunk_size,print_status):
    
    
    # RUN THE BELOW PROXY COMMAND
    # THIS IS ONLY IF USING GCLOUD PSQL
    # ---------------------------------
    # ./cloud_sql_proxy -instances=ven-ml-project:us-central1:pmate-psql=tcp:5432
    
    # 1. Creating a new connection
    # Proxy needs to be running inorder for below to run
    # Below works
    # local settings
    # can be the same for local / vm
    # --------------------------------------------------
    host = '127.0.0.1'
    db = 'solvr'
    #user = 'postgres'
    #password = 'v230385v'

    # 2. create a connection
    # ----------------------
    #con = psql.connect(host = host, database = db, user = user, password = password)
    con = psql.connect(host = host, database = db)
    cur = con.cursor()
    
    # 3. executing query
    # ------------------
    cur.execute(final_queries)
    if print_status == True:
        print('Executed:\n' + str(final_queries))
    
    
    # 4. setting output
    # reserve may be 0 - for generic modes
    # -------------------------------------
    
    # fetching col names only
    # -----------------------
    if mode == 1000:
        
        out = [desc[0] for desc in cur.description]
        
    
    # fetching rows
    # ------------
    elif mode == 7 or mode == 8: # selecting rows
        
        # showing cols
        # ------------
        out = [desc[0] for desc in cur.description]
        print('col heads - ')
        print(out)
        print('**********')
        
        if chunk_size == 'all':
            out = cur.fetchall()
        else:
            out = cur.fetchmany(chunk_size)
    
    
    # viewing count
    # ------------
    elif mode == 9: 
        
        out = cur.fetchone()
    
    
    # all else
    # --------
    else:
        out = 'ok'
        
    
    # 4. final closures
    # -----------------
    con.commit()
    cur.close()
    con.close()
    
    # 5. returning fetches
    # --------------------
    return out
    


# ### 2.1.1 create table
# having table names and col list for later use
# ---------------------------------------------

### USERS ###
#############

# users
# -----
users_d = {}
users_d['table'] = 'userprofile'
users_d['cols'] = '(user_id varchar PRIMARY KEY, user_name varchar, user_phone varchar, user_email varchar, user_title varchar, user_about varchar, user_dpurl varchar, user_educationbrief varchar, user_location varchar, user_projectstatus varchar, user_onlinestatus varchar, user_createdon TIMESTAMP, user_lastlogin TIMESTAMP)'
#del users_d

# skill
# -----
skill_d = {}
skill_d['table'] = 'skill'
skill_d['cols'] = '(skill_id varchar PRIMARY KEY, category_id varchar, skill_name varchar, skill_desc varchar)'
#del skill_d


# category
# -----
category_d = {}
category_d['table'] = 'category'
category_d['cols'] = '(category_id varchar PRIMARY KEY, category_name varchar, category_desc varchar)'
#del category_d

# workex
# ------
workex_d = {}
workex_d['table'] = 'workex'
workex_d['cols'] = '(workex_id varchar PRIMARY KEY, user_id varchar, workex_companyname varchar, workex_startdate DATE, workex_enddate DATE, workex_title varchar, workex_desc varchar)'
#del workex_d

# userskill
# ------
userskill_d = {}
userskill_d['table'] = 'userskill'
userskill_d['cols'] = '(userskill_id varchar PRIMARY KEY, user_id varchar, skill_id varchar, userskill_desc varchar, userskill_achievements varchar)'
#del userskill_d

################
# NOT USING THIS
################
# userskillsachievement
# ---------------------
#userskillsachievement_d = {}
#userskillsachievement_d['table'] = 'userskillsachievement'
#userskillsachievement_d['cols'] = '(userskillsachievement_id varchar PRIMARY KEY, userskill_id varchar, userskillsachievement_desc varchar)'
#del userskillsachievement_d


# userrating
# ---------------------
userrating_d = {}
userrating_d['table'] = 'userrating'
userrating_d['cols'] = '(userrating_id varchar PRIMARY KEY, user_id varchar, userrating_provideduserid varchar, userrating_message varchar)'
#del userrating_d
 
### PROJECTS ###
################

# project
# ---------------------
project_d = {}
project_d['table'] = 'project'
project_d['cols'] = '(project_id varchar PRIMARY KEY, user_id varchar, project_brief varchar, project_status varchar, project_startdate DATE, project_enddate DATE)'
#del project_d

# projectskill
# -------------
projectskill_d = {}
projectskill_d['table'] = 'projectskill'
projectskill_d['cols'] = '(projectskill_id varchar PRIMARY KEY, project_id varchar, skill_id varchar, projectskill_status varchar)'
#del projectskill_d


# projectskilluser
# -------------
projectskilluser_d = {}
projectskilluser_d['table'] = 'projectskilluser'
projectskilluser_d['cols'] = '(projectskilluser_id varchar PRIMARY KEY, projectskill_id varchar, user_id varchar, projectskilluser_userintromessage varchar, projectskilluser_status varchar)'
#del projectskilluser_d


# projectmessages
# -------------
projectmessages_d = {}
projectmessages_d['table'] = 'projectmessage'
projectmessages_d['cols'] = '(projectmessage_id varchar PRIMARY KEY, projectskilluser_id varchar, user_id varchar, projectmessage_timestamp TIMESTAMP, projectmessage_message varchar)'
#del projectmessages_d



# In[17]:


# shopprofile
# -----------

shop_profile = {}
shop_profile['table'] = 'shopprofile'
shop_profile['cols'] = "(shopprofile_id varchar PRIMARY KEY, shopprofile_name varchar, shopprofile_address varchar, shopprofile_phone varchar, shopprofile_email varchar, shopprofile_dpurl varchar, shopprofile_verified varchar NOT NULL DEFAULT 'false', shopprofile_createdon DATE, shopprofile_lastlogin TIMESTAMP)"

shop_post = {}
shop_post['table'] = 'shoppost'
shop_post['cols'] = "(shoppost_id varchar PRIMARY KEY, shopprofile_id varchar, shoppost_content varchar, shoppost_createdon TIMESTAMP, shoppost_img1url varchar NOT NULL DEFAULT 'na', shoppost_img2url varchar NOT NULL DEFAULT 'na', shoppost_img3url varchar NOT NULL DEFAULT 'na', shoppost_img4url varchar NOT NULL DEFAULT 'na')"

shop_product = {}
shop_product['table'] = 'shopproduct'
shop_product['cols'] = "(shopproduct_id varchar PRIMARY KEY, shopprofile_id varchar, prodcategory_name varchar, shopproduct_name varchar, shopproduct_desc varchar, shopproduct_mrp varchar, shopproduct_availability varchar, shopproduct_featured varchar, shopproduct_img1url varchar NOT NULL DEFAULT 'na',  shopproduct_img2url varchar NOT NULL DEFAULT 'na',  shopproduct_img3url varchar NOT NULL DEFAULT 'na',  shopproduct_img4url varchar NOT NULL DEFAULT 'na', shopproduct_lastedit TIMESTAMP)"


prod_category = {}
prod_category['table'] = 'prodcategory'
prod_category['cols'] = "(prodcategory_id varchar PRIMARY KEY, prodcategory_name varchar, prodcategory_dpurl varchar NOT NULL DEFAULT 'na')"



# In[18]:


# 1.
# building query
# --------------

curr_d = prod_category
f,m = return_psql_query(curr_d['table'],0,curr_d['cols'])
del curr_d
f,m

# 2.
# running query
# -------------

run_psql_queries(fq,1,'all',True)
# ## 2.2 generice table ops

# In[ ]:


# 3.
# looking at all tables in the databse after creatin tables
# ---------------------------------------------------------

run_psql_queries("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';",7,'all',True)


# In[ ]:


# 4. 
# looking at cols given a table
# ------------------------------

table_name_for_count = 'userprofile'
f,m = return_psql_query(table_name_for_count,8,10)
out = run_psql_queries(f,m,'all',False)
out


# ## 2.3 local psql ops

# In[14]:


# 1.
# getting col names
# -----------------
#table_name = 'userprofile'

# lookup
# ------
#cols_feilds_builder(table_name)


# In[ ]:


# 2. inserting record query builder
# ---------------------------------
fq = 'INSERT INTO ' + table_name + ' ' + str(cols_feilds_builder(table_name)) + ' VALUES '
fq


# 3. 
# set vrow value here
# -------------------
row = ('up002','Mukundh Venkatesh','9790946063','mukundh@geospot.in','Student','I am student',
      'user_dpurl_tbu','I m in TIPS school.','Chennai,TN','Active','2020-05-12',str(datetime.now()))


# final query
# ----------
fq += str(row) + ';'
fq

INSERT INTO prodcategory (prodcategory_id, prodcategory_name) VALUES 
('pc01', 'Fashion'),
('pc02', 'Electronics'),
('pc03', 'Furniture'),
('pc04', 'Beauty & Health'),
('pc05', 'Toys'),
('pc06', 'Jewellery'),
('pc07', 'Sports'),
('pc08', 'Industrial');
# INSERT INTO posttype (posttype_id, posttype_name) VALUES 
# ('pt01', 'Discount'),
# ('pt02', 'New Arrivals'),
# ('pt03', 'Promo');

# In[264]:


'list' in str(type([1,2,3]))


# # rough

# In[11]:


# search concept
# ---------------

#srch_db_1 = 'This is a head less ear phones for the best experience. Sony earphones pro'
#srch_db_2 = 'This is a great noise free product. Comes with surrond sound. Earphones and mic is attached in it. Philips Headphones Plus.'


# In[12]:


# 

#src_q = 'iphone'
#return_search_score(src_q)

raw_results = {}
raw_results['output'] = {}
raw_results['status'] = 'ok'


raw_results['output'][1] = {}
raw_results['output'][1]['content'] = 'This is a head less ear phones for the best experience'
raw_results['output'][1]['name'] = 'Sony earphones pro'
raw_results['output'][1]['type'] = 'electronic'
raw_results['output'][1]['date'] = '22-2-2020'



raw_results['output'][0] = {}
raw_results['output'][0]['content'] = 'This is a great noise free product. Comes with surrond sound. Earphones and mic is attached in it.'
raw_results['output'][0]['name'] = 'Philips Headphones Plus'
raw_results['output'][0]['type'] = 'electronic'
raw_results['output'][0]['date'] = '25-2-2020'

# example
raw_results