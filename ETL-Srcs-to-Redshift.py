#!/usr/bin/env python
# coding: utf-8

# EXTRACT

# In[3]:


import pandas as pd
from sqlalchemy import create_engine


# In[4]:


engine = create_engine('postgresql+psycopg2://postgres:root@localhost/postgres')


# In[6]:


df_trades = pd.read_sql("select * from trades", engine)


# In[7]:


df_countries = pd.read_json('src/country_data.json')


# In[8]:


df_codes = pd.read_csv('src/hs_codes.csv')


# TRANSFORM

# In[12]:


df_parents = df_codes[df_codes['Level']==2].copy()


# In[14]:


df_codes = df_codes[df_codes['Code_comm'].notnull()]


# In[15]:


def clean_code(text):
    text = str(text)
    parente_code = None
    if len(text) == 11:
        code = text[:5]
        parent_code = text[:1]
    else:
        code = text[:6]
        parent_code = text[:2]
    try:
        parent = df_parents[df_parents['Code_comm']==parent_code]['Description'].values[0]
    except:
        parent = None
    return (code, parent)


# In[16]:


df_codes[['clean_code','parent_description']]=df_codes.apply(lambda x: clean_code(x['Code']),axis=1,result_type='expand')


# In[18]:


df_codes = df_codes[df_codes['clean_code'].notnull()][['clean_code','Description','parent_description']]


# In[20]:


df_codes['id_code'] = df_codes.index + 1
df_codes['clean_code'] = df_codes['clean_code'].astype('int64')
df_codes


# In[22]:


df_countries = df_countries[['alpha-3','country','region','sub-region']]


# In[24]:


df_countries = df_countries[df_countries['alpha-3'].notnull()]


# In[25]:


df_countries['id_country'] = df_countries.index + 1


# In[27]:


df_trades_clean = df_trades.merge(df_codes[['clean_code','id_code']],how='left',
                                  left_on='comm_code',right_on='clean_code')


# In[30]:


df_trades_clean = df_trades_clean.merge(df_countries[['alpha-3','id_country']],how='left',
                                  left_on='country_code',right_on='alpha-3')


# In[32]:


def create_dimension(data, id_name):
    list_keys = []
    value = 1
    for _ in data:
        list_keys.append(value)
        value = value + 1
    return pd.DataFrame({id_name:list_keys, 'values':data})


# In[33]:


df_quantity = create_dimension(df_trades_clean['quantity_name'].unique(),'id_quantity')
df_flow = create_dimension(df_trades_clean['flow'].unique(),'id_flow')
df_year = create_dimension(df_trades_clean['year'].unique(),'id_year')


# In[38]:


df_trades_clean = df_trades_clean.merge(df_quantity, how='left',
                                        left_on='quantity_name', right_on='values')
df_trades_clean = df_trades_clean.merge(df_flow, how='left',
                                        left_on='flow', right_on='values')
df_trades_clean = df_trades_clean.merge(df_year, how='left',
                                        left_on='year', right_on='values')


# In[40]:


df_trades_clean['id_trades'] = df_trades_clean.index + 1


# In[41]:


df_trades_final = df_trades_clean[['id_trades','trade_usd','kg','quantity','id_code','id_country',
                                   'id_quantity','id_flow','id_year']].copy()
df_trades_final


# In[42]:


df_codes = df_codes [['id_code','clean_code','Description','parent_description']]


# LOAD

# In[43]:


df_trades_final.to_csv('target/trades.csv',index=False,sep='|')
df_countries.to_csv('target/countries.csv',index=False,sep='|')
df_codes.to_csv('target/codes.csv',index=False,sep='|')
df_quantity.to_csv('target/quantity.csv',index=False,sep='|')
df_flow.to_csv('target/flow.csv',index=False,sep='|')
df_year.to_csv('target/year.csv',index=False,sep='|')


# In[1]:


import os
import boto3
import redshift_connector


# In[7]:


client = boto3.client(
    's3',
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY'),
    )
conn = redshift_connector.connect(
    host = 'demo-platzi-curso-etl.clstkqbf7evy.sa-east-1.redshift.amazonaws.com',
    database = 'dev',
    port = 5439,
    user = 'demojavi',
    password = 'Fernandez1',
    )


# In[8]:


cursor = conn.cursor()


# In[9]:


def load_file(file_name):
    table_name = file_name.split('.')[0]
    client.upload_file(
        Filename='target/{}'.format(file_name),
        Bucket = 'myawsbucket-javi',
        Key = '/{}'.format(file_name),
    )
    sentence = '''copy public.{} from 's3://myawsbucket-javi/{}' credentials 'aws_access_key_id={};aws_secret_access_key={}'
    csv delimiter '|' region 'sa-east-1' ignoreheader 1'''.format(table_name, file_name, os.environ.get('AWS_ACCESS_KEY_ID'),
                                                                  os.environ.get('AWS_SECRET_ACCESS_KEY'))
    try:
        cursor.execute(sentence)
        print('OK en la tabla '+table_name)
    except:
        print('error en la tabla '+table_name)


# In[11]:


#iterate the function in all my files
#it will give error but the files will be charged
for file in os.listdir('target/'):
    try:
        load_file(file)
    except ClienError as e:
        print(e)

