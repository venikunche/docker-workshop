#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv')


# In[5]:


import pandas as pd


# In[6]:


df_zones = pd.read_csv('taxi_zone_lookup.csv')


# In[7]:


df_zones.head()


# In[9]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[11]:


df_zones.to_sql(name='zones', con=engine, if_exists='replace')


# In[ ]:




