import requests
import pandas as pd
import json
import sys
import ast
import numpy as np
import mysql.connector
import warnings
import streamlit as st
warnings.filterwarnings(action='ignore')


def map_one(y, s, c):
    partitions = ['p1','p2','p3','p4','p5','p6']
    prices = []
    reduce = {}
    st.write('Map:')
    for i in range(len(partitions)):
        data = pd.read_sql('select count(distinct(' + y + ')) as Number_of_rows from city_MR_1_bedroom_n partition('+ partitions[i] +') where State= "' + s +'" and CountyName= "'+ c + '";',db)
        red_data = pd.read_sql('select ' + y + ' from city_MR_1_bedroom_n partition('+ partitions[i] +') where State= "' + s +'" and CountyName= "'+ c + '";',db)
        if not red_data.empty:
            price = red_data.values[0][0]
            prices.append(price)
            if reduce == {}:
                reduce[y] = [price]
            else:
                reduce[y].append(price)
        st.write('Partition:', partitions[i])
        st.write(data)
        st.write()
    finalReduce = pd.DataFrame(reduce)
    st.write('Reduce Function:')
    st.write(finalReduce)

def map_two(y, s, c):
    partitions = ['p1','p2','p3','p4','p5','p6','p7','p8','p9','p10']
    prices = []
    reduce = {}
    st.write('Map:')
    for i in range(len(partitions)):
        data = pd.read_sql('select count(distinct(' + y + ')) as Number_of_rows from city_MR_2_bedroom_n partition(' + partitions[i] + ') where State= "' + s + '" and CountyName= "' + c + '";', db)
        red_data = pd.read_sql('select ' + y + ' from city_MR_2_bedroom_n partition(' + partitions[i] + ') where State= "' + s + '" and CountyName= "' + c + '";', db)
        if not red_data.empty:
            price = red_data.values[0][0]
            prices.append(price)
            if reduce == {}:
                reduce[y] = [price]
            else:
                reduce[y].append(price)
        st.write('Partition:', partitions[i])
        st.write(data)
        st.write()
    finalReduce = pd.DataFrame(reduce)
    st.write('Reduce Function:')
    st.write(finalReduce)

def map_three(y, s, c):
    partitions = ['p1','p2','p3','p4','p5','p6','p7','p8','p9','p10']
    prices = []
    reduce = {}
    st.write('Map:')
    for i in range(len(partitions)):
        data = pd.read_sql('select count(distinct(' + y + ')) as Number_of_rows from city_MR_3_bedroom_n partition(' + partitions[i] + ') where State= "' + s + '" and CountyName= "' + c + '";', db)
        red_data = pd.read_sql('select ' + y + ' from city_MR_3_bedroom_n partition(' + partitions[i] + ') where State= "' + s + '" and CountyName= "' + c + '";', db)
        if not red_data.empty:
            price = red_data.values[0][0]
            prices.append(price)
            if reduce == {}:
                reduce[y] = [price]
            else:
                reduce[y].append(price)
        st.write('Partition:', partitions[i])
        st.write(data)
        st.write()
    finalReduce = pd.DataFrame(reduce)
    st.write('Reduce Function:')
    st.write(finalReduce)

db = mysql.connector.connect(host='ec2-3-101-14-210.us-west-1.compute.amazonaws.com', user='kaceykroeck', password='kK920395!', db='one', auth_plugin='mysql_native_password')
if db.is_connected():
    st.write("Connection done!")

st.write('PMR with MySQL Database')
bedroom = st.text_input('Enter a number of bedrooms (1, 2, or 3):')
state = st.text_input('Enter a state (ex. NY):')
coname = st.text_input('Enter a country name (ex. Queens):')
year = st.text_input('Enter a date in the format of yyyy-mm (ex. 2011-01):')

if bedroom == '1':
    map_one(year,state,coname)
elif bedroom == '2':
    map_two(year,state,coname)
elif bedroom == '3':
    map_three(year,state,coname)    
else:
    st.write("Filter search")