import requests
import pandas as pd
import json
import sys
import ast
import numpy as np
import warnings
import streamlit as st
warnings.filterwarnings(action='ignore')

st.subheader("PMR_Firebase")

def map_one(y, s, c):
    partitions = ['p1','p2','p3','p4','p5','p6']
    prices = []
    reduce = {}
    st.write('Map:')
    for i in range(len(partitions)):
        st.write('Partition:', partitions[i])
        url1 = "https://final-project-551-ce020-default-rtdb.firebaseio.com"
        url = url1 + '/user/kacey/beds/city_MR_1_bedroom_n/'+ partitions[i] + '.json'
        response = requests.get(url)
        fileDict = ast.literal_eval(response.content.decode('UTF-8'))
        for i in fileDict:
            if i['State'] == s and i['CountyName'] == c:
                want = i[y]
                st.write(want)
                prices.append(want)
                if reduce == {}:
                    reduce[y] = [want]
                else:
                    reduce[y].append(want)
    finalReduce = pd.DataFrame(reduce)
    st.write()
    st.write('Reduce Function:')
    st.write(finalReduce)

def map_two(y, s, c):
    partitions = ['p1','p2','p3','p4','p5','p6','p7','p8','p9','p10']
    prices = []
    reduce = {}
    st.write('Map:')
    for i in range(len(partitions)):
        st.write('Partition:', partitions[i])
        url1 = "https://final-project-551-ce020-default-rtdb.firebaseio.com"
        url = url1 + '/user/kacey/beds/city_MR_2_bedroom_n/'+ partitions[i] + '.json'
        response = requests.get(url)
        fileDict = ast.literal_eval(response.content.decode('UTF-8'))
        for i in fileDict:
            if i['State'] == s and i['CountyName'] == c:
                want = i[y]
                st.write(want)
                prices.append(want)
                if reduce == {}:
                    reduce[y] = [want]
                else:
                    reduce[y].append(want)
    finalReduce = pd.DataFrame(reduce)
    st.write()
    st.write('Reduce Function:')
    st.write(finalReduce)

def map_three(y, s, c):
    partitions = ['p1','p2','p3','p4','p5','p6','p7','p8','p9','p10']
    prices = []
    reduce = {}
    st.write('Map:')
    for i in range(len(partitions)):
        st.write('Partition:', partitions[i])
        url1 = "https://final-project-551-ce020-default-rtdb.firebaseio.com"
        url = url1 + '/user/kacey/beds/city_MR_3_bedroom_n/'+ partitions[i] + '.json'
        response = requests.get(url)
        fileDict = ast.literal_eval(response.content.decode('UTF-8'))
        for i in fileDict:
            if i['State'] == s and i['CountyName'] == c:
                want = i[y]
                st.write(want)
                prices.append(want)
                if reduce == {}:
                    reduce[y] = [want]
                else:
                    reduce[y].append(want)
    finalReduce = pd.DataFrame(reduce)
    st.write()
    st.write('Reduce Function:')
    st.write(finalReduce)

st.write('PMR with Firebase Database')
bedroom = st.text_input('Enter a number of bedrooms: (1, 2, or 3):')
state = st.text_input('Enter a state: (ex. NY):')
coname = st.text_input('Enter a country name: (ex. Queens):')
year = st.text_input('Enter a date in the format of yyyy-mm: (ex. 2011-01):')

if bedroom == '1':
    map_one(year,state,coname)
elif bedroom == '2':
    map_two(year,state,coname)
elif bedroom == '3':
    map_three(year,state,coname)
else:
    st.write("Filter search")
