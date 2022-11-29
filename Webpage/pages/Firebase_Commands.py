import requests
import pandas as pd
import json
import sys
import ast
import numpy as np
import streamlit as st

st.write('EDFS Commands with Firebase Database           (command /foldername/filename)')
command = st.text_input('Enter a command:')
com = command.split(' ')
# st.write('command /foldername/filename')

if len(com) > 1:
    if com[0] == 'ls':
        file = com[1]
        url = "https://metadata-114ea-default-rtdb.firebaseio.com"
        finalURL = url + file + '.json'
        m = requests.get(finalURL)
        cont = m.content.decode('UTF-8')
        json = ast.literal_eval(cont)
        for key, value in json.items():
            st.write(key)
    elif com[0] == 'mkdir':
        url = "https://final-project-551-ce020-default-rtdb.firebaseio.com"
        file = com[1]
        finalURL = url + file + '.json'
        final = json.dumps('')
        response1 = requests.put(finalURL, final)
        m = json.dumps(response1.json(), indent=4)
        url1 = "https://metadata-114ea-default-rtdb.firebaseio.com"
        finalURL1 = url1 + file + '.json'
        response11 = requests.put(finalURL1, final)
        m1 = json.dumps(response11.json(), indent=4)
        st.write('Done!')
    elif com[0] == 'cat':
        url = "https://final-project-551-ce020-default-rtdb.firebaseio.com"
        file = com[1]
        file1 = file.split('.')
        finalURL = url + file1[0] + '.json'
        m = requests.get(finalURL)
        cont = m.content.decode('UTF-8')
        json = ast.literal_eval(cont)
        for key, value in json.items():
            st.write(value)
    elif com[0] == 'rm':
        url = "https://final-project-551-ce020-default-rtdb.firebaseio.com"
        file = com[1]
        file1 = file.split('.')
        finalURL = url + file1[0] + '.json'
        final = json.dumps('')
        response1 = requests.put(finalURL, final)
        m = json.dumps(response1.json(), indent=4)
        url1 = "https://metadata-114ea-default-rtdb.firebaseio.com"
        finalURL1 = url1 + file + '.json'
        response11 = requests.put(finalURL1, final)
        m1 = json.dumps(response11.json(), indent=4)
        st.write('Done!')
    elif 'put' in com[0]:
        url = "https://final-project-551-ce020-default-rtdb.firebaseio.com"
        ifile = com[0]
        file1 = ifile.split('(')
        file2 = file1[1].split(')')
        um = file2[0].split(',')
        upload = um[0].strip()
        name = upload.split('.')
        fileName = name[0]
        parts = um[2].split('=')
        part = int(parts[1].strip())
        file = um[1].strip()
        final = {}
        meta = {}
        if '.csv' in upload:
            csv = pd.read_csv(upload)
            count = 0
            for rows in np.split(csv, part):
                count = count + 1
                name = 'p' + str(count)
                newDict = rows.to_dict('index')
                final[name] = newDict
                meta[name] = url + file + fileName + '.json'
        elif '.json' in upload:
            json1 = open(upload)
            new = json.load(json1)
            if type(new) == list:
                partt = len(new) / part
                for i in np.arange(part):
                    j = i + 1
                    name = 'p' + str(j)
                    start = int((j * partt) - partt)
                    end = int((j * partt) - 1)
                    pdict = {}
                    for k in new:
                        if new.index(k) >= start and new.index(k) <= end:
                            pdict[new.index(k)] = k
                        else:
                            pass
                    final[name] = pdict
                    meta[name] = url + file + fileName + '.json'
            else:
                st.write('Put JSON object into list form')
        else:
            pass
        finalURL = url + file + fileName + '.json'
        final1 = json.dumps(final)
        response1 = requests.put(finalURL, final1)
        m = json.dumps(response1.json(), indent=4)

        metaFinal = json.dumps(meta)
        url1 = "https://metadata-114ea-default-rtdb.firebaseio.com"
        finalURL1 = url1 + file + fileName + '.json'
        response11 = requests.put(finalURL1, metaFinal)
        m1 = json.dumps(response11.json(), indent=4)
    elif 'getPartitionLocations' in com[0]:
        input = com[0]
        url1 = "https://metadata-114ea-default-rtdb.firebaseio.com"
        j = input.split('(')
        other = j[1].split(')')
        file = other[0]
        url = url1 + file + '.json'
        response = requests.get(url)
        st.write(response.content.decode('UTF-8'))
    elif 'readPartition' in com[0]:
        input = com[0]
        url1 = "https://final-project-551-ce020-default-rtdb.firebaseio.com"
        j = input.split('(')
        other = j[1].split(')')
        file = other[0]
        um = file.split(',')
        first = um[0].strip()
        second = um[1].strip()
        url = url1 + first + '.json'
        response = requests.get(url)
        fileDict = ast.literal_eval(response.content.decode('UTF-8'))
        for key, value in fileDict.items():
            if key == second:
                for i in value:
                    st.write(i)
    else:
        st.write('Invalid')
else:
    pass
