import requests
import pandas as pd
import json
import sys
import ast
import numpy as np
import mysql.connector
import streamlit as st

db = mysql.connector.connect(host='ec2-3-101-14-210.us-west-1.compute.amazonaws.com', user='kaceykroeck', password='kK920395!', db='one', auth_plugin='mysql_native_password')
st.write('EDFS Commands with MySQL Database')
command = st.text_input('Enter a command:')
com = command.split(' ')

if len(com) > 1:
    if com[0] == 'ls':
        input = com[1]
        input1 = input.split('/')
        last = input1[-1]
        data = pd.read_sql('select name from metadata where id in (select child from structure where parent in (select id from metadata where name = "' + last + '"));', db)
        st.write(data)
    elif com[0] == 'mkdir':
        input = com[1]
        input1 = input.split('/')
        insert = input1[-2]
        last = input1[-1]
        id = pd.read_sql('select id from metadata;', db)
        listID = []
        for i in id.values:
            listID.append(i[0])
        maxID = max(listID)
        newID = str(maxID + 1)
        data = pd.read_sql('select id from metadata where name = "' + insert + '";', db)
        parent = str(data.values[0][0])
        cur = db.cursor()
        cur.execute('insert into metadata (id, name) values (' + newID + ', "' + last + '");')
        db.commit()
        cur.execute('insert into structure (parent, child) values (' + parent + ', ' + newID + ');')
        db.commit()
    elif com[0] == 'cat':
        input = com[1]
        input1 = input.split('/')
        last = input1[-1]
        item = last.split('.')
        file = item[0]
        inside = pd.read_sql('select * from ' + file + ';', db)
        st.write(inside)
    elif com[0] == 'rm':
        input = com[1]
        input1 = input.split('/')
        last = input1[-1]
        item = last.split('.')
        file = item[0]
        inside = pd.read_sql('select id from metadata where name = "' + file + '";', db)
        id = str(inside.values[0][0])
        cur = db.cursor()
        # delete from partition_info
        cur.execute('delete from partition_info where id = ' + id + ';')
        # delete from metadata
        cur.execute('delete from metadata where id = ' + id + ';')
        # delete from structure
        cur.execute('delete from structure where child = ' + id + ';')
        cur.execute('delete from structure where parent = ' + id + ';')
        # delete table
        cur.execute('drop table ' + file + ';')
        db.commit()
    elif 'put' in com[0]:
        input = com[0]
        import numpy as np
        input1 = input.split('(')
        file = input1[-1]
        file1 = file.split(')')[0]
        items = file1.split(',')
        split = items[1].split('/')
        parts = items[2].split('=')
        k = parts[-1].strip()
        parent = split[-2].strip()
        folder = split[-1].strip()
        upload = items[0]
        name = upload.split('.')[0]
        cur = db.cursor()
        if '.json' in upload:
            data = pd.read_json(upload)
            cur.execute('drop table if exists ' + name + ';')
            data['other'] = range(1, len(data) + 1)
            for i in data.columns:
                if i == '':
                    data = data.drop(i, axis=1)
            typeDict = {}
            for i in data.columns:
                for j in data[i]:
                    if type(j) == str:
                        add = 'varchar(255)'
                    elif type(j) == int:
                        add = 'int'
                    elif type(j) == float:
                        add = 'decimal(30,2)'
                    else:
                        add = 'varchar(255)'
                    typeDict[i] = add
            finalString = 'create table ' + name + ' ('
            nums = len(typeDict)
            count = 0
            for key, value in typeDict.items():
                count = count + 1
                finalString = finalString + key + ' ' + value
                if count != nums:
                    finalString = finalString + ', '
            finalString = finalString + ') partition by hash(other) partitions ' + str(k) + ';'
            finalString = finalString.replace('-', '_')
            cur.execute(finalString)
            sString = '('
            count1 = 0
            for i in np.arange(1, nums + 1):
                count1 = count1 + 1
                sString = sString + '%s'
                if count1 != nums:
                    sString = sString + ', '
            sString = sString + ')'
            for i, row in data.iterrows():
                sql = 'insert into ' + name + ' values ' + sString + ';'
                cur.execute(sql, tuple(row))
                db.commit()
            # add to metadata and structure
            id = pd.read_sql('select id from metadata;', db)
            listID = []
            for i in id.values:
                listID.append(i[0])
            maxID = max(listID)
            newID = str(maxID + 1)
            parentID = pd.read_sql('select id from metadata where name = "' + parent + '";', db).values[0][0]
            cur.execute('insert into metadata (id, name) values (' + newID + ', "' + upload + '");')
            cur.execute('insert into structure (parent, child) values (' + str(parentID) + ', ' + newID + ');')
            db.commit()
            # add to partition info
            for i in np.arange(0, int(k)):
                j = (i + 1)
                number = len(data) / int(k)
                ind = round(number * j)
                info = 'all rows with index less than ' + str(ind) + ' located in table ' + name + ';'
                cur.execute('insert into partition_info (id, location) values (' + newID + ', "' + info + '");')
            db.commit()
        elif '.csv' in upload:
            # add table
            data = pd.read_csv(upload, index=False)
            cur.execute('drop table if exists ' + name + ';')
            data['other'] = range(1, len(data) + 1)
            for i in data.columns:
                if i == '':
                    data = data.drop(i, axis=1)
            typeDict = {}
            for i in data.columns:
                for j in data[i]:
                    if type(j) == str:
                        add = 'varchar(255)'
                    elif type(j) == int:
                        add = 'int'
                    elif type(j) == float:
                        add = 'decimal(30,2)'
                    else:
                        add = 'varchar(255)'
                    typeDict[i] = add
            finalString = 'create table ' + name + ' ('
            nums = len(typeDict)
            count = 0
            for key, value in typeDict.items():
                count = count + 1
                finalString = finalString + key + ' ' + value
                if count != nums:
                    finalString = finalString + ', '
            finalString = finalString + ') partition by hash(other) partitions ' + str(k) + ';'
            finalString = finalString.replace('-', '_')
            cur.execute(finalString)
            sString = '('
            count1 = 0
            for i in np.arange(1, nums + 1):
                count1 = count1 + 1
                sString = sString + '%s'
                if count1 != nums:
                    sString = sString + ', '
            sString = sString + ')'
            for i, row in data.iterrows():
                sql = 'insert into ' + name + ' values ' + sString + ';'
                cur.execute(sql, tuple(row))
                db.commit()
            # add to metadata and structure
            id = pd.read_sql('select id from metadata;', db)
            listID = []
            for i in id.values:
                listID.append(i[0])
            maxID = max(listID)
            newID = str(maxID + 1)
            parentID = pd.read_sql('select id from metadata where name = "' + parent + '";', db).values[0][0]
            cur.execute('insert into metadata (id, name) values (' + newID + ', "' + upload + '");')
            cur.execute('insert into structure (parent, child) values (' + str(parentID) + ', ' + newID + ');')
            db.commit()
            # add to partition info
            for i in np.arange(0, int(k)):
                j = (i + 1)
                number = len(data) / int(k)
                ind = round(number * j)
                info = 'all rows with index less than ' + str(ind) + ' located in table ' + name + ';'
                cur.execute('insert into partition_info (id, location) values (' + newID + ', "' + info + '");')
            db.commit()
    elif 'getPartitionLocations' in com[0]:
        input = com[0]
        input1 = input.split('(')
        file = input1[-1]
        file1 = file.split(')')[0]
        table = file1.split('/')
        desire = str(table[-1])
        id1 = pd.read_sql('select id from metadata where name = "' + desire + '";', db)
        id = str(id1.values[0][0])
        info = pd.read_sql('select location from partition_info where id = ' + id + ';', db)
        print(info)
    elif 'readPartition' in com[0]:
        input = com[0]
        input1 = input.split('(')
        file = input1[-1]
        file1 = file.split(')')[0]
        table = file1.split('/')
        desire = str(table[-1])
        items = desire.split(',')
        csv = items[0].strip()
        part = items[1].strip()
        partt = 'p' + str(part)
        id1 = pd.read_sql('select id from metadata where name = "' + csv + '";', db)
        id = id1.values[0][0]
        table = csv.split('.')[0]
        info = pd.read_sql('select * from ' + table + ' partition (' + partt + ');', db)
        print(info)