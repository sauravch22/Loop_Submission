import numpy as np
import pandas as pd
import mysql.connector
import DB_config

#db = mysql.connector.connect(user='root', password='s123a123',database='Loop_Kitchen')
db,cursor = DB_config.get_db_connection()

def db_check(name):
    query = "SELECT * FROM information_schema.tables WHERE table_schema = 'Loop_Kitchen' AND table_name = '"+str(name)+"' LIMIT 1;"
    cursor.execute(query)
    res = cursor.fetchall()
    if len(res)==0:
        return True
    else:
        return False

def DB_create():
    if db_check('Store_status'):
        query1 = "Create Table Store_status(Store_Id varchar(255), Date varchar(255) , Day int , Time varchar(255) , Is_Active varchar(255));"
        cursor.execute(query1)
    if db_check('Open_hours'):
        query2 = "Create Table Open_hours(Store_Id varchar(255), Day int , Open_time varchar(255) , Close_time varchar(255));"
        cursor.execute(query2)
    if db_check('Time_zone'):
        query3 = "Create Table Time_zone(Store_Id varchar(255), Region varchar(255));"
        cursor.execute(query3)

def DB_len(name):
    query = "Select * from "+str(name)+";"
    cursor.execute(query)
    return len(cursor.fetchall())

def DB_insert_store_status():
    data = pd.read_csv('store_status.csv')
    data = np.array(data)
    entry = DB_len('Store_status')
    if entry != len(data):
        for row in data[entry:]:
            Date = str(row[2]).split()[0]
            Time = str(row[2]).split()[1].split(".")[0]
            Day = pd.Timestamp(Date).day_of_week
            query = "INSERT INTO Store_status(Store_Id,Date,Day,Time,Is_Active) VALUES('"+str(row[0])+"','"+Date+"',"+str(Day)+",'"+Time+"','"+str(row[1])+"');"
            cursor.execute(query)
            db.commit()



def DB_insert_time_zone():
    data = pd.read_csv('Time_zone.csv')
    data = np.array(data)
    entry = DB_len('Time_zone')
    if entry != len(data):
        for row in data[entry:]:
            query = "Insert into Time_zone(Store_Id, Region) values ('"+str(row[0])+"','"+str(row[1])+"');"
            cursor.execute(query)
            db.commit()

def DB_insert_open_hrs():    
    data = pd.read_csv('Menu_hours.csv')
    data = np.array(data)
    entry = DB_len('Open_hours')
    if entry != len(data):
        for row in data[entry:]:
            query = "Insert into Open_hours(Store_Id, Day, Open_time, Close_time) values ('"+str(row[0])+"',"+str(row[1])+",'"+str(row[2])+"','"+str(row[3])+"');"
            cursor.execute(query)
            db.commit()

def DB_insert():
    DB_insert_store_status()
    DB_insert_time_zone()
    DB_insert_open_hrs()
