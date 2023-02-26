import threading
import time
import string
import random
import pandas as pd
import os.path
import DB_operation
import Querry_operation

current_date = '2023-01-27'

def Unique_store_id():
    store_l1 = pd.read_csv('store_status.csv')['store_id']
    store_l2 = pd.read_csv('Menu_hours.csv')['store_id']
    store_l3 = pd.read_csv('Time_zone.csv')['store_id']
    store_list = list(set(store_l1) | set(store_l2) | set(store_l3))
    return store_list

def start_report_generator(report_id):
    DB_operation.DB_create()
    DB_operation.DB_insert()
    store_list = Unique_store_id()
    Day = pd.Timestamp(current_date).day_of_week
    #print('starting querry total no of '," ",len(store_list))
    Querry_operation.Store_uptime_downtime(store_list,current_date,Day,report_id)
    
def generate_report_for_particular(store_id):
    DB_operation.DB_create()
    DB_operation.DB_insert()
    Day = pd.Timestamp(current_date).day_of_week
    Querry_operation.query_one_store(store_id,current_date,Day)
    print("Report for ", store_id ," generation completed successfully and saved in this location ", os.getcwd())

def Report_init():
    report_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=7))
    t1 = threading.Thread(target=start_report_generator, args=(report_id,),daemon=True)
    t1.start()
    return report_id

def Report_Display(report_id):
    path = str(os.getcwd()) +"/"+ report_id +".csv"
    if os.path.isfile(path):
        print("Report for ", report_id ," generation completed successfully and saved in this location ", os.getcwd())
    else:
        print("Report generation is in progress for report id", report_id," please try again later")

def exit_kitchen():
    return False
