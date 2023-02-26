import pandas as pd
import numpy as np
import mysql.connector
from datetime import datetime
from dateutil import tz
from datetime import timezone
import DB_config

#db = mysql.connector.connect(user='root', password='s123a123',database='Loop_Kitchen')
db,cursor = DB_config.get_db_connection()

def current_hrs_stat(store_id):
    hrs_uptime,hrs_downtime = 0,0
    current_hrs = str(datetime.now(timezone.utc)).split(" ")[1].split(".")[0]
    current_hrs = current_hrs.split(":")
    from_hrs = str(int(current_hrs[0])-2)+":"+current_hrs[1]+":"+current_hrs[2]
    to_hrs = str(int(current_hrs[0])+2)+":"+current_hrs[1]+":"+current_hrs[2]
    query = "Select * From Store_status where Store_Id='"+str(store_id)+"' and Is_Active = 'active' and Time between '"+str(from_hrs)+"' and '"+str(to_hrs)+"';" 
    cursor.execute(query)
    data_hrs = len(cursor.fetchall())
    if data_hrs >0:
            hrs_uptime = 100
            hrs_downtime = 0
    else:
            hrs_downtime = 0
            hrs_uptime = 100
    return hrs_uptime, hrs_downtime

def current_week_day_stat(store_id,current_date,Day):
      day_uptime,day_downtime,week_uptime,week_downtime = 0,0,0,0
      query = "Select Region from Time_zone where Store_Id ='"+str(store_id)+"';"
      cursor.execute(query)
      data = cursor.fetchall()
      if(len(data) > 0):
            data = data[0]
            to_zone = tz.gettz(str(data[0]))
      else:
            to_zone = tz.gettz('America/Chicago')
      from_zone = tz.gettz('UTC')
      for day_of_week in range(0,7):
            date_split = current_date.split("-")
            current_date_query = date_split[0]+"-"+date_split[1]+"-"+str(int(date_split[2])-day_of_week)
            query = "Select Open_time, Close_time from Open_hours where Store_Id ='"+str(store_id)+"' and Day ="+str(day_of_week)+";"
            cursor.execute(query)
            data = cursor.fetchall()
            if len(data) == 0:
                  data = [('00:00:00','23:59:59')]
            for dt in data:
                time = datetime.strptime(str(dt[0]),'%H:%M:%S')
                time = time.replace(tzinfo=to_zone)
                start_time = str(time.astimezone(from_zone)).split(" ")[1].split("+")[0]
                time = datetime.strptime(str(dt[1]),'%H:%M:%S')
                time = time.replace(tzinfo=to_zone)
                end_time = str(time.astimezone(from_zone)).split(" ")[1].split("+")[0]
                tot_probe, tot_active,percentage = 0,0,0
                if end_time<start_time:
                    t1 = '23:59:59'
                    t2 = '00:00:00'
                    query1 = "Select * from Store_status where Store_id ='"+str(store_id)+"' and Date = '"+str(current_date_query)+"' and Day ="+str(day_of_week)+" and Time between'"+str(start_time)+"' and '"+str(t1)+"';"
                    query2 = "Select * from Store_status where Store_id ='"+str(store_id)+"' and Date = '"+str(current_date_query)+"' and Day ="+str(day_of_week)+" and Time between'"+str(t2)+"' and '"+str(end_time)+"';"
                    cursor.execute(query1)
                    tot_probe += len(cursor.fetchall())
                    cursor.execute(query2)
                    tot_probe += len(cursor.fetchall())
                    query1 = "Select * from Store_status where Store_id ='"+str(store_id)+"' and Date = '"+str(current_date_query)+"' and Day ="+str(day_of_week)+" and Is_Active = 'active' and Time between'"+str(start_time)+"' and '"+str(t1)+"';"
                    query2 = "Select * from Store_status where Store_id ='"+str(store_id)+"' and Date = '"+str(current_date_query)+"' and Day ="+str(day_of_week)+" and Is_Active = 'active' and Time between'"+str(t2)+"' and '"+str(end_time)+"';"
                    cursor.execute(query1)
                    tot_active += len(cursor.fetchall())
                    cursor.execute(query2)
                    tot_active += len(cursor.fetchall())
                else:      
                    query = "Select * from Store_status where Store_id ='"+str(store_id)+"' and Date = '"+str(current_date_query)+"' and Day ="+str(day_of_week)+" and Time between'"+str(start_time)+"' and '"+str(end_time)+"';"
                    cursor.execute(query)
                    tot_probe = len(cursor.fetchall())
                    query = "Select * from Store_status where Store_id ='"+str(store_id)+"' and Date = '"+str(current_date_query)+"' and Day ="+str(day_of_week)+" and Is_Active = 'active' and Time between'"+str(start_time)+"' and '"+str(end_time)+"';"
                    cursor.execute(query)
                    tot_active = len(cursor.fetchall())
                
                if tot_probe == 0:
                     percentage = 0
                else:
                    percentage = tot_active/tot_probe
                if day_of_week == Day:
                    day_uptime += percentage
                    day_downtime += 1-percentage
                week_uptime += percentage
                week_downtime += (1-percentage) 
      week_uptime = week_uptime/7
      week_downtime = week_downtime/7
      week_uptime = week_uptime*100
      week_downtime = week_downtime*100
      day_uptime = day_uptime*100
      day_downtime = day_downtime*100
      return day_uptime, day_downtime, week_uptime, week_downtime

def Store_uptime_downtime(store_list,current_date,Day,report_id):
    Report_list = []
    count = 0
    for store_id in store_list:
        hrs_uptime, hrs_downtime, day_uptime , day_down_time, week_uptime , week_downtime = 0,0,0,0,0,0
        hrs_uptime , hrs_downtime = current_hrs_stat(store_id)
        day_uptime , day_down_time, week_uptime , week_downtime = current_week_day_stat(store_id,current_date,Day)
        Report_list.append([store_id,hrs_uptime,day_uptime,week_uptime,hrs_downtime,day_down_time,week_downtime])
        count += 1
        #print(count)
    Report_list = np.array(Report_list)
    pd.DataFrame(Report_list).to_csv(str(report_id)+'.csv',header=['Store_Id','Hrs_Uptime','Days_Uptime','Week_Uptime','Hrs_downtime','Days_downtime','Week_downtime'],index=False)
        
def query_one_store(store_id,current_date,Day):
     Report_list = []
     hrs_uptime , hrs_downtime = current_hrs_stat(store_id)
     day_uptime , day_down_time, week_uptime , week_downtime = current_week_day_stat(store_id,current_date,Day)
     Report_list.append([store_id,hrs_uptime,day_uptime,week_uptime,hrs_downtime,day_down_time,week_downtime])
     Report_list = np.array(Report_list)
     pd.DataFrame(Report_list).to_csv(str(store_id)+'.csv',header=['Store_Id','Hrs_Uptime','Days_Uptime','Week_Uptime','Hrs_downtime','Days_downtime','Week_downtime'],index=False)
