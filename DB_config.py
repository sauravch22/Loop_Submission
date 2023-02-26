import mysql.connector

def get_db_connection():
    db = mysql.connector.connect(user='root', password='s123a123',database='Loop_Kitchen')
    return db,db.cursor()