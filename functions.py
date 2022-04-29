import pandas as pd
import sqlite3
from datetime import datetime
# This functino will use the createTable.sql and create all tables needed for the model
def createTables():
    createTables = open('createTables.sql', 'r').read()
    conn = sqlite3.connect('trip.db')
    cursor = conn.cursor()
    cursor.executescript(createTables)
# Function to drop all tables in trip.db
def deleteTables():
    conn = sqlite3.connect('trip.db')
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS regions')
    cursor.execute('DROP TABLE IF EXISTS groups')
    cursor.execute('DROP TABLE IF EXISTS datasources')
    cursor.execute('DROP TABLE IF EXISTS trips')
# The connectDB connects to the trip.db and creates it incase it doesnt exist already

# This function facilitates the connection to the trip.db model
def connectDB():
    # Create or Update database
    conn = sqlite3.connect('trip.db')
    create_trip_table = """
        CREATE TABLE IF NOT EXISTS trips (
        region TEXT, 
        datetime TEXT, 
        dataSource TEXT, 
        originLongitud TEXT, 
        originLatitud TEXT, 
        destinationLongitud TEXT, 
        destinationLatitud TEXT, 
        cluster TEXT)
    """
    cursor = conn.cursor()
    # Create trips table if it doesn't exist
    cursor.execute(create_trip_table)
    return conn


# The pointInGeoBox function receives 3 pairs of coordinates.
# It checks if the 3rd coordinate is inside the box made by the first two.
# Testcase: pointInGeoBox(2,2,-1,-1,1.5,1.5)
def pointInGeoBox(lonBox1, latBox1, lonBox2, latBox2, pointLon, pointLat):
    if (pointLon >= min([lonBox1, lonBox2]) and pointLon <= max([lonBox1, lonBox2])):
        if (pointLat >= min([latBox1, latBox2]) and pointLat <= max([latBox1, latBox2])):
            return True
    return False

# The weeklyAverage function calculates the weekly average count of trips for a given dataframe
def weeklyAverage(df):
    df['datetime-7'] = pd.to_datetime(df['datetime']) - pd.to_timedelta(7, unit='d')
    return (df.groupby(pd.Grouper(key='datetime-7', axis=0,
                                  freq='W-Mon', sort=True)).count()['region'].mean())



# The chuncker function spits a dataframe to be loaded in chunks
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

# The insert_with_progress function helps inserting the data into the SQLite database
# while updating the status of the ETL process on the databaseStatus global variable

def insert_with_progress(df,table,conn):
    if(len(df)<10):
        chunksize=len(df)
    else:
        chunksize = int(len(df) / 10)
    for i, cdf in enumerate(chunker(df, chunksize)):
        replace = "replace" if i == 0 else "append"
        cdf.to_sql(name=table, con=conn, if_exists="append", index=False)
        if(i*10<100):
            log('Updating table "'+table+'": '+str(i*10)+'%','a')
    log('Updating table "'+table+'": '+'100'+'%','a')
    log('"'+table+'" table update completed.','a')

# The log function writes comments on logfile.log to keep track of the ETL process and let the user check through the databaseStatus method
def log(message,mode):
    file = open("logfile.log",mode)
    file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+' - '+message +'\n')
    file.close()
