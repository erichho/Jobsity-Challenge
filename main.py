import flask
from flask import request, jsonify,render_template
from flask_wtf import FlaskForm
from wtforms import FileField, SubmitField
from wtforms.validators import InputRequired
from werkzeug.utils import secure_filename
import sqlite3
import pandas as pd
import time
import functions as func
import os
import glob

app = flask.Flask(__name__)
app.config["DEBUG"] = True
####Start process by creating tables in case they dont exist
func.createTables()

########################### Upload Method ###############################
# This method lets the user upload a .csv file with the raw data to be added to the data model
# Test URL: http://127.0.0.1:5000/uploadCSV/
app.config['SECRET_KEY'] = 'supersecretkey'
app.config['UPLOAD_FOLDER'] = 'static/files'
class UploadFileForm(FlaskForm):
    file = FileField("File", validators=[InputRequired()])
    submit = SubmitField("Upload File")
@app.route('/uploadCSV/',methods=['GET','POST'])
def uploadCSV():
    #Empty ./static/files folder so there is always only one .csv file
    files = glob.glob('./static/files/*')
    for f in files:
        os.remove(f)
    form = UploadFileForm()
    if form.validate_on_submit():
        file = form.file.data
        file.save(os.path.join(os.path.abspath(os.path.dirname(__file__)),app.config['UPLOAD_FOLDER'],secure_filename(file.filename))) # Then save the file
        return "File has been uploaded."
    return render_template('uploadCSV.html', form=form)

########################### update Database Method ###############################
# This method reads the trips.csv file and executes the ETL to fill the trips table, groups table, regions table, and datasources table
# Test URL: http://127.0.0.1:5000/updateDatabase/?fileName=trips.csv
@app.route('/updateDatabase/',methods=['GET'])
def updateDatabase():
    filenames = next(os.walk('./static/files/'), (None, None, []))[2]
    # Read trips.csv file uploaded by the user
    df = pd.read_csv('./static/files/'+filenames[0])
    startTime = time.time()
    func.log('Database update process started.','w')
    func.log('File loaded.','a')
    #Clean data
    func.log('Cleaning data','a')
    df[['originLongitud','originLatitud']] = df['origin_coord'].apply(lambda x: x.replace('POINT (','').replace(')','').split(' ')).to_list()
    df[['destinationLongitud','destinationLatitud']] = df['destination_coord'].apply(lambda x: x.replace('POINT (','').replace(')','').split(' ')).to_list()
    #Converting coordinates to float
    df['originLongitud'] = df['originLongitud'].astype(float)
    df['originLatitud'] = df['originLatitud'].astype(float)
    df['destinationLongitud'] = df['destinationLongitud'].astype(float)
    df['destinationLatitud'] = df['destinationLatitud'].astype(float)
    # Remove unnecessary columns
    df = df[['region', 'datetime', 'datasource','originLongitud', 'originLatitud', 'destinationLongitud','destinationLatitud']]
    #Convert to datetime
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S')
    df['timeOfTheDay'] = df['datetime'].dt.hour.apply(lambda x: 'morning' if (x>=4 and x<12) else 'afternoon' if(x>=12 and x<19) else 'night')
    ###################### Update regions table ######################
    conn = func.connectDB()
    cursor = conn.cursor()
    func.log('Connected to trips database.','a')
    regionsTable = pd.read_sql('select region from regions',conn)
    # Get regions and add them to table if they are not in table regions
    newRegions = pd.DataFrame({'region':df['region'].drop_duplicates()})
    newRegions = pd.concat([regionsTable,newRegions]).drop_duplicates(keep=False)
    if(len(newRegions)>0):
        func.insert_with_progress(newRegions,'regions',conn)
    ###################### Grouping trips ######################
    groups = df
    # Round precision of coordinates to 1 decimal which is equivalent 11Km precision
    precision = 1
    groups = groups.round({'originLongitud': precision, 'originLatitud': precision,'destinationLongitud': precision, 'destinationLatitud': precision})
    groups = groups[['originLongitud', 'originLatitud',
           'destinationLongitud', 'destinationLatitud','timeOfTheDay']]
    groups = groups.drop_duplicates( subset =['originLongitud', 'originLatitud',
           'destinationLongitud', 'destinationLatitud','timeOfTheDay'])
    newGroups = pd.DataFrame({'groupSequence':groups['originLongitud'].astype(str)+','+groups['originLatitud'].astype(str)+','+groups['destinationLongitud'].astype(str)+','+groups['destinationLatitud'].astype(str)+','+groups['timeOfTheDay'].astype(str)})

    ###################### Update groups table ######################
    groupsTable = pd.read_sql('select groupSequence from groups',conn)
    newGroups = pd.concat([groupsTable, newGroups]).drop_duplicates(keep=False)
    if(len(newGroups)>0):
        func.insert_with_progress(newGroups,'groups',conn)
    ###################### Update datasources table ######################
    datasourcesTable = pd.read_sql('select datasource from datasources',conn)
    # Get regions and add them to table if they are not in table regions
    newDatasources = pd.DataFrame({'datasource':df['datasource'].drop_duplicates()})
    newDatasources = pd.concat([datasourcesTable,newDatasources]).drop_duplicates(keep=False)
    if(len(newDatasources)>0):
        func.insert_with_progress(newDatasources,'datasources',conn)
    ###################### Prepare trips dataframe ######################
    datasources = pd.read_sql('select datasourceID,datasource from datasources',conn)
    regions = pd.read_sql('select regionID,region from regions',conn)
    groups = pd.read_sql('select groupID,groupSequence from groups',conn)
    trips = pd.merge(df, datasources, on = 'datasource', how = 'left')
    trips = pd.merge(trips, regions, on = 'region', how = 'left')
    trips['groupSequence'] = round(trips['originLongitud'],precision).astype(str)+','+round(trips['originLatitud'],precision).astype(str)+','+round(trips['destinationLongitud'],precision).astype(str)+','+round(trips['destinationLatitud'],precision).astype(str)+','+trips['timeOfTheDay']
    trips = pd.merge(trips, groups, on = 'groupSequence', how = 'left')
    trips = trips[['regionID','groupID','datasourceID',
                   'datetime','originLongitud','originLatitud','destinationLongitud','destinationLatitud']]
    ###################### Update trips table ######################

    func.insert_with_progress(trips,'trips',conn)
    #Calculating processing time
    hours, rem = divmod(time.time() - startTime, 3600)
    minutes, seconds = divmod(rem, 60)
    func.log('Database updated. Processing time: '+'{:0>2}:{:0>2}:{:05.2f}'.format(int(hours),int(minutes),seconds),'a')
    return jsonify('Records on Database',conn.execute('select count(*) from trips').fetchall()[0][0])

########################### databaseStatus Method ###############################
# This method let the user check the progress of the ETL process as it is registered in the logfile.log
# Test URL: http://127.0.0.1:5000/updateDatabaseStatus/
@app.route('/databaseStatus/',methods=['GET'])
def databaseStatus():
    file = open("logfile.log",'r')
    databaseStatus = file.read()
    file.close()
    return databaseStatus.replace('\n', '<br>')

########################### weeklyAverageBox  Method ###############################
# The weeklyAverageBox function calculates the weekly average number of trips for the trips that started
# within a box defined by coordinates lonBox1, latBox1, lonBox2, latBox2
# Test URL(Turin box): http://127.0.0.1:5000/weeklyAverageBox/?lonBox1=7.513035087952872&latBox1=44.976024665620514&lonBox2=7.739760019780325&latBox2=45.13940102848316
@app.route('/weeklyAverageBox/',methods=['GET'])
def weeklyAverageBox():
    lonBox1 = request.args.get('lonBox1')
    latBox1 = request.args.get('latBox1')
    lonBox2 = request.args.get('lonBox2')
    latBox2 = request.args.get('latBox2')
    conn = func.connectDB()
    df = pd.read_sql('select datetime, originLongitud, originLatitud from trips', conn)
    #Filter dataframe with box
    df=df[df.apply(lambda x: func.pointInGeoBox(lonBox1, latBox1, lonBox2, latBox2,x['originLongitud'],x['originLatitud']),axis=1)]
    #Calculate average count of trips per week
    df['datetime-7'] = pd.to_datetime(df['datetime']) - pd.to_timedelta(7, unit='d')
    dic = {'Logitud 1':lonBox1,
           'Latitud 1':latBox1,
           'Logitud 2': lonBox2,
           'Latitud 2': latBox2,
           'Weekly average':str((df.groupby(pd.Grouper(key='datetime-7', axis=0,
                                  freq='W-Mon', sort=True)).count()['datetime'].mean()))
           }
    return jsonify(dic)

########################### weeklyAverageBox  Method ###############################
# The weeklyAverageRegion function calculates the weekly average number of trips for the trips in a Region
# Test URL: http://127.0.0.1:5000/weeklyAverageRegion/?region=Turin
@app.route('/weeklyAverageRegion/',methods=['GET'])
def weeklyAverageRegion():
    region = request.args.get('region')
    conn = func.connectDB()
    df = pd.read_sql('''SELECT datetime,r.region, originLongitud, originLatitud FROM trips t
                 LEFT JOIN regions r
                 ON t.regionID = r.regionID
                 ''', conn)
    #Filter dataframe with box
    df=df[df['region']==region]
    #Calculate average count of trips per week
    df['datetime-7'] = pd.to_datetime(df['datetime']) - pd.to_timedelta(7, unit='d')
    dic = {'Region':region,'Weekly average':str((df.groupby(pd.Grouper(key='datetime-7', axis=0,
                                  freq='W-Mon', sort=True)).count()['datetime'].mean()))}
    return jsonify(dic)
if __name__ == '__main__':
    app.run()