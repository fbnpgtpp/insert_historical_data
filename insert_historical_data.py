import geopandas as gpd
import pandas as pd
import fiona
from shapely.geometry import shape
from shapely.geometry import MultiLineString
from shapely import wkt
import shapely
import os
import datetime
import json
from sqlalchemy import create_engine
from time import sleep
from tqdm import tqdm
import psycopg2 as psql
from unidecode import unidecode
import shutil
import traceback

#gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'
fiona.drvsupport.supported_drivers['kml'] = 'rw'  # enable KML support which is disabled by default
fiona.drvsupport.supported_drivers['KML'] = 'rw'  # enable KML support which is disabled by default
fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'  # enable KML support which is disabled by default

# Create SQLAlchemy engine
engine = create_engine('postgresql+psycopg2://pp_fabien:9N9GNK78TyXQtls6@db01.postgres.database.azure.com/fielddata_dev', client_encoding='utf8') #add client_encoding='utf8'

# Connect to PostgreSQL server
dbConnection    = engine.connect()

# Connect to PostgreSQL server with psql
conn = psql.connect(host="db01.postgres.database.azure.com", dbname="fielddata_dev", user="pp_fabien", password="9N9GNK78TyXQtls6")
conn.set_client_encoding('utf-8')

print("Connected to DB")

# Define SQL Query
project_query = "SELECT id, regionid, lower(projectname) as projectname, projectdevname FROM \"HistoricalData\".projects"
parcelwave_query = "SELECT id, lower(replace(replace(replace(gpsfilename, '_',''),' ',''),'-','')) as gpsfilename FROM \"HistoricalData\".parcelwaves"

gpsextfiles_query = "SELECT filejson FROM \"HistoricalData\".gpsextfiles as g1\
                    join \"HistoricalData\".projects p on g1.projectid = p.id"
gpsexttracks_query = "SELECT gpsname, gps FROM\
                    \"HistoricalData\".gpsexttracks as g\
                    join \"HistoricalData\".gpsextfiles as g1 on g.fileid = g1.id\
                    join \"HistoricalData\".projects p on g1.projectid = p.id"

# Create a gdf from postgis view
df_project = pd.read_sql(project_query, dbConnection)
df_parcelwave = pd.read_sql(parcelwave_query, dbConnection)
df_gpsextfiles = pd.read_sql(gpsextfiles_query, dbConnection)
df_gpsexttracks = pd.read_sql(gpsexttracks_query, dbConnection)

print("Tables created")

#define historicaldata projects list
dictprojet_historicaldata = {
        "africa" : ["diana","ivory coast","rwenzori","sidama"],
        "asia & pacific" : ["aceh","alter trade","darjeeling","kbqb"],
        "europe" : ["espana organica","mihai eminescu trust"],
        "latin america" : ["aprosacao","frajianes","la giorgia","pintze","jubilacion segura","alto huayabamba","cfp","cauca y narino"]
    }

#requêtes d'insertion
cur_insert_file = conn.cursor()
cur_insert_track = conn.cursor()
cur_insert_geom = conn.cursor()
q_insert_file_historicaldata = "INSERT INTO \"HistoricalData\".gpsextfiles(projectid, name, filejson, dateinsert) VALUES (%s, %s, %s, %s) RETURNING id;"
q_insert_track_historicaldata = "INSERT INTO \"HistoricalData\".gpsexttracks(fileid, trackid, gpsname, gps) VALUES (%s, %s, %s, %s) RETURNING id;"
q_insert_geom_historicaldata = "INSERT INTO \"HistoricalData\".gps(parcelwaveid, gpsexttrackid, isgpsodk, geom) VALUES (%s, %s, %s, %s);".replace("'NULL'","NULL")

q_insert_file_prod = "INSERT INTO gpsextfiles(projectid, name, filejson, dateinsert) VALUES (%s, %s, %s, %s) RETURNING id;"
q_insert_track_prod = "INSERT INTO gpsexttracks(fileid, trackid, gpsname, gps) VALUES (%s, %s, %s, %s) RETURNING id;"
q_insert_geom_prod = "INSERT INTO gps(parcelwaveid, gpsexttrackid, isgpsodk, geom) VALUES (%s, %s, %s, %s);".replace("'NULL'","NULL")

try :
    for region in list(dictprojet_historicaldata) :
        for project in dictprojet_historicaldata[region] :
            dir_historicaldata = "C:/Users/pnjoya/Desktop/01_IT Data/historical_data/tracks/"
            dir_archives_historicaldata = "C:/Users/pnjoya/Desktop/01_IT Data/historical_data/tracks_archives/"
            dir_historicaldata = dir_historicaldata+region+'/'+project+'/'
            
            print(project)

            projectid = df_project[df_project.projectname == project]["id"].iloc[0] #PROJECTID
            
            projectid = int(projectid) #from numpy.int64 to int64

            for root, dirs, files in os.walk(dir_historicaldata) :
                pbar = tqdm(os.listdir(root))
                for files in pbar :
                    print(root + files)
                    pbar.set_description(f'Processing {region}-{project}-{files}')

                    filename = files #FILENAME

                    add_date = datetime.date.today() #DATEINSERT

                    #print("REGION : ", region, " - PROJECT :", project, " - FILE: ", files)
                    gpx_test = files.lower().endswith('.gpx')
                    xml_test = files.lower().endswith('.xml')
                    kml_test = files.lower().endswith('.kml')
                    file = root + files

                    if gpx_test :
                        #find the right gpx layer
                        list_layers = ['routes', 'tracks']
                        list_size_layers = list(len(list(fiona.open(file, layer = x))) for x in list_layers) #Get the number of elements of each layers
                        min_value = min(x for x in list_size_layers if not x==0) #Find the minimum value that is not null
                        layer_position = list_size_layers.index(min_value) #Get the the position of this minimum
                        layer_selected = list_layers[layer_position] #Get the layer that has this minimum value

                        #GPX to dataframe
                        geom_file = fiona.open(file, layer = layer_selected)
                        print("file is a GPX")

                    elif kml_test:
                        
                        geom_file = fiona.open(file)
                        print("file is a KML")

                    if gpx_test or kml_test :
                        print("ICI")

                        file_list = list(geom_file)

                        file_json = json.dumps(list(geom_file), ensure_ascii=False) #FILEJSON

                        if df_gpsextfiles[df_gpsextfiles.filejson == file_json].empty : #check if GPS file doesn't already exist in DB

                            cur_insert_file.execute(q_insert_file_historicaldata, (projectid, filename, file_json, add_date)) #insert GPS file in DB

                            id_file = cur_insert_file.fetchone()[0] #FILEID

                            #Create dataframe containing geometry data
                            for i in file_list :
                                geom = [[]]
                                for coordinates in i["geometry"]["coordinates"]:
                                    if isinstance(coordinates, list):
                                        for points in coordinates:
                                            if isinstance(points, list):
                                                for point in points:
                                                    latlong = point[:2]
                                                    geom[0].append(latlong)
                                            else :
                                                latlong = points[:2]
                                                geom[0].append(latlong)
                                    elif isinstance(coordinates, tuple):
                                        latlong = coordinates[:2]
                                        geom[0].append(latlong)
                                    else :
                                        print("probleme here")
                                        print("REGION : ", region, " - PROJECT :", project, " - FILE: ", files)
                                        print(type(y))
                                        print(i)

                                i["geometry"]["coordinates"] = geom
                                i["geometry"]["type"] = 'MultiLineString'
                            
                            

                            df = pd.DataFrame(file_list)

                            #Clean DataFrame
                            keys = list(df.properties[0].keys())
                            keys = [x.lower() for x in keys]
                            values = list(df.properties[0].values())

                            df[keys] = pd.DataFrame(df.properties.tolist(), index = df.index)


                            df['num_points'] = df['geometry'].apply(lambda x: len(x['coordinates'][0])) #Get number of points per tracks

                            df_numpoints = df.copy()[df.num_points >= 2] #Filter to have only tracks with more (or equal) than 2 points

                            df_numpoints.geometry = df_numpoints.geometry.apply(lambda x : shape(x)) #Transform geometry dictionnary into a shapely object

                            df_numpoints = df_numpoints[["id","geometry","name"]] #Select useful columns only

                            #DataFrame to GeoDataFrame
                            gdf = gpd.GeoDataFrame(df_numpoints) #Transform dataFrame intot a geoDataFrame
                            
                            pbar2 = tqdm(range(0, gdf.shape[0]))
                            for files in pbar :
                                pbar.set_description(f'Processing {region}-{project}-{files}')
                                
                            for row in pbar2 :

                                tracknb = row #TRACKID

                                trackname = gdf.iloc[row]["name"] 
                                trackname_check = unidecode(trackname) #TRACKNAME
                                
                                pbar2.set_description(f'Processing {tracknb}-{trackname}')


                                gps = str(gdf.iloc[row]["geometry"]) #GEOM
                                if df_gpsexttracks[(df_gpsexttracks.gpsname == trackname_check)&(df_gpsexttracks.gps == gps)].empty :

                                    cur_insert_track.execute(q_insert_track_historicaldata, (id_file, tracknb, trackname_check.replace("'",""), gps))

                                    gpsextractid = cur_insert_track.fetchone()[0] ###GPS - gpsextrractid###
                                    gpsextractid = int(gpsextractid)
                                    isgpsodk = False

                                    trackname_clean = trackname_check.lower().replace('_','').replace(' ','').replace('-','')
                                    parcelwave = df_parcelwave[df_parcelwave["gpsfilename"] == trackname_clean] 
                                    if parcelwave.empty :
                                        parcelwaveid = None
                                    else :
                                        parcelwaveid = parcelwave['id'].iloc[0]
                                        parcelwaveid = int(parcelwaveid) #PARCELWAVEID

                                    cur_insert_geom.execute(q_insert_geom_historicaldata, (parcelwaveid, gpsextractid, isgpsodk, gps))
                                else :
                                    print("tracks already in DB")
                                    conn.rollback()
                        else :
                            print("File already in DB")
                            conn.rollback()

                        conn.commit()
                        print("OK COMMIT")
                        geom_file.close()
                        print(geom_file.closed)
                        print("OK CLOSE")
                        root_archives = root.replace('tracks','tracks_archives')
                        print("OK REPLACE ARCHIVE")
                        os.makedirs(root_archives, exist_ok=True)
                        print("OK MAKE DIR")
                        shutil.move(root + '/' + files, root_archives+files) #Archivage du fichier
                        print(files, " has been moved to archives")
                    
except (Exception, psql.Error) as error :
    print ("Erreur rencontrée : ", error, ' ', ' ', root,files)
    conn.rollback
    print(traceback.format_exc())
finally:
    if(conn):
        conn.close()
        print("Travail terminé")