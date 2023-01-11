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
projectwaves_query = "SELECT * FROM \"HistoricalData\".projectwaves"
parcelwave_query = "SELECT id, registryid, lower(replace(replace(replace(gpsfilename, '_',''),' ',''),'-','')) as gpsfilename FROM \"HistoricalData\".parcelwaves"

gpsextfiles_query = "SELECT filejson FROM \"HistoricalData\".gpsextfiles2 as g1\
                    join \"HistoricalData\".projects p on g1.projectid = p.id"
gpsexttracks_query = "SELECT gpsname, gps FROM\
                    \"HistoricalData\".gpsexttracks2 as g\
                    join \"HistoricalData\".gpsextfiles2 as g1 on g.fileid = g1.id\
                    join \"HistoricalData\".projects p on g1.projectid = p.id"

dirs_query = "select r.id regionid, r.regionname, \
            p.id projectid, p.projectname, \
            s.id subprojectid, s.subprojectname, \
            p2.id projectwaveid, \
            w.id waveid, w.wavename  \
            from \"HistoricalData\".projects p \
            join \"HistoricalData\".subprojects s on p.id = s.projectid \
            join \"HistoricalData\".projectwaves p2 on s.id = p2.subprojectid \
            join \"HistoricalData\".waves w on p2.waveid = w.id \
            join \"HistoricalData\".regions r on p.regionid = r.id"

# Create a gdf from postgis view
df_projectwaves = pd.read_sql(projectwaves_query, dbConnection)
df_parcelwave = pd.read_sql(parcelwave_query, dbConnection)
df_gpsextfiles = pd.read_sql(gpsextfiles_query, dbConnection)
df_gpsexttracks = pd.read_sql(gpsexttracks_query, dbConnection)
df_dirs = pd.read_sql(dirs_query, dbConnection)

print("Tables created")

dirs_to_insert = []
dirs_archives = []
for i in range(0,df_dirs.shape[0]) :
    regionid = int(df_dirs.iloc[i]["regionid"])
    regionname = df_dirs.iloc[i]["regionname"]
    projectid = int(df_dirs.iloc[i]["projectid"])
    projectname = df_dirs.iloc[i]["projectname"]
    subprojectid = int(df_dirs.iloc[i]["subprojectid"])
    subprojectname = df_dirs.iloc[i]["subprojectname"]
    projectwaveid = int(df_dirs.iloc[i]["projectwaveid"])
    waveid = int(df_dirs.iloc[i]["waveid"])
    wavename = df_dirs.iloc[i]["wavename"]

    region_folder = str(regionid) + '_' + regionname
    project_folder = str(projectid) + '_' + projectname
    subproject_folder = str(subprojectid) + '_' + str(subprojectname)
    wave_folder = str(waveid) + '_' + wavename

    newdir_to_insert = os.path.join("tracks","to_insert",region_folder,project_folder,subproject_folder,wave_folder)
    newdir_to_insert_no_wave = os.path.join("tracks","to_insert",region_folder,project_folder,subproject_folder,"X_nowave")
    newdir_archives = os.path.join("tracks","archives",region_folder,project_folder,subproject_folder,wave_folder)
    newdir_archives_no_wave = os.path.join("tracks","archives",region_folder,project_folder,subproject_folder,"X_nowave")

    dirs_to_insert.append(newdir_to_insert)
    dirs_to_insert.append(newdir_to_insert_no_wave)
    dirs_archives.append(newdir_archives)
    dirs_archives.append(newdir_archives_no_wave)

    os.makedirs(newdir_to_insert, exist_ok=True)
    os.makedirs(newdir_to_insert_no_wave, exist_ok=True)
    os.makedirs(newdir_archives, exist_ok=True)
    os.makedirs(newdir_archives_no_wave, exist_ok=True)

#requêtes d'insertion
cur_insert_file = conn.cursor()
cur_insert_track = conn.cursor()
cur_insert_geom = conn.cursor()
q_insert_file_historicaldata = "INSERT INTO \"HistoricalData\".gpsextfiles2(projectid, projectwaveid, name, filejson, dateinsert) VALUES (%s, %s, %s, %s, %s) RETURNING id;"
q_insert_track_historicaldata = "INSERT INTO \"HistoricalData\".gpsexttracks2(fileid, trackid, gpsname, gps) VALUES (%s, %s, %s, %s) RETURNING id;"
q_insert_geom_historicaldata = "INSERT INTO \"HistoricalData\".gps2(parcelwaveid, gpsexttrackid, isgpsodk, geom) VALUES (%s, %s, %s, %s);".replace("'NULL'","NULL")

q_insert_file_prod = "INSERT INTO gpsextfiles(projectid, name, filejson, dateinsert, projectwaveid) VALUES (%s, %s, %s, %s, %s) RETURNING id;"
q_insert_track_prod = "INSERT INTO gpsexttracks(fileid, trackid, gpsname, gps) VALUES (%s, %s, %s, %s) RETURNING id;"
q_insert_geom_prod = "INSERT INTO gps(parcelwaveid, gpsexttrackid, isgpsodk, geom) VALUES (%s, %s, %s, %s);".replace("'NULL'","NULL")


try :
    for dirs in dirs_to_insert :
        list1 = dirs.split("\\")[2:]
        list2 = [x.split('_')[0] for x in list1]

        regionid = list2[0]
        projectid = list2[1]
        subprojectid = list2[2]
        waveid = list2[3]

        if waveid != 'X' :
            projectwaveid = df_projectwaves[(df_projectwaves['subprojectid'] == int(subprojectid))&(df_projectwaves['waveid'] == int(waveid))].iloc[0]["id"]
            projectwaveid = int(projectwaveid)
        else :
            projectwaveid = None
        
        ###################################################################################################################################
        #until here, we've opened a folder, got the ids from region, project, subproject, wave and projectwave to which the folder belongs#
        ###################################################################################################################################

        for files in os.listdir(dirs) :
            print(os.path.join(dirs,files))

            filename = files #FILENAME

            add_date = datetime.date.today() #DATEINSERT

            #print("REGION : ", region, " - PROJECT :", project, " - FILE: ", files)
            gpx_test = files.lower().endswith('.gpx')
            xml_test = files.lower().endswith('.xml')
            kml_test = files.lower().endswith('.kml')
            file = os.path.join(dirs,files)

            if gpx_test :
                #find the right gpx layer
                list_layers = ['routes', 'tracks']
                with fiona.open(file, layer = 'routes') as routes :
                    routes_len = len(list(routes))
                    print(routes_len)

                with fiona.open(file, layer = 'tracks') as tracks :
                    tracks_len = len(list(tracks))
                    print(tracks_len)

                list_size_layers = [routes_len, tracks_len]
                min_value = min(x for x in list_size_layers if not x==0) #Find the minimum value that is not null
                layer_position = list_size_layers.index(min_value) #Get the the position of this minimum
                layer_selected = list_layers[layer_position] #Get the layer that has this minimum value

                #GPX to dataframe
                with fiona.open(file, layer = layer_selected) as geom_file :
                    file_list = list(geom_file)

                print("file is a GPX")

            elif kml_test:
                with fiona.open(file) as geom_file :
                    file_list = list(geom_file)
                
                #print("file is a KML")

            if gpx_test or kml_test :

                file_json = json.dumps(file_list, ensure_ascii=False) #FILEJSON creation to be inserted in DB

                if df_gpsextfiles[df_gpsextfiles.filejson == file_json].empty : #check if GPS file doesn't already exist in DB

                    cur_insert_file.execute(q_insert_file_historicaldata, (projectid, projectwaveid, filename, file_json, add_date)) #insert GPS file in DB

                    id_file = cur_insert_file.fetchone()[0] #FILEID

                    ##########################################################################################################################################################
                    #until here, we've opened a file, check if it's .gpx or .kml, then created a fiona file. Then we've inserted the file info in DB and get back the file id#
                    ##########################################################################################################################################################

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
                                print("REGION : ", regionname, " - PROJECT :", projectname, " - FILE: ", files)
                                print(type(coordinates))
                                print(coordinates)

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

                    ##########################################################################################################################################################
                    #until here, we've transformed the fiona file into a DataFrame, cleaned the data, filter geometry to avoid points geometry, transformed geometry data to #
                    #have a Multilinestring WKT format and created a GeoDataFrame from the DataFrame                                                                         #
                    ##########################################################################################################################################################

                    pbar2 = tqdm(range(0, gdf.shape[0]))   

                    for row in pbar2 :

                        tracknb = row #TRACKID

                        trackname = gdf.iloc[row]["name"]

                        if trackname is None :
                            trackname_check = None
                        else :
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
                                parcelwave=parcelwave.query('registryid == registryid.max()')

                                if parcelwave.empty :
                                    #This part is for Europe mainly because for a lot of files, the gpstracksname is actually the name of the file 
                                    trackname_bis = filename.lower()[:-3].replace('_','').replace(' ','').replace('-','')
                                    parcelwavebis = df_parcelwave[df_parcelwave["gpsfilename"] == trackname_bis]

                                    if parcelwavebis.empty :
                                        parcelwaveid = None
                                    else :
                                        parcelwaveid = parcelwavebis['id'].iloc[0]
                                        parcelwaveid = int(parcelwaveid) #PARCELWAVEID
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
                root_archives = dirs.replace('to_insert','archives')
                print("OK REPLACE ARCHIVE")
                os.makedirs(root_archives, exist_ok=True)
                print("OK MAKE DIR")
                shutil.move(os.path.join(dirs,files), os.path.join(root_archives,files)) #Archivage du fichier
                print(files, " has been moved to archives")
                    
except (Exception, psql.Error) as error :
    print ("Erreur rencontrée : ", error, ' ', ' ', i,files)
    conn.rollback
    print(traceback.format_exc())
finally:
    if(conn):
        conn.close()
        print("Travail terminé")