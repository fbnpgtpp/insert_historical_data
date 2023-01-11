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

conn = psql.connect(host="db01.postgres.database.azure.com", dbname="fielddata_dev", user="pp_fabien", password="9N9GNK78TyXQtls6")
conn.set_client_encoding('utf-8')

print("Connected to DB")

query = "select r.id regionid, r.regionname, \
            p.id projectid, p.projectname, \
            s.id subprojectid, s.subprojectname, \
            w.id waveid, w.wavename,  \
            p2.id projectwaveid\
            from \"HistoricalData\".projects p \
            join \"HistoricalData\".subprojects s on p.id = s.projectid \
            join \"HistoricalData\".projectwaves p2 on s.id = p2.subprojectid \
            join \"HistoricalData\".waves w on p2.waveid = w.id \
            join \"HistoricalData\".regions r on p.regionid = r.id"

df_dirs = pd.read_sql(query, conn)

projectwaves_query = "SELECT * FROM \"HistoricalData\".projectwaves"
df_projectwaves = pd.read_sql(projectwaves_query, conn)

print(df_projectwaves)

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

    if newdir_to_insert not in dirs_to_insert:
        dirs_to_insert.append(newdir_to_insert)

    if newdir_to_insert_no_wave not in dirs_to_insert:
        dirs_to_insert.append(newdir_to_insert_no_wave)

    if newdir_archives not in dirs_archives:
        dirs_archives.append(newdir_archives)

    if newdir_archives_no_wave not in dirs_archives:
        dirs_archives.append(newdir_archives_no_wave)

    os.makedirs(newdir_to_insert, exist_ok=True)
    os.makedirs(newdir_to_insert_no_wave, exist_ok=True)
    os.makedirs(newdir_archives, exist_ok=True)
    os.makedirs(newdir_archives_no_wave, exist_ok=True)

for dirs in dirs_archives :
        for files in os.listdir(dirs) :
            file = os.path.join(dirs,files)
            root_to_insert = dirs.replace('archives','to_insert')
            os.makedirs(root_to_insert, exist_ok=True)
            shutil.move(os.path.join(dirs,files), os.path.join(root_to_insert,files)) #Archivage du fichier
            print(files, " has been moved to archives")

