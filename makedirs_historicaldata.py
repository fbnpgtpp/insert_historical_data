import os
import psycopg2 as psql
import pandas as pd



conn = psql.connect(host="db01.postgres.database.azure.com", dbname="fielddata_dev", user="pp_fabien", password="9N9GNK78TyXQtls6")
conn.set_client_encoding('utf-8')

print("Connected to DB")

query = "select r.id regionid, r.regionname, \
            p.id projectid, p.projectname, \
            s.id subprojectid, s.subprojectname, \
            w.id waveid, w.wavename  \
            from \"HistoricalData\".projects p \
            join \"HistoricalData\".subprojects s on p.id = s.projectid \
            join \"HistoricalData\".projectwaves p2 on s.id = p2.subprojectid \
            join \"HistoricalData\".waves w on p2.waveid = w.id \
            join \"HistoricalData\".regions r on p.regionid = r.id"

df_dirs = pd.read_sql(query, conn)

print(df_dirs)

'''
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
    subproject_folder = str(subprojectid) + '_' + subprojectname
    wave_folder = str(waveid) + '_' + wavename

    newdir_to_insert = f"C:\Users\Fabien\Documents\GitHub\insert_ext_tracks\\tracks\\to_insert\\{region_folder}\\{project_folder}\\{subproject_folder}\\{wave_folder}"
    newdir_to_insert_no_wave = f"C:\Users\Fabien\Documents\GitHub\insert_ext_tracks\\tracks\\to_insert\\{region_folder}\\{project_folder}\\{subproject_folder}\\X_nowave"
    newdir_archives = f"C:\Users\Fabien\Documents\GitHub\insert_ext_tracks\\tracks\\archives\\{region_folder}\\{project_folder}\\{subproject_folder}\\{wave_folder}"
    newdir_archives_no_wave = f"C:\Users\Fabien\Documents\GitHub\insert_ext_tracks\\tracks\\archives\\{region_folder}\\{project_folder}\\{subproject_folder}\\X_nowave"

    dirs_to_insert.append(newdir_to_insert)
    dirs_to_insert.append(newdir_to_insert_no_wave)
    dirs_archives.append(newdir_archives)
    dirs_archives.append(newdir_archives_no_wave)

    os.makedirs(newdir_to_insert, exist_ok=True)
    os.makedirs(newdir_to_insert_no_wave, exist_ok=True)
    os.makedirs(newdir_archives, exist_ok=True)
    os.makedirs(newdir_archives_no_wave, exist_ok=True)

for i in dirs_to_insert :
        
        pbar = tqdm(os.listdir(i))

        for files in pbar :
            print(i + files)
            pbar.set_description(f'Processing {regionname}-{projectname}-{wavename}-{files}')
'''
