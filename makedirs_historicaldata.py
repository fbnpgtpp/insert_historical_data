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

for i in range(0,df_dirs.shape[0]) :
    region = str(df_dirs.iloc[i]["regionid"]) + "_" + str(df_dirs.iloc[i]["regionname"])
    project = str(df_dirs.iloc[i]["projectid"]) + "_" + str(df_dirs.iloc[i]["projectname"])
    subproject = str(df_dirs.iloc[i]["subprojectid"]) + "_" + str(df_dirs.iloc[i]["subprojectname"])
    wave = str(df_dirs.iloc[i]["waveid"]) + "_" + str(df_dirs.iloc[i]["wavename"])

    newdir_to_insert = f"C:\Users\Fabien\Documents\GitHub\insert_ext_tracks\\tracks\\to_insert\\{region}\\{project}\\{subproject}\\{wave}"
    newdir_archives = f"C:\Users\Fabien\Documents\GitHub\insert_ext_tracks\\tracks\\archives\\{region}\\{project}\\{subproject}\\{wave}"

    os.makedirs(newdir_to_insert, exist_ok=True)
    os.makedirs(newdir_archives, exist_ok=True)
