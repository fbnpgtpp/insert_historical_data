with cte1 as (select --max(p3.id),
r.id regionid,
r.regionname,
p.id projectid,
p.projectname ,
s.id subprojectid,
s.subprojectname ,
w.id waveid, 
w.wavename ,
l.localisationname location1,
l2.localisationname location2,
l3.localisationname location3,
o.organisationname,
f.id farmerid,
f.farmername,
f.gender,
f.age,
f.aaa,
p4.id parcelid,
p3.id parcelwaveid,
g2.gpsname,
p3.gpsfilename, 
p5.plantationmodelname plantingmodel,
dlppm.modelname plantingmodelstandardize
from gps2 g3
left join gpsexttracks2 g2 on g3.gpsexttrackid = g2.id
left join gpsextfiles2 g on g2.fileid = g.id
left join projects p on g.projectid = p.id 
left join regions r on p.regionid = r.id 
left join subprojects s on p.id = s.projectid 
left join projectwaves p2 on g.projectwaveid = p2.id 
left join waves w on p2.waveid = w.id
left join parcelwaves p3 on g3.parcelwaveid = p3.id
left join parcels p4 on p3.parcelid = p4.id 
left join farmers f on p4.farmerid = f.id 
left join plantationmodels p5 on p3.plantationmodelid = p5.id 
left join mapping_plantationmodels mp on p5.id = mp.hd_id 
left join db_link_prod_plantation_models dlppm on mp.standard_id  = dlppm.id 
left join localisations l on f.localisationid = l.id 
left join localisations l2 on l.parentid = l2.id
left join localisations l3 on l2.parentid = l3.id
left join organisations o on f.organisationid  = o.id
where p.projectname in ('aceh','cauca y narino', 'coffee for peace','frajianes','la giorgia','pintze','rwenzori','sidama')
group by r.id ,
r.regionname,
p.id ,
p.projectname ,
s.id ,
s.subprojectname ,
w.id , 
w.wavename ,
l.localisationname,
l2.localisationname,
l3.localisationname,
o.organisationname,
f.id,
f.farmername,
f.gender,
f.age,
f.aaa,
g2.gpsname,
p3.gpsfilename ,
p5.plantationmodelname ,
dlppm.modelname,
p4.id,
p3.id,
p3.gpsfilename),

cte2 as(select parcelwaveid, count(parcelwaveid)
from cte1
group by parcelwaveid)

select * 
from cte1 
join cte2 on cte2.parcelwaveid = cte1.parcelwaveid 
where cte2.count > 1;




with cte1 as (select --max(p3.id),
g3.id gpsid,
p3.id parcelwaveid,
g."name" filename,
g2.gpsname,
p3.gpsfilename
from gps2 g3
left join gpsexttracks2 g2 on g3.gpsexttrackid = g2.id
left join gpsextfiles2 g on g2.fileid = g.id
left join projects p on g.projectid = p.id 
left join regions r on p.regionid = r.id
left join projectwaves p2 on g.projectwaveid = p2.id 
left join subprojects s on p2.id = s.projectid 
left join waves w on p2.waveid = w.id
left join parcelwaves p3 on g3.parcelwaveid = p3.id
where p.projectname in ('aceh','cauca y narino', 'coffee for peace','frajianes','la giorgia','pintze','rwenzori','sidama')),

cte2 as(select parcelwaveid, count(parcelwaveid)
from cte1
group by parcelwaveid)

select * from cte1 join cte2 on cte2.parcelwaveid = cte1.parcelwaveid where cte2.count > 1 and cte1.parcelwaveid is not null
