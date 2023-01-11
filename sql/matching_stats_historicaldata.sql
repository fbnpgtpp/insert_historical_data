with cte1 as(select p.projectname , w.wavename , count(g3.id) as nb_null
from gpsextfiles2 g 
join gpsexttracks2 g2 on g.id = g2.fileid 
join gps2 g3 on g2.id = g3.gpsexttrackid
join projects p on g.projectid = p.id 
left join projectwaves p2 on g.projectwaveid = p2.id 
left join waves w on p2.waveid = w.id 
where g3.parcelwaveid is null
group by p.projectname , w.wavename), 

cte2 as(select p.projectname , w.wavename , count(g3.id) as nb_notnull
from gpsextfiles2 g 
join gpsexttracks2 g2 on g.id = g2.fileid 
join gps2 g3 on g2.id = g3.gpsexttrackid
join projects p on g.projectid = p.id 
left join projectwaves p2 on g.projectwaveid = p2.id 
left join waves w on p2.waveid = w.id 
where g3.parcelwaveid is not null
group by p.projectname , w.wavename), 

cte3 as(select p.projectname , w.wavename , count(g3.id) as nb_tot
from gpsextfiles2 g 
join gpsexttracks2 g2 on g.id = g2.fileid 
join gps2 g3 on g2.id = g3.gpsexttrackid
join projects p on g.projectid = p.id 
left join projectwaves p2 on g.projectwaveid = p2.id 
left join waves w on p2.waveid = w.id
group by p.projectname , w.wavename)

select cte1.projectname, cte1.wavename, nb_tot, nb_notnull, (nb_notnull::float8/nb_tot::float8)*100 pc_notnull, nb_null, (nb_null::float8/nb_tot::float8)*100 pc_null
from cte1
join cte2 on cte1.projectname = cte2.projectname and cte1.wavename = cte2.wavename
join cte3 on cte1.projectname = cte3.projectname and cte1.wavename = cte3.wavename