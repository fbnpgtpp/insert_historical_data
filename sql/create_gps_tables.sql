CREATE TABLE "HistoricalData".gpsextfiles2 (
	id bigserial NOT null,
	name varchar(200) NULL,
	projectid int4 NULL,
	projectwaveid int4 null,
	filejson varchar NULL,
	dateinsert date null,
	CONSTRAINT gpsextfiles2_pkey PRIMARY KEY (id)
);

ALTER TABLE "HistoricalData".gpsextfiles2 ADD CONSTRAINT projectid_fkey FOREIGN KEY (projectid) REFERENCES "HistoricalData".projects(id);
ALTER TABLE "HistoricalData".gpsextfiles2 ADD CONSTRAINT projectwaveid_fkey FOREIGN KEY (projectwaveid) REFERENCES "HistoricalData".projectwaves(id);

CREATE TABLE "HistoricalData".gpsexttracks2 (
	id bigserial NOT NULL,
	fileid int4 NULL,
	trackid int4 NULL,
	gpsname varchar(200) NULL,
	gps varchar null,
	CONSTRAINT gpsexttracks2_pkey PRIMARY KEY (id)
);

ALTER TABLE "HistoricalData".gpsexttracks2 ADD CONSTRAINT fileid_fkey FOREIGN KEY (fileid) REFERENCES "HistoricalData".gpsextfiles2(id);

CREATE TABLE "HistoricalData".gps2 (
	id bigserial NOT NULL,
	parcelwaveid int4 NULL,
	gpsexttrackid int4 NULL,
	isgpsodk bool NULL,
	geom public.geometry null,
	CONSTRAINT gps2_pkey PRIMARY KEY (id)
);


-- "HistoricalData".gps foreign keys

ALTER TABLE "HistoricalData".gps2 ADD CONSTRAINT parcelwaveid_fkey FOREIGN KEY (parcelwaveid) REFERENCES "HistoricalData".parcelwaves(id);