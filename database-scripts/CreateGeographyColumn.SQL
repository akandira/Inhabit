/* add column of type geographic spatial reference to data table */
SELECT AddGeometryColumn('db_table', 'geom', 4326, 'POINT', 2); 

/*update spatial reference column with values */
UPDATE db_table SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);
 
/*create index to facilitate quick queries */
CREATE INDEX idx ON db_table (latitude,longitude,parameter);