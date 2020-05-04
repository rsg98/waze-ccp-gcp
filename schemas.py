'''Copyright 2020 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.'''

#BigQuery Schemas for the three tables that need to be recreated.
#These are also referenced with each write.
from google.cloud import bigquery

jamsSchema = [
 			bigquery.SchemaField('city','STRING',mode='Nullable'),
 			bigquery.SchemaField('turntype','STRING',mode='Nullable'),
 			bigquery.SchemaField('level','INT64',mode='Nullable'),
 			bigquery.SchemaField('country','STRING',mode='Nullable'),
 			bigquery.SchemaField('speedKMH','FLOAT64',mode='Nullable'),
 			bigquery.SchemaField('delay','INT64',mode='Nullable'),
 			bigquery.SchemaField('length','INT64',mode='Nullable'),
 			bigquery.SchemaField('street','STRING',mode='Nullable'),
 			bigquery.SchemaField('ms','INT64',mode='Nullable'),
 			bigquery.SchemaField('ts','TIMESTAMP',mode='Nullable'),
 			bigquery.SchemaField('endNode','STRING',mode='Nullable'),
 			bigquery.SchemaField('geo','GEOGRAPHY',mode='Nullable'),
 			bigquery.SchemaField('geoWKT','STRING',mode='Nullable'),
 			bigquery.SchemaField('type','STRING',mode='Nullable'),
 			bigquery.SchemaField('id','INT64',mode='Nullable'),
 			bigquery.SchemaField('speed','FLOAT64',mode='Nullable'),
 			bigquery.SchemaField('uuid','STRING',mode='Nullable'),
 			bigquery.SchemaField('startNode','STRING',mode='Nullable'),
 		]


alertsSchema = [
 			bigquery.SchemaField('city','STRING',mode='Nullable'),
 			bigquery.SchemaField('confidence','INT64',mode='Nullable'),
 			bigquery.SchemaField('nThumbsUp','INT64',mode='Nullable'),
 			bigquery.SchemaField('street','STRING',mode='Nullable'),
 			bigquery.SchemaField('uuid','STRING',mode='Nullable'),
 			bigquery.SchemaField('country','STRING',mode='Nullable'),
 			bigquery.SchemaField('type','STRING',mode='Nullable'),
 			bigquery.SchemaField('subtype','STRING',mode='Nullable'),
 			bigquery.SchemaField('roadType','INT64',mode='Nullable'),
 			bigquery.SchemaField('reliability','INT64',mode='Nullable'),
 			bigquery.SchemaField('magvar','INT64',mode='Nullable'),
 			bigquery.SchemaField('reportRating','INT64',mode='Nullable'),
 			bigquery.SchemaField('ms','INT64',mode='Nullable'),
 			bigquery.SchemaField('ts','TIMESTAMP',mode='Nullable'),
 			bigquery.SchemaField('reportDescription','STRING',mode='Nullable'),
 			bigquery.SchemaField('geo','GEOGRAPHY',mode='Nullable'),
 			bigquery.SchemaField('geoWKT','STRING',mode='Nullable')
 		]
irregularitiesSchema =[
 			bigquery.SchemaField('trend','INT64',mode='Nullable'),
 			bigquery.SchemaField('street','STRING',mode='Nullable'),
 			bigquery.SchemaField('endNode','STRING',mode='Nullable'),
 			bigquery.SchemaField('nImages','INT64',mode='Nullable'),
 			bigquery.SchemaField('speed','FLOAT64',mode='Nullable'),
 			bigquery.SchemaField('id','STRING',mode='Nullable'),
 			bigquery.SchemaField('severity','INT64',mode='Nullable'),
 			bigquery.SchemaField('type','STRING',mode='Nullable'),
 			bigquery.SchemaField('highway','BOOL',mode='Nullable'),
 			bigquery.SchemaField('nThumbsUp','INT64',mode='Nullable'),
 			bigquery.SchemaField('seconds','INT64',mode='Nullable'),
 			bigquery.SchemaField('alertsCount','INT64',mode='Nullable'),
 			bigquery.SchemaField('detectionDateMS','INT64',mode='Nullable'),
 			bigquery.SchemaField('detectionDateTS','TIMESTAMP',mode='Nullable'),
 			bigquery.SchemaField('driversCount','INT64',mode='Nullable'),
 			bigquery.SchemaField('geo','GEOGRAPHY',mode='Nullable'),
 			bigquery.SchemaField('geoWKT','STRING',mode='Nullable'),
 			bigquery.SchemaField('startNode','STRING',mode='Nullable'),
 			bigquery.SchemaField('updateDateMS','INT64',mode='Nullable'),
 			bigquery.SchemaField('updateDateTS','TIMESTAMP',mode='Nullable'),
 			bigquery.SchemaField('regularSpeed','FLOAT64',mode='Nullable'),
 			bigquery.SchemaField('country','STRING',mode='Nullable'),
 			bigquery.SchemaField('length','INT64',mode='Nullable'),
 			bigquery.SchemaField('delaySeconds','INT64',mode='Nullable'),
 			bigquery.SchemaField('jamLevel','INT64',mode='Nullable'),
 			bigquery.SchemaField('nComments','INT64',mode='Nullable'),
 			bigquery.SchemaField('city','STRING',mode='Nullable'),
 			bigquery.SchemaField('causeType','STRING',mode='Nullable'),
 			bigquery.SchemaField('causeAlertUUID','STRING',mode='Nullable'),

 		]

#cartoSQL Scehmas and Values Strings

cartoAlertsSchema = "(city text,\
	confidence int,\
	nThumbsUp int,\
	street text,\
	uuid text,\
	country text,\
	type text,\
	subtype text,\
	roadType int,\
	reliability int,\
	magvar int,\
	reportRating int,\
	ms bigint,\
	ts timestamp,\
	reportDescription text,\
	the_geom geometry\
)"

cartoAlertsFields = "(city,confidence,nThumbsUp,street,uuid,country,type,subtype,roadType,reliability,magvar,reportRating,ms,ts,reportDescription,the_geom)"

cartoJamsScehma = "(city text,\
	turntype text,\
	level int,\
	country text,\
	speedKMH real,\
	delay int,\
	length int,\
	street text,\
	ms bigint,\
	ts timestamp,\
	endNode text,\
	the_geom geometry,\
	type text,\
	id bigint,\
	speed real,\
	uuid text,\
	startNode text\
)"

cartoJamsFields = "(city,turntype,level,country,speedKMH,delay,length,street,ms,ts,endNode,type,id,speed,uuid,startNode,the_geom)"

cartoIrregularitiesScehma = "(trend int,\
	street text,\
	endNode text,\
	nImages int,\
	speed real,\
	id text,\
	severity int,\
	type text,\
	highway bool,\
	nThumbsUp int,\
	seconds int,\
	alertsCount int,\
	detectionDateMS bigint,\
	detectionDateTS timestamp,\
	driversCount int,\
	the_geom geometry,\
	startNode text,\
	updateDateMS bigint,\
	updateDateTS timestamp,\
	regularSpeed real,\
	country text,\
	length int,\
	delaySeconds int,\
	jamLevel int,\
	nComments int,\
	city text,\
	causeType text,\
	causeAlertUUID text\
)"

cartoIrregularitiesFields = "(trend,street,endNode,nImages,speed,id,severity,type,highway,nThumbsUp,seconds,alertsCount,detectionDateMS,detectionDateTS,driversCount,startNode,updateDateMS,updateDateTS,regularSpeed,country,length,delaySeconds,jamLevel,nComments,city,causeType,causeAlertUUID,the_geom)"
