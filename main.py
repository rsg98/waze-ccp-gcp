'''Copyright 2018 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.'''

import json
import datetime
import logging
import requests
import uuid
import pickle
from google.cloud import bigquery, storage, ndb, tasks_v2
import unidecode
import schemas
from flask import Flask, request, render_template
import os

app = Flask(__name__, instance_relative_config=True)
app.config.from_pyfile("wazeccp.cfg", silent=False)

datastore = ndb.Client()
gcs = storage.Client()

# Your Waze CCP URL
wazeURL= app.config["WAZE_URL"]

# Carto Params
cartoEnabled = app.config["CARTO_ENABLED"]
cartoURLBase = app.config["CARTO_URL_BASE"]
cartoAPIKey = app.config["CARTO_API_KEY"]

#GCS Params
gcsPath = "{}/".format(app.config["GCSPATH"])

#BigQuery Params
bqDataset = app.config['BQDATASET']

guid = app.config['GUID']

#Define a Datastore ndb Model for each Waze "case" (Study area) that you want to monitor
class caseModel(ndb.Model):
  uid = ndb.StringProperty()
  name = ndb.StringProperty()
  day = ndb.StringProperty()

#This application will track unique entities for Jams, Alerts, and Irregularities.
#We don't want to write duplicate events to BigQuery if they persist through the refresh window.

#Define an ndb Model to track unique Jams.
class uniqueJams(ndb.Model):
	tableUUID = ndb.StringProperty()
	jamsUUID = ndb.StringProperty()

#Define an ndb Model to track unique Alerts.
class uniqueAlerts(ndb.Model):
	tableUUID = ndb.StringProperty()
	alertsUUID = ndb.StringProperty()

#Define an ndb Model to track unique Irregularities.
class uniqueIrregularities(ndb.Model):
	tableUUID = ndb.StringProperty()
	irregularitiesUUID = ndb.StringProperty()

#App Request Handler to create a new Case.
#Called ONCE as: {your-app}.appspot.com/newCase/?name={your-case-name}
#This handler can be disabled after you create your first case if you
#only intend to create one.

@app.route("/newCase", methods=["GET"])
def newCase():
	uid = uuid.uuid4()
	name = request.args.get("name")
	day = datetime.datetime.now().strftime("%Y-%m-%d")
	if not name:
		name = ""
	
	with datastore.context():
		#Write the new Case details to Datastore
		wazePut = caseModel(uid=str(uid),day=day,name=name)
		wazePut.put()
	
	#Create the BigQuery Client
	client = bigquery.Client()
	datasetRef = client.get_dataset(bqDataset)
	tableSuffix = str(uid).replace('-','_')

	#Create the Jams Table
	jamsTable = 'jams_' + tableSuffix
	tableRef = datasetRef.table(jamsTable)
	table = bigquery.Table(tableRef,schema=schemas.jamsSchema)
	table = client.create_table(table)
	assert table.table_id == jamsTable


	#Create the Alerts Table
	alertsTable = 'alerts_' + tableSuffix
	tableRef = datasetRef.table(alertsTable)
	table = bigquery.Table(tableRef,schema=schemas.alertsSchema)
	table = client.create_table(table)
	assert table.table_id == alertsTable


	#Create the Irregularities Table
	irregularitiesTable = 'irregularities_' + tableSuffix
	tableRef = datasetRef.table(irregularitiesTable)
	table = bigquery.Table(tableRef,schema=schemas.irregularitiesSchema)
	table = client.create_table(table)
	assert table.table_id == irregularitiesTable

	if cartoEnabled:
		# Create and register Alerts Table in Carto

		url = cartoURLBase + "q=CREATE TABLE " + alertsTable + " " + schemas.cartoAlertsSchema + "&api_key=" + cartoAPIKey
		logging.info(url)
		try:
			result = requests.get(url.replace(" ", "%20"), validate_certificate=True)
			if result.status_code == 200:
				url = cartoURLBase + "q=SELECT cdb_cartodbfytable('" + alertsTable + "')&api_key=" + cartoAPIKey
				logging.info(url)
				try:
					result = requests.get(url.replace(" ", "%20"), validate_certificate=True)
					if result.status_code == 200:
						logging.info('Created and Cartodbfyd Table ' + alertsTable)
				except requests.exceptions.RequestException:
					logging.exception('Caught exception Cartodbfying Waze Alerts Table ' + alertsTable)
			else:
				logging.exception(result.status_code)
		except requests.exceptions.RequestException:
			logging.exception('Caught exception Creating Waze Alerts Table ' + alertsTable)

		# Create and register Jams Table in Carto

		url = cartoURLBase + "q=CREATE TABLE " + jamsTable + " " + schemas.cartoJamsScehma + "&api_key=" + cartoAPIKey
		logging.info(url)
		try:
			result = requests.get(url.replace(" ", "%20"), validate_certificate=True)
			if result.status_code == 200:
				url = cartoURLBase + "q=SELECT cdb_cartodbfytable('" + jamsTable + "')&api_key=" + cartoAPIKey
				logging.info(url)
				try:
					result = requests.get(url.replace(" ", "%20"), validate_certificate=True)
					if result.status_code == 200:
						logging.info('Created and Cartodbfyd Table ' + jamsTable)
				except requests.exceptions.RequestException:
					logging.exception('Caught exception Cartodbfying Waze Jams Table ' + jamsTable)
			else:
				logging.exception(result.status_code)
		except requests.exceptions.RequestException:
			logging.exception('Caught exception Creating Waze Jams Table ' + jamsTable)

		# Create and register Irregularities Table in Carto

		url = cartoURLBase + "q=CREATE TABLE " + irregularitiesTable + " " + schemas.cartoIrregularitiesScehma + "&api_key=" + cartoAPIKey
		logging.info(url)
		try:
			result = requests.get(url.replace(" ", "%20"), validate_certificate=True)
			if result.status_code == 200:
				url = cartoURLBase + "q=SELECT cdb_cartodbfytable('" + irregularitiesTable + "')&api_key=" + cartoAPIKey
				logging.info(url)
				try:
					result = requests.get(url.replace(" ", "%20"), validate_certificate=True)
					if result.status_code == 200:
						logging.info('Created and Cartodbfyd Table ' + irregularitiesTable)
				except requests.exceptions.RequestException:
					logging.exception('Caught exception Cartodbfying Waze Jams Table ' + irregularitiesTable)
			else:
				logging.exception(result.status_code)
		except requests.exceptions.RequestException:
			logging.exception('Caught exception Creating Waze Jams Table ' + irregularitiesTable)

	return render_template("case_create.html", name=name, uid=uid)

#Called at your set cron interval, this function loops through all the cases in datastore
#And adds a task to update the case's tables in Taskqeue
@app.route("/{}/cron".format(guid), methods=["GET"])
def updateCaseStudies():

	# This should be called by AppEngine Cron, which sets this header
	is_appengine_cron = request.headers.get('X-AppEngine-Cron')
	if (not is_appengine_cron) and not app.debug:
		return '', 403

	tasks_client = tasks_v2.CloudTasksClient()

	project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
	location = app.config["CLOUD_TASKS_LOCATION"]
	queue = app.config["CLOUD_TASKS_QUEUE"]

	parent = tasks_client.queue_path(project_id, location, queue)

	updateCaseUri = "/{}/updateCase".format(guid)

	with datastore.context():
		caseStudies = caseModel.query()
		for case in caseStudies:
			task = {
				'app_engine_http_request': {
					'http_method': 'POST',
					'relative_uri': updateCaseUri,
					'body': pickle.dumps(case)
				}
			}

			result = tasks_client.create_task(parent, task)
			
			# Dump the pickle'd case to disk, so it can be used to test:
			# 	curl --request POST --data-binary "@case-dump.bin" http://127.0.0.1:5000/{guid}/updateCase
			if app.config["RUN_LOCAL"]:
				pickle.dump(case, open("case-dump.bin", "wb"))

	return render_template("cron_submit.html", name=result.name)
			
#For each Case, update each table
@app.route("/{}/updateCase".format(guid), methods=["POST"])
def updateCase():
	url = wazeURL
	pickled_case = request.get_data()
	case = pickle.loads(pickled_case)
	try:
		result = requests.get(url)
		if result.status_code == 200:
			data = json.loads(result.content)
			#Get the 3 components from the Waze CCP JSON Response
			alerts = data.get('alerts')
			jams = data.get('jams')
			irregularities = data.get('irregularities')
			if alerts is not None:
				processAlerts(data['alerts'],case.uid,case.day)
			if jams is not None:
				processJams(data['jams'],case.uid,case.day)
			if irregularities is not None:
				processIrregularities(data['irregularities'],case.uid,case.day)

			return json.dumps({'success': True}), 200, {'ContentType':'application/json'}
		else:
			return json.dumps({'success': False}), result.status_code, {'ContentType':'application/json'}
	except requests.exceptions.RequestsException:
		logging.exception('Caught exception fetching Waze URL')

#Process the Alerts
def processAlerts(alerts,uid,day):
	now = datetime.datetime.now().strftime("%s")
	features = []
	bqRows = []
	cartoRows =[]
	with datastore.context():
		for item in alerts:
			ms = item.get('pubMillis')
			itemTimeStamp = datetime.datetime.fromtimestamp(ms/1000.0)
			caseTimeMinimum = datetime.datetime.strptime(day, "%Y-%m-%d")
			if itemTimeStamp >= caseTimeMinimum:
				timestamp = datetime.datetime.fromtimestamp(ms/1000.0).strftime("%Y-%m-%d %H:%M:%S")
				city = item.get('city')
				street = item.get('street')
				confidence = item.get('confidence')
				nThumbsUp = item.get('nThumbsUp')
				uuid = item.get('uuid')
				country = item.get('country')
				subtype = item.get('subtype')
				roadType = item.get('roadType')
				reliability = item.get('reliability')
				magvar = item.get('magvar')
				alertType = item.get('type')
				reportRating = item.get('reportRating')
				reportDescription = item.get('reportDescription')
				longitude = item.get('location').get('x')
				latitude = item.get('location').get('y')
				#Create GCS GeoJSON Properties
				properties = {"city": city,
					"street": street,
					"confidence": confidence,
					"nThumbsUp": nThumbsUp,
					"uuid": uuid,
					"country": country,
					"subtype": subtype,
					"roadType": roadType,
					"reliability": reliability,
					"magvar": magvar,
					"type": alertType,
					"reportRating": reportRating,
					"pubMillis": ms,
					"timestamp": timestamp,
					"reportDescription": reportDescription,
				}
				geometry = {"type": "Point",
								"coordinates": [longitude, latitude]
				}
				features.append({"type": "Feature", "properties": properties, "geometry": geometry})
				#BigQuery Row Creation
				datastoreQuery = uniqueAlerts.query(uniqueAlerts.tableUUID == uid, uniqueAlerts.alertsUUID == uuid)
				datastoreCheck = datastoreQuery.get()
				if not datastoreCheck:
					bqRow = {"city": city,
								"street": street,
								"confidence": confidence,
								"nThumbsUp": nThumbsUp,
								"uuid": uuid,
								"country": country,
								"subtype": subtype,
								"roadType": roadType,
								"reliability": reliability,
								"magvar": magvar,
								"type": alertType,
								"reportRating": reportRating,
								"ms": ms,
								"ts": timestamp,
								"reportDescription": reportDescription,
								"geo": "Point(" + str(longitude) + " " + str(latitude) + ")",
								"geoWKT": "Point(" + str(longitude) + " " + str(latitude) + ")"
					}
					bqRows.append(bqRow)
					
					if cartoEnabled:
						# Create Carto Row
						if city is not None:
							city = unidecode.unidecode(city)
						if street is not None:
							street = unidecode.unidecode(street)
						if uuid is not None:
							uuid = unidecode.unidecode(uuid)
						if country is not None:
							country = unidecode.unidecode(country)
						if alertType is not None:
							alertType = unidecode.unidecode(alertType)
						if subtype is not None:
							subtype = unidecode.unidecode(subtype)
						if reportDescription is not None:
							reportDescription = unidecode.unidecode(reportDescription)

						cartoRow = "('{city}',{confidence},{nThumbsUp},'{street}','{uuid}','{country}','{type}','{subtype}',{roadType},{reliability},{magvar},{reportRating},{ms},'{ts}','{reportDescription}',ST_GeomFromText({the_geom},4326))".format(
							city=city, confidence=confidence, nThumbsUp=nThumbsUp, street=street, uuid=uuid, country=country,
							type=alertType, subtype=subtype, roadType=roadType, reliability=reliability, magvar=magvar,
							reportRating=reportRating, ms=ms, ts=timestamp, reportDescription=reportDescription, the_geom="'Point(" + str(longitude) + " " + str(latitude) + ")'")
						cartoRows.append(cartoRow)

					#Add uuid to Datastore
					alertPut = uniqueAlerts(tableUUID=str(uid), alertsUUID=str(uuid))
					alertPut.put()

	#Write GeoJSONs to GCS
	alertGeoJSON = json.dumps({"type": "FeatureCollection", "features": features})
	writeGeoJSON(alertGeoJSON,gcsPath  + uid + '/' + uid + '-alerts.geojson')
	writeGeoJSON(alertGeoJSON,gcsPath  + uid + '/' + uid + '-' + now +'-alerts.geojson')

	#Stream new Rows to BigQuery
	if bqRows:
		alertsTable = 'alerts_' + str(uid).replace('-','_')
		writeBigqueryRows(bqRows, alertsTable, schemas.alertsSchema)
	
	if cartoEnabled:
		# Load Rows to Carto

		if cartoRows:
			cartoQuery = "INSERT INTO " + alertsTable + " " + schemas.cartoAlertsFields + "VALUES " + ",".join(cartoRows)
			cartoQuery = cartoQuery.replace("'None'","null").replace("None","null")
			cartoQueryEncoded = requests.utils.quote({"q":cartoQuery})
			# logging.info(cartoQuery)
			# logging.info(cartoQueryEncoded)


			# url = cartoURLBase + cartoQueryEncoded + "&api_key=" + cartoAPIKey
			url = cartoURLBase + "&api_key=" + cartoAPIKey
			# logging.info(url)
			try:
				result = requests.post(url, validate_certificate=True, data=cartoQueryEncoded)
				if result.status_code == 200:
					logging.info('Inserted Alerts Data to Carto')
				else:
					logging.exception(result.status_code)
					logging.exception(cartoQuery)
					writeSQLError(cartoQuery,gcsPath + 'carto_errors/' + uid + '-' + now + '-alerts.txt')
			except requests.exceptions.RequestException:
				logging.exception('Caught exception Inserting Data to Alerts Table ' + alertsTable)

#Process the Jams
def processJams(jams,uid,day):
	now = datetime.datetime.now().strftime("%s")
	features = []
	bqRows =[]
	cartoRows = []

	with datastore.context():
		for item in jams:
			ms = item.get('pubMillis')
			itemTimeStamp = datetime.datetime.fromtimestamp(ms/1000.0)
			caseTimeMinimum = datetime.datetime.strptime(day, "%Y-%m-%d")
			if itemTimeStamp >= caseTimeMinimum:
				timestamp = datetime.datetime.fromtimestamp(ms/1000.0).strftime("%Y-%m-%d %H:%M:%S")
				city = item.get('city')
				turnType = item.get('turnType')
				level = item.get('level')
				country = item.get('country')
				segments = item.get('segments')
				speedKMH = item.get('speedKMH')
				roadType = item.get('roadType')
				delay = item.get('delay')
				length = item.get('length')
				street = item.get('street')
				endNode = item.get('endNode')
				jamType = item.get('type')
				iD = item.get('id')
				uuid = item.get('uuid')
				speed = item.get('speed')
				startNode = item.get('startNode')
				#Create GCS GeoJSON Properties
				properties = {"city": city,
					"turnType": turnType,
					"level": level,
					"country": country,
					"segments": segments,
					"speedKMH": speedKMH,
					"roadType": roadType,
					"delay": delay,
					"length": length,
					"street": street,
					"pubMillis": ms,
					"timestamp": timestamp,
					"endNode": endNode,
					"type": jamType,
					"id": iD,
					"speed": speed,
					"uuid": uuid,
					"startNode": startNode
				}
				#Create WKT Polyline for BigQuery
				coordinates = []
				bqLineString = ''
				for vertex in item.get('line'):
					longitude = vertex.get('x')
					latitude = vertex.get('y')
					coordinate = [longitude, latitude]
					bqLineString += str(longitude) + " " + str(latitude) + ', '
					coordinates.append(coordinate)
				geometry = {"type": "LineString",
								"coordinates": coordinates
				}
				features.append({"type": "Feature", "properties": properties, "geometry": geometry})
				#BigQuery Row Creation
				datastoreQuery = uniqueJams.query(uniqueJams.tableUUID == str(uid), uniqueJams.jamsUUID == str(uuid))
				datastoreCheck = datastoreQuery.get()
				if not datastoreCheck:
					bqRow = {"city": city,
								"turnType": turnType,
								"level": level,
								"country": country,
								"segments": segments,
								"speedKMH": speedKMH,
								"roadType": roadType,
								"delay": delay,
								"length": length,
								"street": street,
								"ms": ms,
								"ts": timestamp,
								"endNode": endNode,
								"type": jamType,
								"id": iD,
								"speed": speed,
								"uuid": uuid,
								"startNode": startNode,
								"geo": "LineString(" + bqLineString[:-2] + ")",
								"geoWKT": "LineString(" + bqLineString[:-2] + ")"
					}
					bqRows.append(bqRow)
					
					if cartoEnabled:
						# Create Carto Row

						if city is not None:
							city = unidecode.unidecode(city)
						if turnType is not None:
							turnType = unidecode.unidecode(turnType)
						if country is not None:
							country = unidecode.unidecode(country)
						if street is not None:
							street = unidecode.unidecode(street)
						if endNode is not None:
							endNode = unidecode.unidecode(endNode)
						if jamType is not None:
							jamType = unidecode.unidecode(jamType)
						if startNode is not None:
							startNode = unidecode.unidecode(startNode)

						cartoRow = "('{city}','{turntype}',{level},'{country}',{speedKMH},{delay},{length},'{street}',{ms},'{ts}','{endNode}','{type}',{id},{speed},'{uuid}','{startNode}',ST_GeomFromText({the_geom},4326))".format(
							city=city, turntype=turnType, level=level, country=country, speedKMH=speedKMH, delay=delay,
							length=length, street=street, ms=ms, ts=timestamp, endNode=endNode,
							type=jamType, id=iD, speed=speed, uuid=uuid, startNode=startNode, the_geom="'LineString(" + bqLineString[:-2] + ")'")
						cartoRows.append(cartoRow)

					#Add uuid to Datastore
					jamPut = uniqueJams(tableUUID=str(uid), jamsUUID=str(uuid))
					jamPut.put()
	jamsGeoJSON = json.dumps({"type": "FeatureCollection", "features": features})
	writeGeoJSON(jamsGeoJSON,gcsPath  + uid + '/' + uid + '-jams.geojson')
	writeGeoJSON(jamsGeoJSON,gcsPath  + uid + '/' + uid + '-' + now +'-jams.geojson')

	#Stream new Rows to BigQuery
	if bqRows:
		jamsTable = 'jams_' + str(uid).replace('-','_')
		writeBigqueryRows(bqRows, jamsTable, schemas.jamsSchema)
	
	if cartoEnabled:
	# Load Rows to Carto

		if cartoRows:
			cartoQuery = "INSERT INTO " + jamsTable + " " + schemas.cartoJamsFields + "VALUES " + ",".join(cartoRows)
			cartoQuery = cartoQuery.replace("'None'","null").replace("None","null")
			cartoQueryEncoded = requests.utils.quote({"q":cartoQuery})
			logging.info(len(cartoQueryEncoded))
			# logging.info(cartoQuery)
			# logging.info(cartoQueryEncoded)


			url = cartoURLBase + "&api_key=" + cartoAPIKey
			logging.info(url)
			try:
				result = requests.post(url, validate_certificate=True, data=cartoQueryEncoded)
				if result.status_code == 200:
					logging.info('Inserted Jams Data to Carto')
				else:
					logging.exception(result.status_code)
					logging.exception(cartoQuery)
					writeSQLError(cartoQuery,gcsPath + 'carto_errors/' + uid + '-' + now + '-jams.txt')
			except requests.exceptions.RequestException:
				logging.exception('Caught exception Inserting Data to Jams Table ' + jamsTable)
	
#Process the Irregularities
def processIrregularities(irregularities,uid,day):
	now = datetime.datetime.now().strftime("%s")
	features = []
	bqRows = []
	cartoRows = []

	with datastore.context():
		for item in irregularities:
			detectionDateMS = item.get('detectionDateMillis')
			itemTimeStamp = datetime.datetime.fromtimestamp(detectionDateMS/1000.0)
			caseTimeMinimum = datetime.datetime.strptime(day, "%Y-%m-%d")
			if itemTimeStamp >= caseTimeMinimum:
				detectionDateTS = datetime.datetime.fromtimestamp(detectionDateMS/1000.0).strftime("%Y-%m-%d %H:%M:%S")
				updateDateMS = item.get('updateDateMillis')
				updateDateTS = datetime.datetime.fromtimestamp(updateDateMS/1000.0).strftime("%Y-%m-%d %H:%M:%S")
				causeAlert = item.get('causeAlert')
				if causeAlert is not None:
					causeAlertUUID = causeAlert.get('uuid')
				else:
					causeAlertUUID = None
				trend = item.get('trend')
				street = item.get('street')
				endNode =  item.get('endNode')
				nImages = item.get('nImages')
				speed = item.get('speed')
				iD = item.get('id')
				severity = item.get('severity')
				irregularityType = item.get('type')
				highway = item.get('highway')
				nThumbsUp = item.get('nThumbsUp')
				seconds = item.get('seconds')
				alertsCount = item.get('alertsCount')
				driversCount = item.get('driversCount')
				startNode = item.get('startNode')
				regularSpeed = item.get('regularSpeed')
				country = item.get('country')
				length = item.get('length')
				delaySeconds = item.get('delaySeconds')
				jamLevel = item.get('jamLevel')
				nComments = item.get('nComments')
				city = item.get('city')
				causeType = item.get('causeType')
				#Create GCS GeoJSON Properties
				properties = {"trend": trend,
					"street": street,
					"endNode": endNode,
					"nImages": nImages,
					"speed": speed,
					"id": iD,
					"severity": severity,
					"type": irregularityType,
					"highway": highway,
					"nThumbsUp": nThumbsUp,
					"seconds": seconds ,
					"alertsCount": alertsCount,
					"driversCount": driversCount ,
					"startNode": startNode,
					"regularSpeed": regularSpeed,
					"country": country,
					"length": length,
					"delaySeconds": delaySeconds,
					"jamLevel": jamLevel ,
					"nComments": nComments,
					"city": city,
					"causeType": causeType,
					"detectionDateMS": detectionDateMS,
					"detectionDateTS": detectionDateTS,
					"updateDateMS": updateDateMS,
					"updateDateTS": updateDateTS,
					"causeAlertUUID": causeAlertUUID
				}
				#Create WKT Polyline for BigQuery
				coordinates = []
				bqLineString = ''
				for vertex in item.get('line'):
					longitude = vertex.get('x')
					latitude = vertex.get('y')
					coordinate = [longitude, latitude]
					bqLineString += str(longitude) + " " + str(latitude) + ', '
					coordinates.append(coordinate)
				geometry = {"type": "LineString",
								"coordinates": coordinates
				}
				features.append({"type": "Feature", "properties": properties, "geometry": geometry})

				#BigQuery Row Creation
				datastoreQuery = uniqueIrregularities.query(uniqueIrregularities.tableUUID == str(uid), uniqueIrregularities.irregularitiesUUID == str(iD)+str(updateDateMS))
				datastoreCheck = datastoreQuery.get()
				if not datastoreCheck:
					bqRow = {"trend": trend,
								"street": street,
								"endNode": endNode,
								"nImages": nImages,
								"speed": speed,
								"id": iD,
								"severity": severity,
								"type": irregularityType,
								"highway": highway,
								"nThumbsUp": nThumbsUp,
								"seconds": seconds ,
								"alertsCount": alertsCount,
								"driversCount": driversCount ,
								"startNode": startNode,
								"regularSpeed": regularSpeed,
								"country": country,
								"length": length,
								"delaySeconds": delaySeconds,
								"jamLevel": jamLevel ,
								"nComments": nComments,
								"city": city,
								"causeType": causeType,
								"detectionDateMS": detectionDateMS,
								"detectionDateTS": detectionDateTS,
								"updateDateMS": updateDateMS,
								"updateDateTS": updateDateTS,
								"causeAlertUUID": causeAlertUUID,
								"geo": "LineString(" + bqLineString[:-2] + ")",
								"geoWKT": "LineString(" + bqLineString[:-2] + ")"
					}
					bqRows.append(bqRow)
					
					if cartoEnabled:
						# Create Carto Row

						if street is not None:
							street = unidecode.unidecode(street)
						if endNode is not None:
							endNode = unidecode.unidecode(endNode)
						if irregularityType is not None:
							irregularityType = unidecode.unidecode(irregularityType)
						if startNode is not None:
							startNode = unidecode.unidecode(startNode)
						if city is not None:
							city = unidecode.unidecode(city)
						if causeType is not None:
							causeType = unidecode.unidecode(causeType)
						if causeAlertUUID is not None:
							causeAlertUUID = unidecode.unidecode(causeAlertUUID)

						cartoRow = "({trend},'{street}','{endNode}',{nImages},{speed},'{id}',{severity},'{type}',{highway},{nThumbsUp},{seconds},{alertsCount},{detectionDateMS},'{detectionDateTS}',{driversCount},'{startNode}',{updateDateMS},'{updateDateTS}',{regularSpeed},'{country}',{length},{delaySeconds},{jamLevel},{nComments},'{city}','{causeType}','{causeAlertUUID}',ST_GeomFromText({the_geom},4326))".format(
							trend=trend, street=street, endNode=endNode, nImages=nImages,speed=speed,id=iD,severity=severity,type=irregularityType,
							highway=highway, nThumbsUp=nThumbsUp,seconds=seconds,alertsCount=alertsCount,detectionDateMS=detectionDateMS,detectionDateTS=detectionDateTS,
							driversCount=driversCount,startNode=startNode,updateDateMS=updateDateMS, updateDateTS=updateDateTS, regularSpeed=regularSpeed, country=country,length=length,
							delaySeconds=delaySeconds, jamLevel=jamLevel,nComments=nComments, city=city, causeType=causeType, causeAlertUUID=causeAlertUUID, the_geom="'LineString(" + bqLineString[:-2] + ")'")
						cartoRows.append(cartoRow)
						

				#Add uuid to Datastore
				irregularityPut = uniqueIrregularities(tableUUID=str(uid), irregularitiesUUID=str(iD)+str(updateDateMS))
				irregularityPut.put()

	irregularitiesGeoJSON = json.dumps({"type": "FeatureCollection", "features": features})
	writeGeoJSON(irregularitiesGeoJSON,gcsPath  + uid + '/' + uid + '-irregularities.geojson')
	writeGeoJSON(irregularitiesGeoJSON,gcsPath  + uid + '/' + uid + '-' + now +'-irregularities.geojson')
	# logging.info(irregularitiesGeoJSON)

	#Stream new Rows to BigQuery
	if bqRows:
		irregularitiesTable = 'irregularities_' + str(uid).replace('-','_')
		writeBigqueryRows(bqRows, irregularitiesTable, schemas.irregularitiesSchema)

	if cartoEnabled:
	# Load Rows to Carto
		if cartoRows:
			cartoQuery = "INSERT INTO " + irregularitiesTable + " " + schemas.cartoIrregularitiesFields + "VALUES " + ",".join(cartoRows)
			cartoQuery = cartoQuery.replace("'None'","null").replace("None","null")
			cartoQueryEncoded = requests.utils.quote({"q":cartoQuery})
			# logging.info(cartoQuery)
			# logging.info(cartoQueryEncoded)

			url = cartoURLBase  + "&api_key=" + cartoAPIKey
			# logging.info(url)
			try:
				result = requests.post(url, validate_certificate=True,data=cartoQueryEncoded)
				if result.status_code == 200:
					logging.info('Inserted Irregularities Data to Carto')
				else:
					logging.exception(result.status_code)
					logging.exception(cartoQuery)
					writeSQLError(cartoQuery,gcsPath + 'carto_errors/' + uid + '-' + now + '-irregularities.txt')
			except requests.exceptions.RequestException:
				logging.exception('Caught exception Inserting Data to Irregularities Table ' + irregularitiesTable)

# Write the GeoJSON to GCS
def writeGeoJSON(geoJSON,filename):
	content_type='application/json'
	writeGcsBlob(geoJSON, filename, content_type)

# Write Carto SQL Errors to GCS
def writeSQLError(sql,filename):
	content_type='text/html'
	writeGcsBlob(sql, filename, content_type)
	
def writeGcsBlob(contents, path, mimetype):
	storage_client = storage.Client()
	bucket_path = path.split("/", 1)
	bucket = storage_client.bucket(bucket_path[0])
	blob = bucket.blob(bucket_path[1])
	
	blob.upload_from_string(contents, content_type=mimetype)

def writeBigqueryRows(insert_rows, table_name, table_schema):
	client = bigquery.Client()
	datasetRef = client.get_dataset(bqDataset)
	
	tableRef = datasetRef.table(table_name)
	table = bigquery.Table(tableRef,schema=table_schema)
	errors = client.insert_rows(table, insert_rows)

	try:
		assert errors == []
		logging.info(errors)
	except AssertionError as e:
		logging.warning(e)

if __name__ == '__main__':
	app.config["RUN_LOCAL"] = True
	app.run(host='127.0.0.1', port=8080, debug=True)