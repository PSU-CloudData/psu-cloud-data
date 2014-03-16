#!/usr/bin/env python
#
# Copyright 2014 Shaun Brandt <sbrandt@pdx.edu>, Neil Gebhard <gebhard@pdx.edu>,
#	Eddie Kelley <kelley@pdx.edu>, Eric Mumm <emumm@pdx.edu>, Tim Reilly <tfr@pdx.edu>.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import webapp2
import csv
import logging
import urllib
import datetime

from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext.blobstore import BlobInfo
from google.appengine.ext import ndb
from google.appengine.runtime import apiproxy_errors

from BaseHandler import BaseHandler
from FileMetadata import FileMetadata
from FreewayData import Highway, Station, Detector, SpeedSum, DetectorEntry, StationEntry


def combine_stations():
	""" store Station entity keys in Highway records in datastore

	Append each Station entity's key to the corresponding Highway's list of stations.
	"""
	stn_q = Station.query()
	for stn in stn_q.fetch():
		hwy_q = Highway.query(Highway.highwayid == stn.highwayid)
		for hwy in hwy_q.fetch():
			if stn.key not in hwy.stations:
				logging.info("Appending station %s to highway %s", stn.stationid, hwy.highwayid)
				hwy.stations.append(stn.key)
				hwy.put()


def combine_detectors():
	""" combine Station and Detector data records in datastore

	Append each Detector record to the corresponding Station's list of detectors properties.
	Remove Detector entities after they have been imported.
	"""
	det_q = Detector.query()
	for det in det_q.fetch():
		stn_q = Station.query(Station.stationid == det.stationid)
		for stn in stn_q.fetch():
			if det not in stn.detectors:
				stn.detectors.append(det)
				stn.put()
				logging.info("Put detector:%s in station:%s", det.key, stn.stationid)
	#deleteDetectors()

def deleteDetectors():
	""" delete all Detector entities from datastore """
	detector_keys = Detector.query().fetch(keys_only = True)
	detector_entities = ndb.get_multi(detector_keys)
	ndb.delete_multi([d.key for d in detector_entities])


def getHighways():
	""" get a list of highway entities

	Query the datastore for the list of highways
	
	Returns:
	  list of Highway entities
	"""

	hwy_q = Highway.query()
	results = hwy_q.fetch(12)
	hwys = [hwy for hwy in results]
	return hwys


def getStationsForHighway(hw, dir):
	""" get a list of station entities

	Query the datastore for a list of station entities located on a specific highway

	Args:
	  hw: highwayname that contains the desired stations
	  dir: direction of highway for stations that are requested
	Returns:
	  list of Station entities
	"""
	stations = []
	hwy_q = Highway.query(Highway.highwayname == hw, Highway.shortdirection == dir)
	for hwy in hwy_q.fetch():
		stations = [station.get() for station in hwy.stations]
	return stations


class MainHandler(BaseHandler):
	""" MainHandler class definition
	
	Provides a user interface
	"""
	def get(self):
		""" respond to HTTP GET requests
	
		Display a user interface for uploading files to Blobstore
		"""
		hwy = self.request.get('hwy', default_value = 'I-205')
		dir = self.request.get('dir', default_value = 'N')

		try:
			# get a list of files residing in Blobstore
			file_q = FileMetadata.query()
			results = file_q.fetch(10)
			files = [result for result in results]
			file_count = len(files)

			# get highways from Datastore
			hwys = getHighways()
			hwy_count = len(hwys)
			
			stns = getStationsForHighway(hwy, dir)
			stn_count = len(stns)
			
			hwy_list = set([hwy.highwayname for hwy in hwys])
			stn_list = set([stn.locationtext for stn in stns])
			
			upload_url = blobstore.create_upload_url('/upload')
					
			self.render_template("index.html",{
								 "file_count": file_count,
								 "files": files,
								 "highways": hwys,
								 "hwys_count": hwy_count,
								 "stations": stns,
								 "stns_count": stn_count,
								 "upload_url":upload_url})
		except apiproxy_errors.OverQuotaError, message:
			logging.error(message)
			self.response.out.write("<h3>Error</h3><br/><p>%s</p>", message)

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
	""" UploadHandler class definition
	
	Handle uploads of data to Blobstore
	"""
	def post(self):
		""" respond to HTTP POST requests
	
		Create FileMetadata entity in Datastore to keep track of uploaded files
		"""
		blob_info = self.get_uploads()[0]
		
		file_metadata = FileMetadata(id = str(blob_info.key()),
									 content_type = blob_info.content_type,
									 creation = blob_info.creation,
									 filename = blob_info.filename,
									 size = blob_info.size,
									 md5_hash = blob_info.md5_hash,
									 blobkey = str(blob_info.key()))
									 
		file_metadata.put()
		self.redirect("/")


class ImportHandler(BaseHandler):
	""" ImportHandler class definition
	
	Handle import of data to Datastore
	"""
	def post(self):
		""" respond to HTTP POST requests
	
		Perform import of blob data referenced by blobkey
		"""
		# get the resource key
		resource = self.request.get('blobkey')
		# get BlobInfo from the blobstore using resource key
		blob_info = blobstore.BlobInfo.get(resource)
		# get the filename of the blob
		filename = blob_info.filename
		# get a BlobReader object for the resource
		blob_reader = blobstore.BlobReader(resource)
		# get a DictReader object to use for parsing the resource
		csv_reader = csv.DictReader(blob_reader)
		if filename == 'highways.csv':
			for line in csv_reader:
				h = Highway(id=line['highwayid'],
							highwayid=int(line['highwayid']),
							shortdirection=line['shortdirection'],
							direction=line['direction'],
							highwayname=line['highwayname'])
				h.put()
		elif filename == 'freeway_stations.csv':
			for line in csv_reader:
				s = Station(id=line['stationid'],
							stationid=int(line['stationid']),
							highwayid=int(line['highwayid']),
							milepost=float(line['milepost']),
							locationtext=line['locationtext'],
							upstream=int(line['upstream']),
							downstream=int(line['downstream']),
							stationclass=int(line['stationclass']),
							numberlanes=int(line['numberlanes']),
							latlon=ndb.GeoPt(line['latlon']),
							highway=ndb.Key(Highway, line['highwayid']))
				if '.' in line['length_mid']:
					setattr(s, 'length_mid', float(line['length_mid']))
				s.put()
			combine_stations()
		elif filename == 'freeway_detectors.csv':
			for line in csv_reader:
				d = Detector(id=line['detectorid'],
							 detectorid=int(line['detectorid']),
							 highwayid=int(line['highwayid']),
							 milepost=float(line['milepost']),
							 locationtext=line['locationtext'],
							 detectorclass=int(line['detectorclass']),
							 lanenumber=int(line['lanenumber']),
							 stationid=int(line['stationid']))
				d.put()
				self.response.out.write(d)
			combine_detectors()
		else:
			logging.info("Import not supported for file: "+blob_info.filename)
		self.redirect("/")



#    Single-Day Station Travel Times: Find travel time for each NB station for 5 minute intervals for Sept 22, 2011.

class Q1Handler(BaseHandler):
	""" Q1Handler class definition
	
	Handle running query 1 on over the datastore given user input
	"""
	def post(self):
		freeway = self.request.get('q1freeway')
		(fwayname, fwaydir) = freeway.split()
		interval = self.request.get('q1interval')
		date = self.request.get('q1date')
		
		highway_q = Highway.query(Highway.highwayname == fwayname, Highway.shortdirection == fwaydir)
		logging.info(highway_q)
		highway = highway_q.get()
		results = []
		
		if highway:
			stations = Station.query(Station.highwayid == highway.highwayid).fetch()
			
			for station in stations:
				# get all station entries for this station
				if station.stationclass == 1:
					# don't count the freeway onramp loop data
					station_entries = StationEntry.query(StationEntry.stationid == station.stationid,
														StationEntry.date == datetime.datetime.strptime(date, "%m/%d/%Y")).fetch()
					entries = []
					s_time = None
					s_sum = 0
					s_count = 0
					for station_entry in station_entries:
						# aggregate all station entry SumCounts
						s_sum += sum([int(speed_sum.sum) for speed_sum in station_entry.fivemin_speed])
						s_count += sum([int(speed_sum.count) for speed_sum in station_entry.fivemin_speed])
						if len(station_entry.fivemin_speed) > 0:
							s_time = station_entry.fivemin_speed[0].time
					
					average_speed = 0
					if (s_count != 0) and (s_sum != 0):
						average_speed = s_sum / s_count
						
					if not s_time:
						results.append("Station:%s date:%s average speed:%f" % (station_entry.stationid, station_entry.date, average_speed))
					else:
						results.append("Station:%s date:%s time:%s average speed:%f" % (station_entry.stationid, station_entry.date, s_time, average_speed))

			self.render_template("query.html", {'freeway': fwayname,
												'direction': fwaydir,
												'interval': interval,
												'date': date,
												'results': results,})

#    Hourly Corridor Travel Times: Find travel time for the entire I-205 NB freeway section in the data set 
#    (Sunnyside Rd to the river - all NB stations in the data set) for each hour in the 2-month test period

class Q2Handler(BaseHandler):
	""" Q2Handler class definition
	
	Handle running query 2 on over the datastore given user input
	"""
	def post(self):
# get the user input, imortant varaiables at the end are.
# fway: the freeway associated with Highway.highwayname
# dir: the highway direction associated with Highway.shortdirection
# start: the date to start query on
# end: the date to end query on

		freeway = self.request.get('q2freeway')
		hold= freeway.split()
		i=iter(hold)
		fway = i.next()
		dir = i.next()
		start = self.request.get('q2sdate')
		end = self.request.get('q2edate')
		counter = 0
		speed_sums = []
		lengths = []

# get Highwayid of highway= highwayname and direction = dir, create a list of them 

		highway = Highway.query(ndb.AND(Highway.highwayname == fway, Highway.shortdirection == dir)).get()	
		if highway:
			stations = Station.query(Station.highwayid == highway.highwayid).fetch()
			
			for station in stations:
				lengths.append(station.length_mid)
				speed_sums.append([])

			for station in stations:
				if station.stationclass == 1:
					counter = 0
					# don't count the freeway onramp loop data
					for detector in station.detectors:
						# get all detector entry entities in each station grouping
						detector_key = ndb.Key(Detector, detector.detectorid)
						det_entries_q = DetectorEntry.query(DetectorEntry.detectorid == detector.detectorid,
											DetectorEntry.date >= datetime.datetime.strptime(start, "%m/%d/%Y"),
											DetectorEntry.date <= datetime.datetime.strptime(end, "%m/%d/%Y"))

						det_entries = det_entries_q.fetch()
						for det_entry in det_entries:
							speed_sums[counter].append(det_entry.hourly_speed.sum)
					
						counter += 1

#    Mid-Weekday Peak Period Travel Times: Find the average travel time for 7-9AM and 4-6PM on Tuesdays, Wednesdays and 
#    Thursdays for the I-205 NB freeway during the 2-month test period

class Q3Handler(BaseHandler):
	""" Q3Handler class definition
	
	Handle running query 3 on over the datastore given user input
	"""
	def post(self):

# get all of the info from the user interaction the important variables when done are:
# fway: The freeway
# dir: The freeway direction N E S W
# sday: start day: Monday Tuesday ...
# sdate: start date of form 09/12/2011
# eday: end day: Monday Tuesday ...
# edate: end date of form 09/12/2011

		freeway = self.request.get('q3freeway')
		hold= freeway.split()
		i=iter(hold)
		fway = i.next()
		dir = i.next()
		start = self.request.get('q3sdate')
		end = self.request.get('q3edate')
		seven_am = datetime.datetime.strptime("07:00:00 AM", "%I:%M:%S %p")
		nine_am = datetime.datetime.strptime("09:00:00 AM", "%I:%M:%S %p")
		four_pm = datetime.datetime.strptime("04:00:00 PM", "%I:%M:%S %p")
		six_pm = datetime.datetime.strptime("06:00:00 PM", "%I:%M:%S %p")


# get Highwayid of highway= highwayname and direction = dir, create a list of them 

		highway = Highway.query(ndb.AND(Highway.highwayname == fway, Highway.shortdirection == dir)).get()	
		if highway:
			stations = Station.query(Station.highwayid == highway.highwayid).fetch()
			
			for station in stations:
				if station.stationclass == 1:
					# don't count the freeway onramp loop data
					speed_sums = []
					for detector in station.detectors:
						# get all detector entry entities in each station grouping
						det_entries_q = DetectorEntry.query(DetectorEntry.detectorid == detector.detectorid,
											DetectorEntry.date == datetime.datetime.strptime(start, "%m/%d/%Y"),
											DetectorEntry.date <= datetime.datetime.strptime(end, "%m/%d/%Y"))
						
						
						det_times = det_entries_q.filter(ndb.OR(ndb.AND(DetectorEntry.fivemin_speed.time >= seven_am.time(),
												DetectorEntry.fivemin_speed.time <= nine_am.time()),
												ndb.AND(DetectorEntry.fivemin_speed.time >= four_pm.time(),
												DetectorEntry.fivemin_speed.time <= six_pm.time())))

						det_entries = det_times.fetch()
						for det_entry in det_entries:
							if (date(det_entry.date).weekday() == 2) or (date(det_entry.date).weekday() == 3) or (date(det_entry.date).weekday() == 4):
								speed_sums.append(det_entry.fivemin_speed)
					
					for time_interval in speed_sums:
						
						# sum detector entries for this interval
						speed = sum([int(speed_sum.sum) for speed_sum in time_interval])
						count = sum([int(speed_sum.count) for speed_sum in time_interval])
						average_speed = 0
						if (count != 0) and (speed != 0):
							average_speed = speed / count
						results.append("Station:%s date:%s time:%s average speed:%f" % (station.stationid, det_entry.date, speed_sum.time, average_speed))					



#    Station-to-Station Travel Times: Find travel time for all station-to-station NB pairs for 8AM on Sept 22, 2011

class Q4Handler(BaseHandler):
	""" Q4Handler class definition
	
	Handle running query 4 on over the datastore given user input
	"""
	def post(self):
# get all of the user info: important variables are:
# fway: the highway.highwayname
# dir: the highway.shortdirection
# time: the time to query on, no range, just that time.
# date: the date for DetectorEntity

# delete the self.response.out.write(time) when implimented, it is a check

		freeway = self.request.get('q4freeway')		
		hold= freeway.split()
		i=iter(hold)
		fway = i.next()
		dir = i.next()

		hour = self.request.get('q4hour')
		min = self.request.get('q4min')
		ampm = self.request.get('q4ampm')
		date = self.request.get('q4date')
		time = hour + ":" + min + ":00 " + ampm
		self.response.out.write(time)
		

# get Highwayid of highway= highwayname and direction = dir, create a list of them (bad approach but it works)

		stationget = Highway.query(ndb.AND(Highway.highwayname == fway, Highway.shortdirection == dir))
		hwyid = stationget.fetch(projection=[Highway.highwayid])
		stationlist = list()
		for highway in hwyid:
		  stationlist.append(highway.highwayid)

# Get the stations in the highway returned above		

		stations = Station.query(Station.highwayid.IN(stationlist))



		self.response.out.write('''
        	<html>
          		<body>
				
            			<form action ="/">
              		 	  <input type="submit" name="Home" value="Home"/>
            			</form>
          		</body>
       	 	</html>
        	''')


class FileInfoHandler(BaseHandler):
	""" FileInfoHandler class definition
	
	Display information about uploaded files
	"""
	def get(self, file_id):
		""" Respond to HTTP GET requests
	
		Render a file info page from FileMetadata matching key file_id
		
		Args:
		  file_id: a key that references some FileMetadata entity in the Datastore
		"""
		file_key = ndb.Key(FileMetadata, str(urllib.unquote(file_id)).strip())
		file_info = file_key.get()
		logging.info("File info:%s for key:%s", file_info, file_key)
		if not file_info:
			self.error(404)
			return
		self.render_template("file_info.html", {
							 'file_info': file_info
							 })


class FileDownloadHandler(blobstore_handlers.BlobstoreDownloadHandler):
	""" FileDownloadHandler class definition
	
	Download files from Blobstore
	"""
	def get(self, file_id):
		""" Respond to HTTP GET requests
		
		Download files from Blobstore that are referenced by file_id key
		
		Args:
		  file_id a key that references some FileMetadata entity in the Datastore
		"""
		file_info = str(urllib.unquote(file_id)).strip()
		if not file_info:
			self.error(404)
			return
		self.send_blob(BlobInfo.get(file_id), save_as=False)


app = webapp2.WSGIApplication([
    ('/', MainHandler),
	('/upload', UploadHandler),
	('/import', ImportHandler),
	('/Queryone', Q1Handler),
	('/Querytwo', Q2Handler),
	('/Querythree', Q3Handler),
	('/Queryfour', Q4Handler),
	('/file/(.*)', FileInfoHandler),
	('/blobstore/(.*)', FileDownloadHandler),
], debug=True)
