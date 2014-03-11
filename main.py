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

from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext.blobstore import BlobInfo
from google.appengine.ext import ndb

from BaseHandler import BaseHandler
from FileMetadata import FileMetadata
from FreewayData import Highway, Station, Detector, LoopData, SpeedSum, DetectorEntry

class MainHandler(BaseHandler):
	""" MainHandler class definition
	
	Provides a user interface
	"""
	def get(self):
		""" respond to HTTP GET requests
	
		Display a user interface for uploading files to Blobstore
		"""
		q = FileMetadata.query()
		results = q.fetch(10)
		
		files = [result for result in results]
		
		file_count = len(files)
		
		upload_url = blobstore.create_upload_url('/upload')
				
		self.render_template("index.html",{
							 "file_count": file_count,
							 "files": files,
							 "upload_url":upload_url})


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
				self.response.out.write(h)
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
				self.response.out.write(s)
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
		else:
			logging.info("Import not supported for file: "+blob_info.filename)
		self.redirect("/")

app = webapp2.WSGIApplication([
    ('/', MainHandler),
	('/upload', UploadHandler),
	('/import', ImportHandler)
], debug=True)
