import webapp2
import logging
import re
import zipfile
import csv
import datetime
import time
import urllib
import os
import math

from google.appengine.ext import blobstore
from google.appengine.ext import ndb

from google.appengine.api import files
from google.appengine.api import taskqueue
from google.appengine.api import urlfetch

from mapreduce import operation as op
from mapreduce import context
from mapreduce import model
from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
from mapreduce import shuffler
from mapreduce import util

from FileMetadata import FileMetadata
from FreewayData import Detector, DetectorEntry, SpeedSum

#MARK: MapReduce pipelines

def split_into_columns(s):
	""" split a string into columns
	      
	Split s into a list of values using the ',' character as a delimiter
	"""
	s = re.sub(',,,', ',0,0,', s)
	s = re.sub(',,', ',0,', s)
	return s.split(',')


def daily_speed_sum_map(data):
	"""Daily Speed Sum map function"""
	(byte_offset, line_value) = data
	columns = split_into_columns(line_value)
	if columns[3] != 'speed':
		yield ("%s_%s" % (columns[0], columns[1][:10]), columns[3])


def daily_speed_sum_reduce(key, values):
	"""Daily Speed Sum reduce function."""
	yield "%s: %s, %s\n" % (key, sum([int(value) for value in values]), len(values))


class DailySpeedSumPipeline(base_handler.PipelineBase):
  """A pipeline to run daily_speed_sum.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """


  def run(self, filekey, blobkey):
    """ run the DailySpeedSum MapReduce job
	      
    Setup the MapReduce pipeline and yield StoreOutput function
	"""
    output = yield mapreduce_pipeline.MapreducePipeline(
        "daily_speed_sum",
        "map_reduce.daily_speed_sum_map",
        "map_reduce.daily_speed_sum_reduce",
        "mapreduce.input_readers.BlobstoreZipLineInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
		mapper_params={
			"blob_keys": blobkey,
        },
        reducer_params={
            "mime_type": "text/plain",
        },
        shards=16)
    yield StoreOutput("DailySpeedSum", filekey, output)

def hourly_speed_sum_map(data):
	"""Daily Speed Sum map function"""
	(byte_offset, line_value) = data
	columns = split_into_columns(line_value)
	if columns[3] != 'speed':
		# replace the space (' ') character with 'T'
		dtime = re.sub(' ', 'T', columns[1][:13])
		yield ("%s_%s" % (columns[0], dtime), columns[3])


def hourly_speed_sum_reduce(key, values):
	"""Daily Speed Sum reduce function."""
	yield "%s: %s, %s\n" % (key, sum([int(value) for value in values]), len(values))


class HourlySpeedSumPipeline(base_handler.PipelineBase):
  """A pipeline to run hourly_speed_sum.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """


  def run(self, filekey, blobkey):
    """ run the HourlySpeedSum MapReduce job
	      
    Setup the MapReduce pipeline and yield StoreOutput function
	"""
    output = yield mapreduce_pipeline.MapreducePipeline(
        "hourly_speed_sum",
        "map_reduce.hourly_speed_sum_map",
        "map_reduce.hourly_speed_sum_reduce",
        "mapreduce.input_readers.BlobstoreZipLineInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
		mapper_params={
			"blob_keys": blobkey,
        },
        reducer_params={
            "mime_type": "text/plain",
        },
        shards=16)
    yield StoreOutput("HourlySpeedSum", filekey, output)
	
	
def fifteen_min_speed_sum_map(data):
	"""Fifteen Minute Speed Sum map function"""
	(byte_offset, line_value) = data
	columns = split_into_columns(line_value)
	if columns[3] != 'speed':
		# replace the space (' ') character with 'T'
		dtime = re.sub(' ', 'T', columns[1][:16])
		# replace the colon (':') character with '.'
		dtime = re.sub(':', '.', dtime)
		# set the corret time interval
		dtime = dtime.split('.')
		mins = (int(math.floor((int(dtime[1])+15)/15)) - 1)*15
		if mins == 0:
			dtime = "%s.00" % (dtime[0])
		else:
			dtime = "%s.%d" % (dtime[0], mins)
			
		yield ("%s_%s" % (columns[0], dtime), columns[3])


def fifteen_min_speed_sum_reduce(key, values):
	"""Fifteen Minute Speed Sum reduce function."""
	yield "%s: %s, %s\n" % (key, sum([int(value) for value in values]), len(values))


class FifteenMinSpeedSumPipeline(base_handler.PipelineBase):
  """A pipeline to run fifteen_min_speed_sum.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """


  def run(self, filekey, blobkey):
    """ run the FifteenMinSpeedSum MapReduce job
	      
    Setup the MapReduce pipeline and yield StoreOutput function
	"""
    output = yield mapreduce_pipeline.MapreducePipeline(
        "fifteen_min_speed_sum",
        "map_reduce.fifteen_min_speed_sum_map",
        "map_reduce.fifteen_min_speed_sum_reduce",
        "mapreduce.input_readers.BlobstoreZipLineInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
		mapper_params={
			"blob_keys": blobkey,
        },
        reducer_params={
            "mime_type": "text/plain",
        },
        shards=16)
    yield StoreOutput("FifteenMinSpeedSum", filekey, output)


def five_min_speed_sum_map(data):
	"""Five Minute Speed Sum map function"""
	(byte_offset, line_value) = data
	columns = split_into_columns(line_value)
	if columns[3] != 'speed':
		# replace the space (' ') character with 'T'
		dtime = re.sub(' ', 'T', columns[1][:16])
		# replace the colon (':') character with '.'
		dtime = re.sub(':', '.', dtime)
		# set the corret time interval
		dtime = dtime.split('.')
		mins = (int(math.floor((int(dtime[1])+5)/5)) - 1)*5
		if mins == 0:
			dtime = "%s.00" % (dtime[0])
		elif mins == 5:
			dtime = "%s.05" % (dtime[0])
		else:
			dtime = "%s.%d" % (dtime[0], mins)
			
		yield ("%s_%s" % (columns[0], dtime), columns[3])


def five_min_speed_sum_reduce(key, values):
	"""Five Minute Speed Sum reduce function."""
	yield "%s: %s, %s\n" % (key, sum([int(value) for value in values]), len(values))


class FiveMinSpeedSumPipeline(base_handler.PipelineBase):
  """A pipeline to run five_min_speed_sum.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """


  def run(self, filekey, blobkey):
    """ run the FiveMinSpeedSum MapReduce job
	      
    Setup the MapReduce pipeline and yield StoreOutput function
	"""
    output = yield mapreduce_pipeline.MapreducePipeline(
        "five_min_speed_sum",
        "map_reduce.five_min_speed_sum_map",
        "map_reduce.five_min_speed_sum_reduce",
        "mapreduce.input_readers.BlobstoreZipLineInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
		mapper_params={
			"blob_keys": blobkey,
        },
        reducer_params={
            "mime_type": "text/plain",
        },
        shards=16)
    yield StoreOutput("FiveMinSpeedSum", filekey, output)

	
class StoreOutput(base_handler.PipelineBase):
	"""A pipeline to store the result of the MapReduce job in the database.

	Args:
	mr_type: the type of mapreduce job run (e.g., DailySpeedSum)
	encoded_key: the DB key corresponding to the metadata of this job
	output: the blobstore location where the output of the job is stored
	"""

	def run(self, mr_type, encoded_key, output):
		logging.info("output is %s" % str(output))
		logging.info("key is %s" % encoded_key)
		key = ndb.Key(FileMetadata, encoded_key)
		m = key.get()

		if mr_type == "DailySpeedSum":
			blob_key = blobstore.BlobKey(output[0])
			if blob_key:
				m.daily_speed_sum = blob_key
		elif mr_type == "HourlySpeedSum":
			blob_key = blobstore.BlobKey(output[0])
			if blob_key:
				m.hourly_speed_sum = blob_key
		elif mr_type == "FifteenMinSpeedSum":
			blob_key = blobstore.BlobKey(output[0])
			if blob_key:
				m.fifteen_min_speed_sum = blob_key
		elif mr_type == "FiveMinSpeedSum":
			blob_key = blobstore.BlobKey(output[0])
			if blob_key:
				m.five_min_speed_sum = blob_key

		m.put()


#MARK: RequestHandlers


class IndexHandler(webapp2.RequestHandler):
	def post(self):
		""" respond to HTTP POST requests
		
		Perform requested MapReduce operation on Datastore or Blobstore.
		"""
		filekey = self.request.get("filekey")
		blob_key = self.request.get("blobkey")
		
		if self.request.get("daily_speed_sum"):
			logging.info("Starting daily speed sum...")
			pipeline = DailySpeedSumPipeline(filekey, blob_key)
			pipeline.start()
			self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)
		elif self.request.get("hourly_speed_sum"):
			logging.info("Starting hourly speed sum...")
			pipeline = HourlySpeedSumPipeline(filekey, blob_key)
			pipeline.start()
			self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)
		elif self.request.get("fifteen_min_speed_sum"):
			logging.info("Starting fifteen minute speed sum...")
			pipeline = FifteenMinSpeedSumPipeline(filekey, blob_key)
			pipeline.start()
			self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)
		elif self.request.get("five_min_speed_sum"):
			logging.info("Starting five minute speed sum...")
			pipeline = FiveMinSpeedSumPipeline(filekey, blob_key)
			pipeline.start()
			self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)
		elif self.request.get("speed_mapper"):
			# Import aggregated data from Blobstore to Datastore using Mapper job
			url = "http://" + os.environ['HTTP_HOST'] +"/mapreduce/command/start_job"
			form_fields = {
				"name": "Import aggregate data",
				"mapper_input_reader": "mapreduce.input_readers.BlobstoreLineInputReader",
				"mapper_handler": "map_reduce.import_aggregate_data",
				"params.done_callback": "/done",
				"mapper_params.blob_keys": re.sub('/blobstore/', '', str(blob_key)),
				"mapper_params.processing_rate": 100,
				"mapper_params.shard_count": 16
			}
			form_data = urllib.urlencode(form_fields)
			logging.info(form_data)
			result = urlfetch.fetch(url=url,
									payload=form_data,
									method=urlfetch.POST,
									headers={'Content-Type': 'application/x-www-form-urlencoded',
									'X-Requested-With': 'XMLHttpRequest'})
									
			logging.info(result.headers)
			self.redirect("/mapreduce/status")

		else:
			logging.info("Unrecognized operation.")


class DoneHandler(webapp2.RequestHandler):
	"""Handler for completion of *_speed_sum operation."""
	def post(self):
		logging.info("Import done %s" % self.request.arguments())
		logging.info(self.request.headers)
			
		# use MR state to get original blob_key
		job_id = self.request.headers['Mapreduce-Id']
		state = model.MapreduceState.get_by_job_id(job_id)
		
		logging.info(state.mapreduce_spec)
		
		# use MR state to get original blob_key
		params = state.mapreduce_spec.mapper.params
		if 'blob_key' in params:
			input_blob_key = params['blob_key']
		elif 'blob_keys' in params:
			input_blob_key = params['blob_keys']
			logging.info("Input blob_key:%s", input_blob_key)

		# use MR state to get job name
		job_name = state.mapreduce_spec.name

		logging.info("Job \"%s\" with id %s done", job_name, job_id)
		if job_name == 'Import aggregate data':
			logging.info("Import of aggregate data completed.")
		else:
		# unknown job
			logging.error("Unknown MapReduce job \"%s\"", job_name)


#MARK: Pre-defined Mapper jobs


def split_aggregate_output(s):
	""" split aggregated MapReduce output into a dict
	      
	Split s into key and value, using the ':' character as a delimiter

	Args:
	s: string to be split.
		Should be of the format: 0000_YY-mm-ddTMM:HH, where 0000 is the detector id
		Note: TMM:HH may not be included for single-day intervals
	Returns:
		dictionary representing detectory entry
	"""
	# the key and value from MR job 1 is separated by the ":" character
	# split there, and keep track of each half
	(key, value) = s.split(":")
	# the detector and date/time stamp are delimited by "_"
	(det, datestamp) = key.split("_")
	timestamp = None
	# speed sum and count values are split with a "," character
	(speed_sum, speed_count) = value.split(",")
	# if this datestamp includes the time as well, it needs to be extracted
	# split on the "T" character (as used in ISO 8601 datetimes)
	if re.search("T", datestamp):
		# the datestamp includes a time interval - extract it
		(datestamp, timestamp) = datestamp.split("T")

	# create a detectory entry dictionary, and return
	entry = {'detector': det, 'datestamp':datestamp, 'timestamp':timestamp, 'sum': speed_sum, 'count': speed_count}
	return entry


def import_aggregate_data(entity):
	""" Method defined for "Import aggregate data" MR job specified in mapreduce.yaml
		
		Args:
		entity: entity to process as string. Should be a text file.
	"""
	ctx = context.get()
	params = ctx.mapreduce_spec.mapper.params
	blob_key = params['blob_keys']
	
	# get the blob_keys that were passed to this function to determine filename
	blob_reader = blobstore.BlobReader(blob_key, buffer_size=1048576)
	if blob_reader:
		# the filename helps us to determine what interval this data is for
		interval = None
		if re.search('daily', blob_reader.blob_info.filename):
			interval = 'daily_speed'
		elif re.search('hourly', blob_reader.blob_info.filename):
			interval = 'hourly_speed'
		elif re.search('fifteen', blob_reader.blob_info.filename):
			interval = 'fifteenmin_speed'
		elif re.search('five', blob_reader.blob_info.filename):
			interval = 'fivemin_speed'
		
		# get the entity passed to this mapper function (one line, delimited by \n)
		(byte_offset, line_value) = entity

		#logging.info("Got line:%s", line)
		detectorentry_dict = split_aggregate_output(line_value)
		# the entry has a time stamp - likely hourly, 15 min, or 5 min count
		timestamp = None
		if detectorentry_dict['timestamp']:
			# there is a timestamp - set it for the SpeedSum object
			if interval == 'hourly_speed':
				timestamp = datetime.datetime.strptime(detectorentry_dict['timestamp'], "%H").time()
			else:
				timestamp = datetime.datetime.strptime(detectorentry_dict['timestamp'], "%H.%M").time()
		else:
			# there is no timestamp - likely a daily sum
			timestamp = datetime.time(0,0)

		entry = DetectorEntry.query(DetectorEntry.date == datetime.datetime.strptime(detectorentry_dict['datestamp'], "%Y-%m-%d").date(),
									DetectorEntry.detectorid == int(detectorentry_dict['detector'])).get()
		speed_sum = None
		if entry != None:
			# an entry for this detectory entry exists - fill in SpeedSum values
			speed_sum = SpeedSum(time=timestamp,
									sum = int(detectorentry_dict['sum']),
									count = int(detectorentry_dict['count']))

			if interval == 'daily_speed':
				# this is a daily_speed (non-repeated) property
				setattr(entry, 'daily_speed', speed_sum)
			elif interval == 'hourly_speed':
				# this is a hourly_speed (repeated) property
				entry.hourly_speed.append(speed_sum)
			elif interval == 'fifteenmin_speed':
				# this is a fifteenmin_speed (repeated) property
				entry.fifteenmin_speed.append(speed_sum)
			elif interval == 'fivemin_speed':
				# this is a fivemin_speed (repeated) property
				entry.fivemin_speed.append(speed_sum)
			else:
				logging.error("Unknown interval:%s", interval)
		else:
			# a DetectorEntry does not exist for this detector/date combo - create one...
			entry = DetectorEntry(date = datetime.datetime.strptime(detectorentry_dict['datestamp'], "%Y-%m-%d").date(),
									detectorid = int(detectorentry_dict['detector']))
			speed_sum = SpeedSum(time = timestamp,
									sum = int(detectorentry_dict['sum']),
									count = int(detectorentry_dict['count']))
			
			if interval == 'daily_speed':
				# this is a daily_speed (non-repeated) property
				setattr(entry, 'daily_speed', speed_sum)
			elif interval == 'hourly_speed':
				# this is a hourly_speed (repeated) property
				entry.hourly_speed.append(speed_sum)
			elif interval == 'fifteenmin_speed':
				# this is a fifteenmin_speed (repeated) property
				entry.fifteenmin_speed.append(speed_sum)
			elif interval == 'fivemin_speed':
				# this is a fivemin_speed (repeated) property
				entry.fivemin_speed.append(speed_sum)
			else:
				logging.error("Unknown interval:%s", interval)

		yield op.db.Put(entry)
	else:
		logging.error("No blob was found for key %s", blob_key)


app = webapp2.WSGIApplication(
    [
        ('/done', DoneHandler),
        ('/map_reduce', IndexHandler)
    ],
    debug=True)
