import webapp2
import logging
import re
import zipfile
import csv

from google.appengine.ext import blobstore
from google.appengine.ext import ndb

from google.appengine.api import files
from google.appengine.api import taskqueue

from mapreduce import operation as op
from mapreduce import context
from mapreduce import model
from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
from mapreduce import shuffler
from mapreduce import util

from FileMetadata import FileMetadata

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
    input_blob_key = params['blob_key']
    logging.info("Input blob_key:%s", input_blob_key)
	
	# use MR state to get job name
    job_name = state.mapreduce_spec.name
    logging.info("Got job name:%s", job_name)
	
    logging.info("Import for job %s done" % job_id)
    if job_name == 'Perform daily speed sum' or job_name == 'Perform hourly speed sum':
      counters = state.counters_map.counters
      # Remove counters not needed for stats
      if 'mapper-calls' in counters.keys():
  		del counters['mapper-calls']
      if 'mapper-walltime-ms' in counters.keys():
  		del counters['mapper-walltime-ms']
		
      outputString = ''
	  
	  # Get the blob_key for the input FileMetadata object
      file_meta_key = ndb.Key(FileMetadata, input_blob_key)
	  
	  # Get the FileMetadata object from the Datastore
      file_meta = file_meta_key.get()

      for counter in counters.keys():
        outputString += "%s: %s\n" % (counter, counters[counter])

      # Create the file
      file_name = files.blobstore.create(mime_type='application/octet-stream',_blobinfo_uploaded_filename=input_blob_key+"_"+counter)

      # Open the file and write to it
      with files.open(file_name, 'a') as f:
        f.write(outputString)

      # Finalize the file. Do this before attempting to read it.
      files.finalize(file_name)

      # Get the file's blob key
      logging.info(blobstore.BlobKey(file_name))
      blob_key = files.blobstore.get_blob_key(file_name)

      # Set the FileMetadata object's '*_speed_sum' property to blob_key
      if job_name == 'Perform daily speed sum':
        setattr(file_meta, 'daily_speed_sum', blob_key)
      elif job_name == 'Perform hourly speed sum':
        setattr(file_meta, 'hourly_speed_sum', blob_key)

      # Put the FileMetadata object back in the Datastore
      file_meta.put()
    else:
		# unknown job
		logging.error("Unknown MapReduce job \"%s\"", job_name)


#MARK: Pre-defined MapReduce jobs


def daily_speed_sum(entity):
	""" Method defined for "Perform daily speed sum" MR job specified in mapreduce.yaml
		
		Args:
		entity: entity to process as string. Should be a zip archive with
		text files inside.
		"""
	ctx = context.get()
	params = ctx.mapreduce_spec.mapper.params
	blob_key = params['blob_key']
	
	logging.info("Got key:%s", blob_key)
	blob_reader = blobstore.BlobReader(blob_key, buffer_size=1048576)
	if blob_reader:
		logging.info("Got filename:%s", blob_reader.blob_info.filename)
		fw_ld = re.search('freeway_loopdata.*', blob_reader.blob_info.filename)
		if fw_ld:
			if blob_reader.blob_info.content_type == "application/zip":
				logging.info("Got content type:%s", blob_reader.blob_info.content_type)
				zip_file = zipfile.ZipFile(blob_reader)
				file = zip_file.open(zip_file.namelist()[0])
			elif blob_reader.blob_info.content_type == "text/plain":
				logging.info("Got content type:%s", blob_reader.blob_info.content_type)
				file = blob_reader
			else:
				logging.info("Unrecognized content type:%s", blob_reader.blob_info.content_type)
			
			if file:
				csv_reader = csv.DictReader(file)
				csv_headers = csv_reader.fieldnames
				if 'speed' in csv_headers:
					for line in csv_reader:
						#logging.info("Got line:%s", line)
						date = re.search('(2011-..-..) .*', line['starttime'])
						if line['speed'] != '' and date:
							yield op.counters.Increment('%s_%s_speed_count' % (line['detectorid'], date.group()[:10]))
							yield op.counters.Increment('%s_%s_speed_sum' % (line['detectorid'], date.group()[:10]), int(line['speed']))
				else:
					logging.error("No field named 'speed' found in CSV headers:%s", csv_headers)
	else:
		logging.error("No blob was found for key %s", blob_key)


def hourly_speed_sum(entity):
	""" Method defined for "Perform hourly speed sum" MR job specified in mapreduce.yaml
		
		Args:
		entity: entity to process as string. Should be a zip archive with
		text files inside.
		"""
	ctx = context.get()
	params = ctx.mapreduce_spec.mapper.params
	blob_key = params['blob_key']
	
	logging.info("Got key:%s", blob_key)
	blob_reader = blobstore.BlobReader(blob_key, buffer_size=1048576)
	if blob_reader:
		logging.info("Got filename:%s", blob_reader.blob_info.filename)
		fw_ld = re.search('freeway_loopdata.*', blob_reader.blob_info.filename)
		if fw_ld:
			if blob_reader.blob_info.content_type == "application/zip":
				logging.info("Got content type:%s", blob_reader.blob_info.content_type)
				zip_file = zipfile.ZipFile(blob_reader)
				file = zip_file.open(zip_file.namelist()[0])
			elif blob_reader.blob_info.content_type == "text/plain":
				logging.info("Got content type:%s", blob_reader.blob_info.content_type)
				file = blob_reader
			else:
				logging.info("Unrecognized content type:%s", blob_reader.blob_info.content_type)
			
			if file:
				csv_reader = csv.DictReader(file)
				csv_headers = csv_reader.fieldnames
				if 'speed' in csv_headers:
					for line in csv_reader:
						#logging.info("Got line:%s", line)
						date = re.search('(2011-..-.. ..):.*', line['starttime'])
						if line['speed'] != '' and date:
							yield op.counters.Increment('%s_%s_speed_count' % (line['detectorid'], re.sub(' ', 'T', date.group()[:13])))
							yield op.counters.Increment('%s_%s_speed_sum' % (line['detectorid'], re.sub(' ', 'T', date.group()[:13])), int(line['speed']))
				else:
					logging.error("No field named 'speed' found in CSV headers:%s", csv_headers)
	else:
		logging.error("No blob was found for key %s", blob_key)


app = webapp2.WSGIApplication(
    [
        ('/done', DoneHandler),
        ('/map_reduce', IndexHandler)
    ],
    debug=True)
