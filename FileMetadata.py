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
from google.appengine.ext import ndb

class FileMetadata(ndb.Model):
	"""A helper class that will hold metadata for uploaded blobs.
	
	Keep track of where a file is uploaded to, along with other file statistics.
	"""
	content_type = ndb.StringProperty()
	creation = ndb.DateTimeProperty()
	filename = ndb.StringProperty()
	size = ndb.IntegerProperty()
	md5_hash = ndb.StringProperty()
	blobkey = ndb.StringProperty()
	daily_speed_sum = ndb.BlobKeyProperty()
	hourly_speed_sum = ndb.BlobKeyProperty()
	fifteen_min_speed_sum = ndb.BlobKeyProperty()
	five_min_speed_sum = ndb.BlobKeyProperty()
