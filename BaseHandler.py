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
import os
import webapp2
import jinja2

class BaseHandler(webapp2.RequestHandler):
	""" base RequestHandler class definition
	
	This class defines the base RequestHandler for subclasses that will be used
	for outputting HTML content. Specifically, the jinja2 template system is used.
	
	jinja2 documentation: http://jinja.pocoo.org/docs/
	"""

	def render_template(self, file, template_args):
		""" render the contents of an HTML file

		This function renders the contents of an HTML file located at /templates,
		substituting the contents of tokens in that document with the arguments specified
		in the passed dictionary.
		
		Args:
			file: the name of a file located inside of the /templates folder
			template_args: a dictionary of key/value pairs
		"""
		JINJA_ENVIRONMENT = jinja2.Environment(
										   loader=jinja2.FileSystemLoader(os.path.dirname(__file__)+'/templates'),
										   extensions=['jinja2.ext.autoescape'],
										   autoescape=True)
		template = JINJA_ENVIRONMENT.get_template(file)
		self.response.write(template.render(template_args))
