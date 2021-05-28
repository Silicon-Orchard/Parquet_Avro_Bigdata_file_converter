from django.shortcuts import render
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.conf import settings
from json2parquet import convert_json
from django.contrib import messages

import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa
import json
import requests

# function to render an upload form.
from .forms import UploadFileForm

import time

# Create your views here.
FILE_TYPES = ['json', 'txt']

class FileLocation:
	external_input_url = ''

	def __init__(self, input_type, output_type, external_input_url = None):
		self.input_type = input_type
		self.output_type = output_type
		if external_input_url is not None:
			self.external_input_url = external_input_url

		file_name = str(int(time.time()))

		if input_type == 'json':
			self.input_root = settings.JSON_IN_ROOT + file_name +'.json'
			self.input_url = settings.JSON_IN_URL + file_name +'.json'
		elif input_type == 'parquet':
			self.input_root = settings.PARQUET_IN_ROOT + file_name +'.parquet'
			self.input_url = settings.PARQUET_IN_URL + file_name +'.parquet'

		if output_type == 'json':
			self.output_root = settings.JSON_OUT_ROOT + file_name +'.json'
			self.output_url = settings.JSON_OUT_URL + file_name +'.json'
		elif output_type == 'parquet':
			self.output_root = settings.PARQUET_OUT_ROOT + file_name +'.parquet'
			self.output_url = settings.PARQUET_OUT_URL + file_name +'.parquet'

def converter_view(request):
	if request.method == 'POST':
		form = UploadFileForm(request.POST, request.FILES)
		if form.is_valid():
			#file_name = str(int(time.time()))
			print (request.POST.get("url", ""))
			if(request.POST.get("url", "") == ""):
				if is_uploadable_size(request.FILES['file']): 					
					file_location = FileLocation(request.POST.get("convert_from", ""), request.POST.get("convert_to", ""))
					handle_uploaded_file(request.FILES['file'], file_location.input_root)
				else:
					messages.add_message(request, messages.INFO, 'subscribe', extra_tags='show_subscribe')
					return render(request, 'converter/upload_data.html', {'form': form})
			else:
				if is_downloadable_size(request.POST.get("url", "")): 
					file_location = FileLocation(request.POST.get("convert_from", ""), request.POST.get("convert_to", ""), request.POST.get("url", ""))	
					handle_download_file(file_location.external_input_url, file_location.input_root)
				else:
					messages.add_message(request, messages.INFO, 'subscribe', extra_tags='show_subscribe')
					return render(request, 'converter/upload_data.html', {'form': form})

			if file_location.input_type == 'json' and file_location.output_type == 'parquet':
				convert_json(file_location.input_root, file_location.output_root)
				# if (file_location.external_input_url == ''):
				# 	convert_json(file_location.input_root, file_location.output_root)
				# else:
				# 	convert_json(file_location.external_input_url, file_location.output_root)
			elif file_location.input_type == 'parquet' and file_location.output_type == 'json':
				parquet_to_json(file_location.input_root, file_location.output_root)
				# if (file_location.external_input_url == ''):
				# 	parquet_to_json(file_location.input_root, file_location.output_root)
				# else:
				# 	parquet_to_json(file_location.external_input_url, file_location.output_root)

			return render(request, 'converter/upload_done.html', {'output_type': file_location.output_type, 'file_path': file_location.output_url})
	else:
		form = UploadFileForm()
		# messages.add_message(request, messages.INFO, 'subscribe', extra_tags='show_subscribe')
	return render(request, 'converter/upload_data.html', {'form': form})

def is_uploadable_size(file):
	if file.size > settings.MAX_UPLOAD_SIZE:
		return False
	else:
		return True

def handle_uploaded_file(file, file_path):
	with open(file_path, 'wb+') as destination:
		for chunk in file.chunks():
			destination.write(chunk)
	print ("Downloaded complete!")
	return

def parquet_to_json(parquet_file_path, json_file_path):
	# Read parquet data:
	table = pq.read_table(parquet_file_path)
	# Creating a dataframe
	dataframe = table.to_pandas()
	# Converting to JSON
	dataframe.to_json(json_file_path,index=False,orient='table')

def handle_download_file(url, file_path):
	# create HTTP response object
	response = requests.get(url, allow_redirects=True, stream = True)
	# open(file_path, 'wb').write(response.content)
	with open(file_path,"wb") as data_file: 
		for chunk in response.iter_content(chunk_size=1024): 
			 # writing one chunk at a time to data_file 
			 if chunk: 
				 data_file.write(chunk) 

# Does the url contain a downloadable resource
def is_downloadable_content(url):
	h = requests.head(url, allow_redirects=True)
	header = h.headers
	content_type = header.get('content-type')
	if 'text' in content_type.lower():
		return False
	if 'html' in content_type.lower():
		return False
	return True

def is_downloadable_size(url):
	h = requests.head(url, allow_redirects=True)
	header = h.headers
	content_length = header.get('content-length', None)
	print ("length: " + content_length)
	if content_length and int('0' + content_length) > settings.MAX_UPLOAD_SIZE:
		return False
	else:
		return True
