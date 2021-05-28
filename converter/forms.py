from django import forms
from django.conf import settings
from django.template.defaultfilters import filesizeformat
from django.utils.translation import ugettext_lazy as _
from django.core.validators import URLValidator
from django.core.exceptions import ValidationError

from captcha.fields import ReCaptchaField
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout, Submit, Row, Column
from crispy_forms.bootstrap import Tab, TabHolder
from crispy_forms.layout import Div

import requests

convert_choises = (
		('', '-Select-'), 
		('json', 'JSON'), 
		('parquet', 'Parquet')
	)

class UploadFileForm(forms.Form):
	# title = forms.CharField(label=_('Title'), widget=forms.TextInput(attrs={'required':''}), required=True)
	convert_from = forms.ChoiceField( label='Convert From', choices = convert_choises )
	convert_to = forms.ChoiceField( label='Convert To', choices = convert_choises )
	url = forms.CharField( label='', required=False, widget=forms.TextInput(attrs={'placeholder': 'http://www.example.com/.../source.json'}))
	file = forms.FileField( label='', required=False )
	captcha = ReCaptchaField()

	JSON_FILE_EXT = ['json','txt']
	PARQUET_FILE_EXT = ['parquet']

	def clean(self):
		cleaned_data = super().clean()
		clean_convert_from = cleaned_data.get('convert_from')
		clean_convert_to = cleaned_data.get('convert_to')
		clean_url = cleaned_data.get('url')
		clean_file = self.cleaned_data['file']

		if (clean_convert_from == clean_convert_to):
			raise forms.ValidationError('You are trying to convert to same format, choose a different format.')
		
		if clean_file:
			# file_type = clean_file.content_type.split('/')[0]
			# print (file_type) #text
			print ('clean_file.content_type' + clean_file.content_type) #text/plain = json, application/octet-stream(binary file) = parquet
			# print (file_content.name) #json_payload2.txt
			print (clean_file.name.split('.')) #['json_payload2', 'txt']

			if len(clean_file.name.split('.')) == 1:
				raise forms.ValidationError("File type is not supported.")

			if (clean_convert_from == "json"):
				self.is_valid_json(clean_file.name, clean_file.content_type, "text")
			elif (clean_convert_from == "parquet"):
				self.is_valid_parquet(clean_file.name, clean_file.content_type, "application")
			# if clean_file.size > settings.MAX_UPLOAD_SIZE:
			# 	raise forms.ValidationError(_('Please keep filesize under %s. Current filesize %s') % (filesizeformat(settings.MAX_UPLOAD_SIZE), filesizeformat(file_content.size)))
		
		elif clean_url:
			h = requests.head(clean_url, allow_redirects=True)
			header = h.headers
			content_type = header.get('content-type')

			if (clean_convert_from == "json"):
				self.is_valid_json(clean_url, content_type, "text")
			elif (clean_convert_from == "parquet"):
				self.is_valid_parquet(clean_url, content_type, "text")

			validator = URLValidator()
			try:
				validator(clean_url)
			except ValidationError:
				raise ValidationError(_("Please enter a valid URL"))

		else:
			raise forms.ValidationError("Please upload a file")

	def is_valid_json(self, content, content_type, target_type):
		if not (content.split('.')[-1] in self.JSON_FILE_EXT and target_type in content_type.lower()):
			raise forms.ValidationError("Only 'txt' and 'json' files are allowed.")

	def is_valid_parquet(self, content, content_type, target_type):
		if not (content.split('.')[-1] in self.PARQUET_FILE_EXT and target_type in content_type.lower()):
			raise forms.ValidationError("Only 'parquet' files are allowed.")

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.helper = FormHelper()

		# Enable Abide validation on the form
		# self.helper.attrs = {'data_abide': '', 'novalidate': ''}
		# self.fields['title'].abide_msg = "This field is required !"

		self.helper.layout = Layout(
			# 'title',
			Row(
				Column('convert_from', css_class='form-group col-md-6 mb-0'),
				Column('convert_to', css_class='form-group col-md-6 mb-0'),
				css_class='form-row'
			),
			# Submit('submit', 'Sign in')
		)
		self.helper.layout.append(
			TabHolder(
				Tab('Source URL',
					'url'
				),
				Tab('Upload File',
					'file'
				),
			)			
		)
		self.helper.layout.append(
			Div('captcha', css_class = 'captcha-stuff')
		)
		self.helper.layout.append(
			Submit('submit', 'Convert', css_class='btn btn-primary btn-theme'),
		)
