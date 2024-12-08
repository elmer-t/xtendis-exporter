import os
import requests.cookies
import database
import requests
import logging
import json
import pyodbc
import decimal

from typing import List 

from requests_ntlm import HttpNtlmAuth
from requests_negotiate_sspi import HttpNegotiateAuth
from requests.auth import HTTPDigestAuth
from tqdm import tqdm
from time import sleep
import settings

settings = settings.Settings()

logging.basicConfig(
	level=logging.DEBUG, 
	format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
	filename='xtendis_export.log', 
	filemode='w'
)
logger = logging.getLogger(__name__)

db = database.Database()
session = None


archive_key_fields = {
	1: "DOCUMENTID",
	2: "DOCUMENTID",
	3: "DOCUMENTID",
	4: "DOCUMENTID",
	5: "DOCUMENTID",
	6: "Interne_referentie",
	7: "Boekstuknummer",
	8: "DOCUMENTID",
	9: "DOCUMENTID",
	10: "DOCUMENTID",
	11: "DOCUMENTID",
	12: "Interne_referentie"
}

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super().default(o)
	
def main():
	global session

	logger.info("Starting export")

	# create Xtendis session
	session = login_to_website("https://dms.actamarine.com/Xtendis.web/")
	if session is None:
		logger.error("Failed to login to Xtendis")
		return
	
	archives = db.get_archives(12)
	logger.info(f"Found {len(archives.rows)} archives")

	for archive in tqdm(archives.rows):
		logger.info(f"Processing archive {archive[0]}-{archive[1]}")

		documents = db.get_documents(archive[0])
		logger.info(f"Found {len(documents.rows)} documents in archive {archive[0]}")

		doc_id = 0
		for document in tqdm(documents.rows, desc=f"Archive {archive[0]}"):

			download_file(archive, document)
			safe_metadata(archive, document, documents.fields)
			
			# Save audit log for financial documents, if document changes (don't save for all pages)
			if doc_id != document.DOCUMENTID and archive[0] == 12: # Financieel
				safe_audit_log(archive, document)

			logger.info(f"Processed document {document.DOCUMENTID}")

			doc_id = document.DOCUMENTID

def _folder_name(archive_nr: int, archive_name: str, row: List[pyodbc.Row]):
	key_field = row.__getattribute__(archive_key_fields[archive_nr])

	year = row.INDEXEERDATUM.year
	month = row.INDEXEERDATUM.month
	
	return f"{settings.EXPORT_FOLDER}/archief {str(archive_nr).zfill(2)} - {archive_name}/{year}/{month}/{key_field}"

def _file_name(archive_nr: int, row: List[pyodbc.Row]):
	key_field = row.__getattribute__(archive_key_fields[archive_nr])
	return f"{key_field}"

def safe_audit_log(archive: database.ResultSet, row: List[pyodbc.Row]):

	folder = _folder_name(archive.ARCHIEFID, archive.NAAM, row)
	file_name = f"{_file_name(archive.ARCHIEFID, row)}-audit.json"

	logger.info(f"Saving audit log for document {row.DOCUMENTGUID} in {folder}/{file_name}")

	log = db.get_audit_log(row.DOCUMENTGUID).to_dict()

	if not os.path.exists(folder):
		os.makedirs(folder)

	with open(f"{folder}/{file_name}", "wb") as file:
		
		# Write to file
		file.write(json.dumps(log, indent=4, default=str, cls=DecimalEncoder).encode("utf-8"))

	

def	safe_metadata(archive: database.ResultSet, row: List[pyodbc.Row], fields: List[str]):
	# Safe metadata in XML or JSON format
	folder = _folder_name(archive.ARCHIEFID, archive.NAAM, row)
	file_name = f"{_file_name(archive.ARCHIEFID, row)}.json"

	logger.info(f"Saving metadata for document {row.DOCUMENTID} in {folder}/{file_name}")

	if not os.path.exists(folder):
		os.makedirs(folder)
	
	with open(f"{folder}/{file_name}", "wb") as file:
			
			# Convert row to dictionary
			r = [dict((fields[i], value) for i, value in enumerate(row))]
			
			# Get notes and add to dictionary
			notes = db.get_notes(archive.ARCHIEFID, row.DOCUMENTID).to_dict()
			r[0]["Notes"] = notes
			
			# Write to file
			file.write(json.dumps(r, indent=4, default=str, cls=DecimalEncoder).encode("utf-8"))


def download_file(archive: database.ResultSet, row: database.ResultSet):
	
	extension = row.FILE_TYPE.lower()
	folder = _folder_name(archive.ARCHIEFID, archive.NAAM, row)
	file_name = f"{_file_name(archive.ARCHIEFID, row)}-{str(row.PAGE_NR).zfill(2)}"
	
	if row.FILE_TYPE == "TIF":
		url = f"https://dms.actamarine.com/xtendis.web/services/httphandler.ashx/TiffpageAsPNG?&archiefid={archive.ARCHIEFID}&documentid={row.DOCUMENTID}&paginanummer={row.PAGE_NR}&filenummer={row.FILE_NR}&date=&contenttype=TIF&maxsize=1600"
		extension = "png"
	else:
		url = f"https://dms.actamarine.com/Xtendis.Web/services/httphandler.ashx/page?sessie={settings.XTENDIS_SESSION_ID}&archiefId={archive.ARCHIEFID}&documentid={row.DOCUMENTID}&paginanummer={row.PAGE_NR}&contenttype=image_tiff&filenummer={row.FILE_NR}&attachment=1&filename={file_name}"

	logger.info(f"Downloading file {file_name}.{extension}")

	response = session.get(url)

	if response.status_code == 200:
		# Create directory if it does not exist
		if not os.path.exists(folder):
			os.makedirs(folder)

		with open(f"{folder}/{file_name}.{extension}", "wb") as file:
			logger.info(f"Saving file {file_name}.{extension}")
			file.write(response.content)

	else:
		logger.error(f"Error downloading file: {response.status_code}: {response.reason}")

def login_to_website(url: str) -> requests.Session:
	try:

		logger.info(f"Logging in to {url} using Windows Authentication account name {settings.WINDOWS_USERNAME}")

		# Create a session to maintain cookies
		session = requests.Session()

		# Perform authentication using Windows Authentication (SSPI)
		response = session.get(
			url, 
			auth=HttpNegotiateAuth(
				domain="AD",
				username=settings.WINDOWS_USERNAME, 
				password=settings.WINDOWS_PASSWORD,
				
			),  # Windows Authentication
			verify=True  # Set to False if dealing with SSL certificate issues
		)

		# Check if authentication was successful
		response.raise_for_status()
		logger.info("Authentication successful")

		sleep(3)

		# Now you can use the session for subsequent requests
		# The session will maintain the authentication and cookies
		return session

	except requests.exceptions.RequestException as e:
		logger.error(f"Authentication error: {e}")
		return None
	
if __name__ == "__main__":
	main()