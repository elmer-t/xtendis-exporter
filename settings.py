import os
from dotenv import load_dotenv

DATABASE_SERVER = 'localhost'
DATABASE_USERNAME = 'sa'
DATABASE_PASSWORD = 'your_password'

WINDOWS_USERNAME = 'your_windows_username'
WINDOWS_PASSWORD = 'your_windows_password'

EXPORT_FOLDER = 'export'

class Settings:
	def __init__(self):
		load_dotenv()
		
		self.DATABASE_SERVER = os.getenv('database_server')
		self.DATABASE_USERNAME = os.getenv('database_username')
		self.DATABASE_PASSWORD = os.getenv('database_password')

		self.WINDOWS_USERNAME = os.getenv('windows_username')
		self.WINDOWS_PASSWORD = os.getenv('windows_password')

		self.EXPORT_FOLDER = os.getenv('export_folder')