# Xtendis Export Project
This project is designed to export data from the Xtendis document management system (DMS). 
It extracts documents via the Xtendis web UI and stores them in the export folder. This means Xtendis needs to be running for extraction to work.

Meta- and audit data is stored in JSON-files. Extracted files are organised per year and month and each document has its own folder. Each document page is extracted into its own file. TIF-files are converted to PNG.

As an optional second step, `pdf-collector.py` can be used to collect all PNG-files and create one single PDF-file per invoice. Meta data is added to the last page as JSON.

(C) 2024