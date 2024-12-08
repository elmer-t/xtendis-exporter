
import pyodbc
import logging

import settings
from typing import List 

settings = settings.Settings()

class ResultSet:
	def __init__(self, fields: List[str], rows: List[pyodbc.Row]):
		self.fields = fields
		self.rows = rows

	def __str__(self):
		return f"Fields: {self.fields}\nRows: {self.rows}"
	
	def to_dict(self) -> dict:
		return [dict(zip(self.fields, row)) for row in self.rows]
	
class Database:
	
	connection = None
	logger = None

	def __init__(self):

		self.logger = logging.getLogger(__name__)
		
		conn_string = "DRIVER={{SQL Server}}; \
						SERVER={settings.DATABASE_SERVER}; \
						UID={settings.DATABASE_USERNAME}; \
						PWD={settings.DATABASE_PASSWORD}; \
						DATABASE=X10DBASE;".format(settings=settings)

		self.connection = pyodbc.connect(conn_string)
		self.logger.info("Connected to database")
		self.logger.debug(f"Connection string: {conn_string}")

	def get_archives(self, archive_number: int = None) -> ResultSet:
		query = "SELECT ARCHIEFID, NAAM FROM X10DBASE.dbo.ARCHIEF"

		if archive_number != None:
			query += f" WHERE ARCHIEFID = {archive_number} "
		
		query += " ORDER BY 1"
		return self.query(query)
	
	def _get_index_fields(self, archive_number: int = None) -> ResultSet:
		if archive_number == None:
			Exception("archive_number is required")

		query = f"SELECT [WAARDENKOLOMNAAM], REPLACE([NAAM], ' ', '_'), [WAARDENTABELNAAM] \
			FROM X10DBASE.dbo.[INDEXDEFINITIE] \
				WHERE [DOCUMENTARCHIEFID] = {archive_number}"

		return self.query(query)
	
	def get_documents(self, document_number: int = None) -> ResultSet:
		if document_number == None:
			Exception("document_number is required")

		# Default join table, must always be included in the joins
		ref_tables = [{"Name": f"DOCUMENT_{document_number}", "Alias": "d", "LocalKey": "OBJECTID", "ForeignKey": "OBJ_ID", "ForeignAlias": "o"}]

		# Some fields are named INDEXFIELD_1, INDEXFIELD_2, etc. We need to rename them to the actual field name
		index_field_rows = self._get_index_fields(document_number).rows
		index_fields = ""

		for index_field in index_field_rows:
			index_fields += f",[{index_field[0]}] AS [{index_field[1]}]"

		query = f"SELECT d.DOCUMENTID, d.DOCUMENTGUID, so.PAGE_NR, so.FILE_NR, so.TYPE_ID, t.FILE_TYPE, \
			 d.STATUS, d.GEBRUIKERID, g.[NAAMVOLUIT] + ' (' + g.NAAM + ')' AS GEBRUIKER, \
				d.AANMAAKDATUM, d.MUTATIEDATUM, d.INDEXEERDATUM "
		
		if index_field_rows:
			query += index_fields

			# Join tables are the unique values in index_field_rows[2]
			for idx, idx_field in enumerate(index_field_rows):

				# Check if the table is already in the list
				if not any(d['Name'] == idx_field[2] for d in ref_tables):
					
					local_key = "OBJECTID"
					foreign_key = "OBJ_ID"

					if idx_field[2].startswith("MVDOC"):
						local_key = "DOCUMENTID"
						foreign_key = "DOCUMENTID"
					
					ref_tables.append({"Name": idx_field[2], "Alias": f"x{idx}", "LocalKey": local_key, "ForeignKey": foreign_key, "ForeignAlias": f"d"})
			

		query += f"FROM \
	OBJECTMANAGER.dbo.A{document_number}SUBOBJECT so INNER JOIN \
	OBJECTMANAGER.dbo.A{document_number}OBJECT o ON o.OBJ_ID=so.OBJ_ID INNER JOIN"
		
		for ref_table in ref_tables:
			query += f" X10DBASE.dbo.{ref_table['Name']} {ref_table['Alias']} ON {ref_table['Alias']}.{ref_table['LocalKey']}={ref_table['ForeignAlias']}.{ref_table['ForeignKey']} INNER JOIN "

		query += "OBJECTMANAGER.dbo.SUBOBJ_TYPE_LOOKUP t ON so.TYPE_ID=t.TYPE_ID INNER JOIN \
				X10DBASE.dbo.GEBRUIKER g ON d.GEBRUIKERID=g.GEBRUIKERID \
			  ORDER BY 1 DESC"

		return self.query(query)

	def get_notes(self, archive_nr: int, document_id: int) -> ResultSet:
		query = f"SELECT n.[GEBRUIKERID], n.[AANMAAKDATUM], n.[MUTATIEDATUM], n.[TEKST], \
				g.[NAAMVOLUIT] + ' (' + g.NAAM + ')' AS GEBRUIKER \
			FROM X10DBASE.dbo.NOTITIE_{archive_nr} n INNER JOIN \
				X10DBASE.dbo.GEBRUIKER g ON n.GEBRUIKERID=g.GEBRUIKERID \
				WHERE [DOCUMENTID] = {document_id} \
					ORDER BY MUTATIEDATUM DESC"
		return self.query(query)
	
	def get_audit_log(self, document_guid: str) -> ResultSet:
		# query = f"SELECT *, \
	  	# 	case WHEN (INDEXWAARDE_16 LIKE 'noteId:%') THEN \
		# 		(SELECT TEKST FROM NOTITIE_12 WHERE NOTITIEID=CONVERT(int, RIGHT(INDEXWAARDE_16, CHARINDEX(':', INDEXWAARDE_16)-2))) \
		# 		ELSE '' END AS NOTE \
		# 	FROM X10DBASE.dbo.DOCUMENT_5 \
		# 	WHERE INDEXWAARDE_11 = {document_id} \
		# 		ORDER BY AANMAAKDATUM"


		query = f"""
		SELECT d.[DOCUMENTID],
			d.[GEBRUIKERID],
			g.[NAAMVOLUIT] + ' (' + g.NAAM + ')' AS GEBRUIKER,
			d.[AANMAAKDATUM],
			d.[MUTATIEDATUM],
			d.[INDEXEERDATUM],
			d.[OBJECTID],
			d.[BRON],
			d.[INDEXWAARDE_10] AS Entiteit,
			d.[INDEXWAARDE_11] AS EntiteitId,
			d.[INDEXWAARDE_12] AS UitvoerendeId,
			d.[INDEXWAARDE_13] AS Handeling,
			d.[INDEXWAARDE_14] AS Resultaat,
			d.[INDEXWAARDE_15] AS Actor,
			d.[INDEXWAARDE_16] AS Melding,
			d.[INDEXWAARDE_17] AS ArchiefId,
			d.[INDEXWAARDE_75] AS ChangeInfoXml,
			d.[INDEXWAARDE_76] AS ChangeInfoList,
			CASE
				WHEN (INDEXWAARDE_16 LIKE 'noteId:%') THEN CASE
					WHEN [INDEXWAARDE_17] = 7 THEN (
						SELECT TEKST
						FROM NOTITIE_7
						WHERE NOTITIEID = CONVERT(int, REPLACE(INDEXWAARDE_16, 'noteId:', ''))
					)
					WHEN [INDEXWAARDE_17] = 12 THEN (
						SELECT TEKST
						FROM NOTITIE_12
						WHERE NOTITIEID = CONVERT(int, REPLACE(INDEXWAARDE_16, 'noteId:', ''))
					)
					ELSE '---'
				END
				ELSE ''
			END AS NOTE
		FROM [X10DBASE].[dbo].[DOCUMENT_5] d INNER JOIN
			[X10DBASE].[dbo].[GEBRUIKER] g ON d.[GEBRUIKERID] = g.[GEBRUIKERID]
		where d.CORRELATIEGUID = '{document_guid}'
		order by d.AANMAAKDATUM
		"""

		return self.query(query)

	def query(self, query: str) -> ResultSet:
		cursor = self.connection.cursor()
		cursor.execute(query)
		fields = [ i[0] for i in cursor.description ]

		return ResultSet(fields, cursor.fetchall())

	def __del__(self):
		if self.connection:
			self.connection.close()