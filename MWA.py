from time import sleep
import httplib
import json
from pprint import pprint
import pypyodbc as pyodbc
from datetime import datetime
print("Starting...")
i = 0
while True:
	sleep(60)
	conn = httplib.HTTPConnection("drvrapp.net")

	headers = {
		'content-type': "application/json",
		'drvr-session': "0c7dd60f-904f-4f54-bac8-b7b9cf221cd1",
		'cache-control': "no-cache",
		'postman-token': "0f62fb7a-7377-34d4-8e21-ba0f87f6e55e"
		}

	conn.request("GET", "/mwa/jobs/01?from=20170409&to=20170410", headers=headers)

	res = conn.getresponse()
	data = res.read()
	jsonData = json.loads(data.decode('utf-8'))
	#print(data.decode("utf-8"))
	with open('test.json', 'w') as f:
		json.dump(jsonData, f)

	#print('loop end')
	#creating connection Object which will contain SQL Server Connection  
	connection = pyodbc.connect('Driver={SQL Server};''Server=Labtop;''Database=Testdb;')  
	print("Connection Successfully Established") 

	#Creating Cursor  
	cursor = connection.cursor() 
	sql = "DELETE FROM [dbo].[MWA2]"
	cursor.execute(sql)
	#Commiting any pending transaction to the database.  
	connection.commit()  
	#closing connection  
	connection.close()  
	print("Data Successfully Deleted") 

	for item in jsonData["RESULTS"]:
		#creating connection Object which will contain SQL Server Connection  
		connection = pyodbc.connect('Driver={SQL Server};''Server=Labtop;''Database=Testdb;')  
		print("Connection Successfully Established") 

		#Creating Cursor  
		cursor = connection.cursor() 

		WLMA_JOB_CODE = item['WLMA_JOB_CODE']
		A = item['DT_JOB_OPEN']
		DT_JOB_OPEN = datetime.strptime(A, "%Y%m%d %H%M%S")
		B = item['DT_FIELD_BEGIN']
		if B is not None:
			DT_FIELD_BEGIN = datetime.strptime(B, "%Y%m%d %H%M%S")
		else:
			DT_FIELD_BEGIN = B
		C = item['DT_FIELD_END']
		if C is not None:
			DT_FIELD_END = datetime.strptime(C, "%Y%m%d %H%M%S")
		else:
			DT_FIELD_END = C
		TEAM_ID = item['TEAM_ID']

		SQLCommand = ("INSERT INTO [dbo].[MWA2]([WLMA_JOB_CODE],[DT_JOB_OPEN],[DT_FIELD_BEGIN],[DT_FIELD_END],[TEAM_ID])VALUES(?,?,?,?,?)")
    
		Values = [WLMA_JOB_CODE,  DT_JOB_OPEN, DT_FIELD_BEGIN, DT_FIELD_END,TEAM_ID]

		#Processing Query
		#cursor.execute(DeleteCommand)
		cursor.execute(SQLCommand,Values)
		#Commiting any pending transaction to the database.  
		connection.commit()  
		#closing connection  
		connection.close()  
	print("Data Successfully Inserted")  
	i = i + 1