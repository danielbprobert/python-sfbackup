import sys, os
import mysql.connector
from simple_salesforce import Salesforce, SalesforceError
from simple_salesforce.login import SalesforceLogin
import time

# setup your environment
try:
	from backconf import mysqlun,mysqlpwd,mysqlhn,mysqldb,sfusername,sfpassword,sfsecuritytoken
	config = {'user': mysqlun,'password': mysqlpwd,'host': mysqlhn,'database': mysqldb,'raise_on_warnings': True,}
	sf = Salesforce(username=sfusername, password=sfpassword, security_token=sfsecuritytoken)
except ImportError as error:
	print("No configuration file present.")
	mysqlusername = input("MySQL Username: ")
	mysqlpassword = input("MySQL Password: ")
	mysqldbname = input("MySQL DBName: ")
	mysqlhostname = input("MySQL Hostname(localhost or remote url): ")
	salesforceusername = input("Salesforce Username: ")
	salesforcepassword = input("Salesforce Password: ")
	salesforcesecuritytoken = input("Salesforce Token: ")
	storeconfigsettings = input("Store these credentials(Yes/No): ")

	if storeconfigsettings == 'Yes':
		print("Creating a stored credential in future you will not be asked for your credentials - to be prompted again delete the file backconf.py")
		file_path = 'backconf.py'
		try:
		    fp = open(file_path)
		except IOError:
		    # If not exists, create the file
		    fp = open(file_path, 'w+')
		    fp.write("mysqlun='%s'\nmysqlpwd='%s'\nmysqldb='%s'\nmysqlhn='%s'\nsfusername='%s'\nsfpassword='%s'\nsfsecuritytoken='%s'" % (mysqlusername,mysqlpassword,mysqldbname,mysqlhostname,salesforceusername,salesforcepassword,salesforcesecuritytoken))
		    fp.close()
	else:
		print("Proceeding with temporary credentials")

	config = {'user': mysqlusername,'password': mysqlpassword,'host': mysqlhostname,'database': mysqldbname,'raise_on_warnings': True,}
	sf = Salesforce(username=salesforceusername, password=salesforcepassword, security_token=salesforcesecuritytoken)


# establish connection to local db
print ("Testing local MySQL DB connection")
try:
  cnx = mysql.connector.connect(**config)
  cursor = cnx.cursor(buffered=True)
  print ("Connection successful")
except mysql.connector.Error as err:
  print("MySQL db settings error: {}".format(err))

# silent test to verify connection to salesforce complete
print ("Testing Salesforce Instance connection")
try:
	sf.describe()
	print ("Connection successful")
except SalesforceError as err:
	print("Salesforce connection error: {}".format(err))

# check if first run of script
print ("Checking to See if Default SF Object Table is available")
sfobjectcheck = "SHOW TABLES LIKE 'sfobject_sfobject'"
cursor.execute(sfobjectcheck)
result1 = cursor.fetchone()
if result1:
    sfobjcheck = 1
else:
	sobjectablecreate = ("CREATE TABLE sfobject_sfobject (data_id int(11) NOT NULL AUTO_INCREMENT Primary Key, obj_name TEXT,obj_keyPrefix TEXT,obj_label TEXT,obj_createable TEXT,obj_custom TEXT,obj_customSetting TEXT,created_at DATETIME DEFAULT NULL,modified_at DATETIME DEFAULT NULL)")
	cursor.execute(sobjectablecreate)
	sfobjcheck = 0

print ("Checking to See if Default SF Object Field Table is available")
sfobjectfieldcheck = "SHOW TABLES LIKE 'sfobjectfield_sfobjectfield'"
cursor.execute(sfobjectfieldcheck)
result2 = cursor.fetchone()
if result2:
    sfobjfieldcheck = 1
else:
	sobjectablecreate = ("CREATE TABLE sfobjectfield_sfobjectfield (data_id int(11) NOT NULL AUTO_INCREMENT Primary Key, field_name TEXT,field_type TEXT,field_label TEXT,field_length TEXT,field_obj_name TEXT,created_at DATETIME DEFAULT NULL,modified_at DATETIME DEFAULT NULL)")
	cursor.execute(sobjectablecreate)
	sfobjfieldcheck = 0

if sfobjcheck == 0:
	print ('Query SF and insert list of SF Objects into DB')
	for x in sf.describe()["sobjects"]:
		usefultime = time.strftime('%Y-%m-%d %H:%M:%S')
		add_objects = ("INSERT INTO sfobject_sfobject "
	              "(obj_name,obj_label,obj_keyPrefix,obj_createable,obj_custom,obj_customSetting,created_at,modified_at) "
	              "VALUES (%(obj_name)s,%(obj_label)s,%(obj_keyPrefix)s,%(obj_createable)s,%(obj_custom)s,%(obj_customSetting)s,%(created_at)s,%(modified_at)s)")
		data_objects = {
		    'obj_name' : x["name"],
	    	'obj_keyPrefix' : x["keyPrefix"],
	    	'obj_label' : x["label"],
		    'obj_createable' : x["createable"],
		    'obj_custom' : x["custom"],
		    'obj_customSetting' : x["customSetting"],
		    'created_at' : usefultime,
		    'modified_at' : usefultime,
		}
		cursor.execute(add_objects,data_objects)
		cnx.commit()
	countobject = ("SELECT COUNT(*) from sfobject_sfobject")
	cursor.execute(countobject)
	result=cursor.fetchone()
	cleanresult = " ".join(str(x) for x in result)
	print ('SF Objects have now been imported into your local DB - %s objects imported' % (cleanresult))
else:
	for x in sf.describe()["sobjects"]:
		objectname = x["name"]
		countrecords = ("SELECT count(*) from sfobject_sfobject where obj_name = '%s'" % (objectname))
		cursor.execute(countrecords)
		result=cursor.fetchone()
		cleanresult = " ".join(str(x) for x in result)
		if cleanresult == '1':
			pass
		else:
			usefultime = time.strftime('%Y-%m-%d %H:%M:%S')
			add_objects = ("INSERT INTO sfobject_sfobject "
		              "(obj_name,obj_label,obj_keyPrefix,obj_createable,obj_custom,obj_customSetting,created_at,modified_at) "
		              "VALUES (%(obj_name)s,%(obj_label)s,%(obj_keyPrefix)s,%(obj_createable)s,%(obj_custom)s,%(obj_customSetting)s,%(created_at)s,%(modified_at)s)")
			data_objects = {
			    'obj_name' : x["name"],
		    	'obj_keyPrefix' : x["keyPrefix"],
		    	'obj_label' : x["label"],
			    'obj_createable' : x["createable"],
			    'obj_custom' : x["custom"],
			    'obj_customSetting' : x["customSetting"],
			    'created_at' : usefultime,
			    'modified_at' : usefultime,
			}
			cursor.execute(add_objects,data_objects)
			cnx.commit()

if sfobjfieldcheck == 0:
	print ('Query DB for list of Objects in SF')
	query = ("SELECT obj_name FROM sfobject_sfobject where obj_keyPrefix is not NULL")
	cursor.execute(query)
	for x in cursor:
		objectname = str(x)
		cleanobjectname1 = objectname.replace("(","")
		cleanobjectname2 = cleanobjectname1.replace("u'","")
		cleanobjectname3 = cleanobjectname2.replace("'","")
		cleanobjectname4 = cleanobjectname3.replace(",","")
		cleanobjectname = cleanobjectname4.replace(")","")

		for xy in getattr(sf, cleanobjectname).describe()["fields"]:
		  	usefultime = time.strftime('%Y-%m-%d %H:%M:%S')

		  	add_fields = ("INSERT INTO sfobjectfield_sfobjectfield "
		  		"(field_name,field_obj_name,field_label,field_length,field_type,created_at,modified_at) "
		  		"VALUES (%(field_name)s,%(field_obj_name)s,%(field_label)s,%(field_length)s,%(field_type)s,%(created_at)s,%(modified_at)s)")
		  	
		  	data_fields = {
		  		'field_name' : xy["name"].encode('cp850', errors='replace').decode('cp850'),
		  		'field_type' : xy["type"].encode('cp850', errors='replace').decode('cp850'),
				'field_label' : xy["label"].encode('cp850', errors='replace').decode('cp850'),
				'field_length' : xy["length"],
		  		'field_obj_name' : cleanobjectname,
		  		'created_at' : usefultime,
		  		'modified_at' : usefultime,
		  	}
		  	inner_cur = cnx.cursor()
		  	inner_cur.execute(add_fields,data_fields)
  			cnx.commit()
	inner_cur2 = cnx.cursor()
	countobjectfields = ("SELECT COUNT(*) from sfobjectfield_sfobjectfield where field_obj_name = '%s' ") % cleanobjectname
	inner_cur2.execute(countobjectfields)
	result=inner_cur2.fetchone()
	cleanresult = " ".join(str(x) for x in result)
	print ('All fields for %s object have now been added to your DB, there were a total of %s') % (cleanobjectname,cleanresult)
else:
	print ('Query each object in the DB and check for new fields')
	query = ("SELECT obj_name FROM sfobject_sfobject where obj_keyPrefix is not NULL")
	cursor.execute(query)
	
	for x in cursor:
		print ("Figure this script out later")
		objectname = str(x)
		print (objectname)
		cleanobjectname1 = objectname.replace("(","")
		cleanobjectname2 = cleanobjectname1.replace("u'","")
		cleanobjectname3 = cleanobjectname2.replace("'","")
		cleanobjectname4 = cleanobjectname3.replace(",","")
		cleanobjectname = cleanobjectname4.replace(")","")

		for xy in getattr(sf, cleanobjectname).describe()["fields"]:
			print(xy)


query = ("SELECT obj_name FROM sfobject_sfobject where obj_keyPrefix is not NULL")
cursor.execute(query)

if sfobjfieldcheck == 0:
	print ('Perform the Initial Table Creation as this is the first import')
	for newx in cursor:
		objectname = str(newx)
		cleanobjectname1 = objectname.replace("(","")
		cleanobjectname2 = cleanobjectname1.replace("u'","")
		cleanobjectname3 = cleanobjectname2.replace("'","")
		cleanobjectname4 = cleanobjectname3.replace(",","")
		cleanobjectname = cleanobjectname4.replace(")","")
		finalcleanobjectname = 'sf_'+cleanobjectname
		
		querytblcreate = ("CREATE TABLE %s (data_id int(11) NOT NULL AUTO_INCREMENT Primary Key) ENGINE=MyISAM") % finalcleanobjectname
		inner_cur = cnx.cursor(buffered=True)
		inner_cur.execute(querytblcreate)

		query2 = ("SELECT field_name, field_type, field_length FROM sfobjectfield_sfobjectfield where field_obj_name = '%s' ") % cleanobjectname 
		inner_curq = cnx.cursor(buffered=True)
		inner_curq.execute(query2)

		for xyz in inner_curq:
			objectname = str(newx)
			fieldtype = xyz[1]
			fieldname = xyz[0]
			fieldlength = xyz[2]
			cleanobjectname1 = objectname.replace("(","")
			cleanobjectname2 = cleanobjectname1.replace("u'","")
			cleanobjectname3 = cleanobjectname2.replace("'","")
			cleanobjectname4 = cleanobjectname3.replace(",","")
			cleanobjectname = cleanobjectname4.replace(")","")
			finalcleanobjectname = 'sf_'+cleanobjectname


			fn = 'SF_'+fieldname
			
			if fieldtype == 'id':
				ft = 'VARCHAR(%s)' % (fieldlength)
			if fieldtype == 'boolean':
				ft = 'BOOL'
			if fieldtype == 'datetime':
				ft = 'DATETIME'
			if fieldtype == 'reference':
				ft = 'TEXT'
			if fieldtype == 'string':
				ft = 'TEXT'
			if fieldtype == 'picklist':
				ft = 'TEXT'
			if fieldtype == 'textarea':
				ft = 'BLOB'
			if fieldtype == 'email':
				ft = 'TEXT'
			if fieldtype == 'url':
				ft = 'TEXT'
			if fieldtype == 'phone':
				ft = 'TEXT'
			if fieldtype == 'encryptedstring':
				ft = 'TEXT'
			if fieldtype == 'double':
				ft = 'DECIMAL'
			if fieldtype == 'date':
				ft = 'DATE'
			if fieldtype == 'multipicklist':
				ft = 'TEXT'
			if fieldtype == 'percent':
				ft = 'DECIMAL'
			if fieldtype == 'currency':
				ft = 'VARCHAR(%s)' % (fieldlength)	
			queryfigureshitout = ("ALTER TABLE %s ADD %s %s") % (finalcleanobjectname,fn, ft)
			inner_cur1 = cnx.cursor(buffered=True)
			inner_cur1.execute(queryfigureshitout)
else:
	print ("what do i do now - arggggghhhh")

print ('Ok so that is cool - we have just replicated the Salesforce scheme to MySQL')

for x in cursor:
 	objectname = str(x)
 	cleanobjectname1 = objectname.replace("(","")
 	cleanobjectname2 = cleanobjectname1.replace("'","")
 	cleanobjectname3 = cleanobjectname2.replace(",","")
 	cleanobjectname = cleanobjectname3.replace(")","")
 	print (objectname)
# 	for x in sf.query("SELECT Id FROM User"):

# 		add_fields = ("INSERT INTO User "
# 	  		"(id) "
# 	  		"VALUES (%(id)s)")
# 		data_fields = {
# 			'id' : x["id"].encode('cp850', errors='replace').decode('cp850'),
# 		}
		
# 		inner_cur = cnx.cursor()
# 		inner_cur.execute(add_fields,data_fields)
# 		cnx.commit()

cnx.close()