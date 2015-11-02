import sys, os
#sys.tracebacklimit = 0
import mysql.connector
from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceLogin
from simple_salesforce.util import date_to_iso8601, SalesforceError
import time

# setup your environment
try:
	from backconf import mysqlun,mysqlpwd,mysqlhn,mysqldb,sfusername,sfpassword,sfsecuritytoken
	config = {'user': mysqlun,'password': mysqlpwd,'host': mysqlhn,'database': mysqldb,'raise_on_warnings': True,}
	sf = Salesforce(username=sfusername, password=sfpassword, security_token=sfsecuritytoken)
	print("imported the file?")
except ImportError, e:
	print("error importing the file?")
	mysqlusername = raw_input("MySQL Username: ")
	mysqlpassword = raw_input("MySQL Password: ")
	mysqldbname = raw_input("MySQL DBName: ")
	mysqlhostname = raw_input("MySQL Hostname(localhost or remote url): ")
	salesforceusername = raw_input("Salesforce Username: ")
	salesforcepassword = raw_input("Salesforce Password: ")
	salesforcesecuritytoken = raw_input("Salesforce Token: ")
	storeconfigsettings = raw_input("Store these credentials(Yes/No): ")

	if storeconfigsettings == 'Yes':
		print("Creating a stored credential in future you will not be asked for your credentials")
		file_path = 'backconf.py'
		try:
		    fp = open(file_path)
		except IOError:
		    # If not exists, create the file
		    fp = open(file_path, 'w+')
		    fp.write("mysqlun='%s'\nmysqlpwd='%s'\nmysqldb='%s'\nmysqlhn='%s'\nsfusername='%s'\nsfpassword='%s'\nsfsecuritytoken='%s'" % (mysqlusername,mysqlpassword,mysqldbname,mysqlhostname,salesforceusername,salesforcepassword,salesforcesecuritytoken))
		    fp.close()
	else:
		print("Keep on rocking with temp connnection details")

	config = {'user': mysqlusername,'password': mysqlpassword,'host': mysqlhostname,'database': mysqldbname,'raise_on_warnings': True,}
	sf = Salesforce(username=salesforceusername, password=salesforcepassword, security_token=salesforcesecuritytoken)


# establish connection to local db
print ("Testing connection with local MySQL DB")
try:
  cnx = mysql.connector.connect(**config)
  cursor = cnx.cursor(buffered=True)
  print ("Connection successful")
except mysql.connector.Error as err:
  print("Something went wrong with your db settings: {}".format(err))

# silent test to verify connection to salesforce complete
print ("Testing connection with Salesforce Instance")
try:
	sf.describe()
	print ("Connection successful")
except SalesforceError as err:
	print("Something went wrong with your salesforce connection: {}".format(err))

# check if first run of script
print ("Checking to See if Default SF Object Table is available")
sfobjectcheck = "SHOW TABLES LIKE 'sfobject_sfobject'"
cursor.execute(sfobjectcheck)
result1 = cursor.fetchone()
if result1:
    print("The SF Object Table Exists")
    sfobjcheck = 1;
else:
	sobjectablecreate = ("CREATE TABLE sfobject_sfobject (data_id int(11) NOT NULL AUTO_INCREMENT Primary Key, obj_name TEXT,obj_keyPrefix TEXT,obj_label TEXT,obj_createable TEXT,obj_custom TEXT,obj_customSetting TEXT,created_at DATETIME DEFAULT NULL,modified_at DATETIME DEFAULT NULL)")
	cursor.execute(sobjectablecreate)
	print("New Table Created")
	sfobjcheck = 0;

print ("Checking to See if Default SF Object Field Table is available")
sfobjectfieldcheck = "SHOW TABLES LIKE 'sfobjectfield_sfobjectfield'"
cursor.execute(sfobjectfieldcheck)
result2 = cursor.fetchone()
if result2:
    print("The SF Object Field Table Exists")
    sfobjfieldcheck = 1;
else:
	sobjectablecreate = ("CREATE TABLE sfobjectfield_sfobjectfield (data_id int(11) NOT NULL AUTO_INCREMENT Primary Key, field_name TEXT,field_type TEXT,field_label TEXT,field_length TEXT,field_obj_name TEXT,created_at DATETIME DEFAULT NULL,modified_at DATETIME DEFAULT NULL)")
	cursor.execute(sobjectablecreate)
	print("New Table Created")
	sfobjfieldcheck = 0;

if sfobjcheck == 0:
	print ('Load the full list of your objects into your MySQL database')
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
	print ('You list of objects have now been added to your local Database, you have a total of', result )
else:
	print('Figure this script out later')

if sfobjfieldcheck == 0:
	print ('Get list of Object currently stored in mysql')
	query = ("SELECT obj_name FROM sfobject_sfobject where obj_keyPrefix is not NULL")
	cursor.execute(query)
	
	print ('Loop through each objectname and import field details into MySQL')
	for x in cursor:
		objectname = str(x)
		cleanobjectname1 = objectname.replace("(","")
		cleanobjectname2 = cleanobjectname1.replace("u'","")
		cleanobjectname3 = cleanobjectname2.replace("'","")
		cleanobjectname4 = cleanobjectname3.replace(",","")
		cleanobjectname = cleanobjectname4.replace(")","")
		print(cleanobjectname)

		for xy in getattr(sf, cleanobjectname).describe()["fields"]:
		  	objectname = str(x)
		  	cleanobjectname1 = objectname.replace("(","")
			cleanobjectname2 = cleanobjectname1.replace("u'","")
			cleanobjectname3 = cleanobjectname2.replace("'","")
			cleanobjectname4 = cleanobjectname3.replace(",","")
			cleanobjectname = cleanobjectname4.replace(")","")
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
		print ('All fields have now been added to mysql we added a total of', result)
else:
	print ("Figure this script out later")


query = ("SELECT obj_name FROM sfobject_sfobject where obj_keyPrefix is not NULL")
cursor.execute(query)
print ('Create Tables for Each SF Object Exported')

for newx in cursor:
	objectname = str(newx)
	cleanobjectname1 = objectname.replace("(","")
	cleanobjectname2 = cleanobjectname1.replace("'","")
	cleanobjectname3 = cleanobjectname2.replace(",","")
	cleanobjectname = cleanobjectname3.replace(")","")
	finalcleanobjectname = 'sf_'+cleanobjectname
	
	querytblcreate = ("CREATE TABLE %s (data_id int(11) NOT NULL AUTO_INCREMENT Primary Key)") % finalcleanobjectname
	inner_cur = cnx.cursor(buffered=True)
	inner_cur.execute(querytblcreate)

	query2 = ("SELECT field_name, field_type, field_length FROM sfobjectfield_sfobjectfield where field_obj_name = %s ") % cleanobjectname 
	inner_curq = cnx.cursor(buffered=True)
	inner_curq.execute(query2)

	for xyz in inner_curq:
		objectname = str(newx)
		fieldtype = xyz[1]
		fieldname = xyz[0]
		fieldlength = xyz[2]
		cleanobjectname1 = objectname.replace("(","")
		cleanobjectname2 = cleanobjectname1.replace("'","")
		cleanobjectname3 = cleanobjectname2.replace(",","")
		cleanobjectname = cleanobjectname3.replace(")","")
		finalcleanobjectname = 'sf_'+cleanobjectname


		fn = 'SF_'+fieldname

		if fieldtype == 'id':
			ft = 'VARCHAR(255)'
		if fieldtype == 'boolean':
			ft = 'BOOL'
		if fieldtype == 'datetime':
			ft = 'DATETIME'
		if fieldtype == 'reference':
			ft = 'VARCHAR(255)'
		if fieldtype == 'string':
			ft = 'VARCHAR(255)'
		if fieldtype == 'picklist':
			ft = 'VARCHAR(255)'
		if fieldtype == 'textarea':
			ft = 'BLOB'
		if fieldtype == 'email':
			ft = 'VARCHAR(255)'
		if fieldtype == 'url':
			ft = 'VARCHA(255)'
		if fieldtype == 'phone':
			ft = 'VARCHAR(255)'
		if fieldtype == 'encryptedstring':
			ft = 'VARCHAR(255)'
		if fieldtype == 'double':
			ft = 'DECIMAL'
		if fieldtype == 'date':
			ft = 'DATE'
		if fieldtype == 'multipicklist':
			ft = 'VARCHAR(255)'
		if fieldtype == 'percent':
			ft = 'DECIMAL'
		if fieldtype == 'currency':
			ft = 'VARCHAR(255)'
		
		queryfigureshitout = ("ALTER TABLE %s ADD %s %s") % (finalcleanobjectname,fn, ft)
		inner_cur1 = cnx.cursor(buffered=True)
		inner_cur1.execute(queryfigureshitout)

print ('Ok so that is cool - we have just replicated the Salesforce scheme to MySQL')

# for x in cursor:
# 	objectname = str(x)
# 	cleanobjectname1 = objectname.replace("(","")
# 	cleanobjectname2 = cleanobjectname1.replace("'","")
# 	cleanobjectname3 = cleanobjectname2.replace(",","")
# 	cleanobjectname = cleanobjectname3.replace(")","")
# 	print (objectname)
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