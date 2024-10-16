#SFBackup
This is a crude script for core backup of an entire salesforce database

This current version has no source control and so each run will override your current data - i.e. if you backup today, then run it again tomorrow it not going to let you access the data from yesterday it will only show todays backup data. if you want to backup like that you could backup your local db.

#Setup Instructions

##Install the required packages:
pip install simple-salesforce mysql-connector-python

##Fill in Credentials:
Replace placeholders with your actual Salesforce and MySQL credentials.

##Run the Script: Run the script using:

python sfbackupMySQL.py