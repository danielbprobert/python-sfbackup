# python-sfbackup
Simple Python Script for backing up your Salesforce instance

This is a concept born from the fact that I hate having to pay for a daily salesforce backup, at the moment the full blown version will only pull out your db schema and even that I need to nail down error but ultimately my goal for this script is this:

1. Connect to a local mysql db
2. Connect to your remote SF Instance
3. Export a list of objects from Salesforce into MySQL
4. Export a list of fields stored in each object within Salesforce
5. Export all data from Salesforce for each object creating a dedicated table for each in your mysqldb
6. After initial import every time it runs check to see if data exists and only perform update functions.

# Setup Instruction

1. pip install requirements.txt
2. run sfbackup.py
3. answer questions - sit back and wait for the message: Ok so that is cool - we have just replicated the Salesforce scheme to MySQL

# Updates

03/11/2015 - Major improvements to the interfact with the inclusion of storing settings so the next time you run the script it's already got your connection details, the script now will 100% export all objects/export all fields related to objects/create a dedicated table for each object and recreate your field structure(flat) ready for backing up of data - additionally the first steps toward checking if data already exists is in place so that if a new object is added in Salesforce it's imported automatically into your MySQL db - at the moment only the object details are pulled in but the next version will have all columns being pulled in as well.

02/11/2015 - Within the current version will only create 2 local tables and import a list of your objects and their associated field

