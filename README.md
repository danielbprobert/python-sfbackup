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
2. update the connection string in sfbackup.py
2.1  - line 10 - insert your mysql db details
2.2  - line 24 - insert you salesforce credentials
3. run sfbackup.py

# Updates

Within the current version will only create 2 local tables and import a list of your objects and their associated field
