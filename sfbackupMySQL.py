import mysql.connector
from simple_salesforce import Salesforce
import json
import collections
import traceback
from datetime import datetime

# Salesforce connection details
sf_username = 'your_salesforce_username'
sf_password = 'your_salesforce_password'
sf_security_token = 'your_salesforce_security_token'
sf_domain = 'your_salesforce_domain'  # e.g., 'login', 'test'

# MySQL connection details
mysql_host = 'your_mysql_host'
mysql_user = 'your_mysql_user'
mysql_password = 'your_mysql_password'
mysql_database = 'your_mysql_database'

# Get the current time to generate a unique log file
start_datetime = datetime.now()
start_datetime_str = start_datetime.strftime('%Y%m%d-%H%M%S')
error_log_file = f"error-{start_datetime_str}.log"

def connect_to_salesforce():
    try:
        sf = Salesforce(
            username=sf_username,
            password=sf_password,
            security_token=sf_security_token,
            domain=sf_domain
        )
        return sf
    except Exception as e:
        log_error("Salesforce Connection", str(e))
        raise

def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password
        )
        
        # Check if the database exists, and create it if it doesn't
        cursor = conn.cursor()
        cursor.execute(f"SHOW DATABASES LIKE '{mysql_database}'")
        result = cursor.fetchone()

        if not result:
            print(f"Database '{mysql_database}' does not exist. Creating it.")
            cursor.execute(f"CREATE DATABASE `{mysql_database}`")
        else:
            print(f"Database '{mysql_database}' already exists.")

        conn.close()
        
        conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )
        
        # Create error_objects and backuprunlog tables if they don't exist
        create_error_table(conn)
        create_backuprunlog_table(conn)
        
        return conn
    except Exception as e:
        log_error("MySQL Connection", str(e))
        raise

def create_error_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS error_objects (
        object_name VARCHAR(255) PRIMARY KEY,
        error_message TEXT,
        date_logged DATETIME
    )
    """)
    conn.commit()

def create_backuprunlog_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS backuprunlog (
        run_id INT AUTO_INCREMENT PRIMARY KEY,
        start_datetime DATETIME,
        end_datetime DATETIME,
        objects_backed_up INT,
        error_objects INT
    )
    """)
    conn.commit()

def insert_backuprunlog_start(conn):
    """Insert a new row into backuprunlog with the start time, and return the inserted run ID."""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO backuprunlog (start_datetime, objects_backed_up, error_objects) VALUES (%s, 0, 0)",
        (start_datetime,)
    )
    conn.commit()
    return cursor.lastrowid

def update_backuprunlog_end(conn, run_id, objects_backed_up, error_objects):
    """Update the end time and counts for the backup run log."""
    end_datetime = datetime.now()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE backuprunlog SET end_datetime = %s, objects_backed_up = %s, error_objects = %s WHERE run_id = %s",
        (end_datetime, objects_backed_up, error_objects, run_id)
    )
    conn.commit()

def get_skipped_objects(conn):
    """Retrieve a list of objects that should be skipped based on previous errors."""
    cursor = conn.cursor()
    cursor.execute("SELECT object_name FROM error_objects")
    result = cursor.fetchall()
    return {row[0] for row in result}

def get_salesforce_metadata(sf, skipped_objects):
    try:
        object_descriptions = sf.describe()['sobjects']
        objects_metadata = {}
        
        for obj in object_descriptions:
            obj_name = obj['name']
            
            if obj_name in skipped_objects:
                print(f"Skipping metadata retrieval for {obj_name} due to previous errors.")
                continue

            try:
                print(f"Retrieving metadata for {obj_name}")
                metadata = sf.__getattr__(obj_name).describe()
                objects_metadata[obj_name] = metadata
            except Exception as e:
                if "does not support query" in str(e):
                    log_query_error(conn, obj_name, str(e))
                else:
                    log_error(f"Metadata Retrieval for {obj_name}", str(e))
        
        return objects_metadata
    except Exception as e:
        log_error("Salesforce Metadata Retrieval", str(e))
        raise

def create_mysql_tables(conn, objects_metadata):
    cursor = conn.cursor()

    for object_name, metadata in objects_metadata.items():
        try:
            print(f"Checking if table '{object_name}' exists")

            # Check if table exists
            cursor.execute(f"SHOW TABLES LIKE '{object_name}'")
            result = cursor.fetchone()

            if result:
                print(f"Table '{object_name}' already exists. Skipping creation.")
                continue

            print(f"Creating table for {object_name}")

            columns = []
            for field in metadata['fields']:
                field_name = field['name']
                field_type = field['type']
                mysql_type = map_salesforce_type_to_mysql(field_type)
                columns.append(f"`{field_name}` {mysql_type}")

            create_table_sql = f"""
            CREATE TABLE `{object_name}` (
                {', '.join(columns)}
            );
            """
            cursor.execute(create_table_sql)
            conn.commit()
        except Exception as e:
            log_error(f"Create Table {object_name}", str(e), create_table_sql)
            continue

def map_salesforce_type_to_mysql(sf_type):
    mapping = {
        'string': 'VARCHAR(255)',
        'picklist': 'VARCHAR(255)',
        'id': 'VARCHAR(18)',
        'boolean': 'BOOLEAN',
        'int': 'INT',
        'double': 'DOUBLE',
        'currency': 'DECIMAL(18,2)',
        'date': 'DATE',
        'datetime': 'DATETIME',
        'textarea': 'TEXT',
        'phone': 'VARCHAR(50)',
        'url': 'VARCHAR(255)',
        'email': 'VARCHAR(255)',
        'reference': 'VARCHAR(18)'
    }
    return mapping.get(sf_type, 'TEXT')

def convert_datetime(value):
    try:
        if value and isinstance(value, str):
            dt = datetime.strptime(value[:19], '%Y-%m-%dT%H:%M:%S')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        log_error("Datetime Conversion", str(e), value)
    return None

def export_data_to_mysql(sf, conn, objects_metadata, run_id):
    cursor = conn.cursor()

    objects_backed_up = 0
    error_objects = 0

    for object_name, metadata in objects_metadata.items():
        try:
            print(f"Exporting data for {object_name}")

            fields = [field['name'] for field in metadata['fields']]
            fields_str = ', '.join(fields)

            query = f"SELECT {fields_str} FROM {object_name}"
            try:
                result = sf.query_all(query)
            except Exception as e:
                if "does not support query" in str(e) or "MALFORMED_QUERY" in str(e):
                    log_query_error(conn, object_name, str(e))
                    error_objects += 1
                    continue
                else:
                    log_error(f"Query {object_name}", str(e), query)
                    error_objects += 1
                    continue
            
            if 'records' in result and result['records']:
                records = result['records']

                placeholders = ', '.join(['%s'] * len(fields))
                columns = ', '.join([f"`{field}`" for field in fields])
                insert_sql = f"INSERT INTO `{object_name}` ({columns}) VALUES ({placeholders})"

                for record in records:
                    values = []
                    for field in fields:
                        value = record.get(field, None)

                        if value and metadata['fields']:
                            field_type = next(
                                (f['type'] for f in metadata['fields'] if f['name'] == field), None
                            )
                            if field_type == 'datetime':
                                value = convert_datetime(value)

                        if isinstance(value, collections.OrderedDict) or isinstance(value, dict):
                            value = json.dumps(value)

                        if isinstance(value, list):
                            value = json.dumps(value)

                        values.append(value)
                    
                    cursor.execute(insert_sql, values)

            objects_backed_up += 1
            conn.commit()

        except Exception as e:
            log_error(f"Export Data for {object_name}", str(e), query)
            error_objects += 1
            continue
    
    # Update the backuprunlog at the end
    update_backuprunlog_end(conn, run_id, objects_backed_up, error_objects)

def log_query_error(conn, object_name, error_message):
    """Logs objects with errors 'does not support query' or 'MALFORMED_QUERY' to the error_objects table."""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT IGNORE INTO error_objects (object_name, error_message, date_logged) VALUES (%s, %s, %s)",
        (object_name, error_message, datetime.now())
    )
    conn.commit()
    print(f"Logged non-queryable or malformed query object: {object_name}")

def log_error(step, error_message, query=None):
    with open(error_log_file, "a") as error_file:
        error_file.write(f"Step: {step}\n")
        error_file.write(f"Error: {error_message}\n")
        if query:
            error_file.write(f"Query: {query}\n")
        error_file.write(f"Traceback: {traceback.format_exc()}\n")
        error_file.write("---------------------------------------------------\n")

def main():
    sf = connect_to_salesforce()
    conn = connect_to_mysql()

    # Insert a new row into the backuprunlog with the start time
    run_id = insert_backuprunlog_start(conn)

    skipped_objects = get_skipped_objects(conn)
    objects_metadata = get_salesforce_metadata(sf, skipped_objects)
    create_mysql_tables(conn, objects_metadata)
    export_data_to_mysql(sf, conn, objects_metadata, run_id)
    
    conn.close()

if __name__ == '__main__':
    main()
