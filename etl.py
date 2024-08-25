import os
import csv
import requests
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from datetime import datetime
import time

# Download and read the CSV file
url = 'https://s3-ap-southeast-2.amazonaws.com/jiangren-de-bucket/assignments/video_data.csv'

response = requests.get(url)
lines = response.text.splitlines()
reader = csv.reader(lines)

data = []
bad_file =[]

# Process the CSV data
for i, row in enumerate(reader):
    try:
        if i ==0:
            headers = row
            continue
        
        if len(row) != len(headers):
            raise ValueError (f'Row {i+1} has extra column')

        data.append(row)

    except Exception as e:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        bad_file.append(row + [str(e), timestamp])


# Write bad lines to a CSV file
with open('bad_lines.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(headers + ['Error', 'Timestamp'])
    writer.writerows(bad_file)

# MySQL connection and data upload 
# For simplicity, this MySQL is hosted on AWS and is public accessible

'''
sqlalchemy and mysql.connector are two common modules used to create MySQL connections in Python.
In this practice, we are using mysql.connector to create a connection to a public MySQL database (security group setting is required).
'''
load_dotenv()

start_time = datetime.now()
start_datetime = time.time() 
print(f"Process started at: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")


try:
    connection = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        port=os.getenv('MYSQL_PORT')
    )


    if connection.is_connected():
        cursor = connection.cursor()

        # Create a new database using raw SQL queryes
        create_database_query = "CREATE DATABASE IF NOT EXISTS VIDEO_DATA"
        cursor.execute(create_database_query)
        print("Database 'VIDEO_DATA' created or already exists")

        # Switch to the new database
        cursor.execute("USE VIDEO_DATA")

        # Create table using raw SQL queryes if it doesn't exist
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS video_raw (
            {', '.join([f'{header} VARCHAR(255)' for header in headers])}
        )
        """
        cursor.execute(create_table_query)

        # Insert data into the table
        # The %s is used as a placeholder in SQL queries when using mysql.connector to prevent SQL injection
        insert_query = f"""
        INSERT INTO video_raw ({', '.join(headers)})
        VALUES ({', '.join(['%s' for _ in headers])})
        """

        # Instead of trying to insert all records at once, we're now inserting data in batches. 
        # This helps to avoid the max_allowed_packet error by sending smaller chunks of data at a time.
        batch_size = 100
        total_inserted = 0

        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
            total_inserted += len(batch)
            
        end_datetime = time.time()
        end_datetime = datetime.now()
        total_time = end_datetime - start_datetime

        print(f"{total_inserted} records inserted successfully into the table")
        print(f"Process ended at: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total time taken: {total_time:.2f} seconds")

except Error as e:
    print(f"Error while connecting to MySQL: {e}")

finally:
    if connection is not None and connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
    else:
        print("Connection to MySQL could not be established")

print(f"Number of bad lines: {len(bad_file)}")
print(f"Bad lines have been written to 'bad_lines.csv'")