import sqlite3
from os.path import expanduser
import logging
import sys
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# InfluxDB connection details
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "_H4gpU0VBoFYvoI_yHaGQi05Rahvl63rTlsdOP1VNqSnh9gvBhTMTf-UfZUsnBpY046VVlFgnfOis5kmIHnIxQ=="
INFLUX_ORG = "Talissa"
INFLUX_BUCKET = "screentime_data"

def query_database():
    # Connect to the SQLite database
    knowledge_db = expanduser("~/Library/Application Support/Knowledge/knowledgeC.db")
    logging.info(f"Attempting to connect to database: {knowledge_db}")
    
    try:
        with sqlite3.connect(knowledge_db) as con:
            cur = con.cursor()
            
            # Execute the SQL query to fetch data
            logging.info("Executing SQL query")
            query = """
            SELECT
                ZOBJECT.ZVALUESTRING AS "app", 
                (ZOBJECT.ZENDDATE - ZOBJECT.ZSTARTDATE) AS "usage",
                (ZOBJECT.ZSTARTDATE + 978307200) as "start_time", 
                (ZOBJECT.ZENDDATE + 978307200) as "end_time",
                (ZOBJECT.ZCREATIONDATE + 978307200) as "created_at", 
                ZOBJECT.ZSECONDSFROMGMT AS "tz",
                ZSOURCE.ZDEVICEID AS "device_id",
                ZMODEL AS "device_model"
            FROM
                ZOBJECT 
                LEFT JOIN
                ZSTRUCTUREDMETADATA 
                ON ZOBJECT.ZSTRUCTUREDMETADATA = ZSTRUCTUREDMETADATA.Z_PK 
                LEFT JOIN
                ZSOURCE 
                ON ZOBJECT.ZSOURCE = ZSOURCE.Z_PK 
                LEFT JOIN
                ZSYNCPEER
                ON ZSOURCE.ZDEVICEID = ZSYNCPEER.ZDEVICEID
            WHERE
                ZSTREAMNAME = "/app/usage"
            ORDER BY
                ZSTARTDATE DESC
            """
            cur.execute(query)
            
            # Fetch all rows from the result set
            results = cur.fetchall()
            logging.info(f"Retrieved {len(results)} rows of data")
            return results
    except sqlite3.Error as e:
        logging.error(f"SQLite error occurred: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise

def write_to_influxdb(data):
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    for row in data:
        app, usage, start_time, end_time, created_at, tz, device_id, device_model = row
        
        # Convert Unix timestamp to datetime
        start_time = datetime.utcfromtimestamp(start_time)
        
        point = Point("screen_time") \
            .tag("app", app) \
            .tag("device_id", device_id) \
            .tag("device_model", device_model) \
            .field("usage", float(usage)) \
            .time(start_time)
        
        write_api.write(bucket=INFLUX_BUCKET, record=point)
    
    logging.info(f"Wrote {len(data)} points to InfluxDB")
    client.close()

def main():
    try:
        data = query_database()
        logging.info("Data retrieved successfully")
        
        if data:
            write_to_influxdb(data)
        else:
            logging.warning("No data retrieved from SQLite database")
        
        # For demonstration, let's just print the first few rows
        for row in data[:5]:
            logging.info(f"Sample data: {row}")
    except Exception as e:
        logging.error(f"Failed to retrieve or process data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()