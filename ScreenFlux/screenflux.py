import sqlite3
from os.path import expanduser
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

def main():
    try:
        data = query_database()
        logging.info("Data retrieved successfully")
        # Here you would typically process the data further or send it to InfluxDB
        # For demonstration, let's just print the first few rows
        for row in data[:5]:
            logging.info(f"Sample data: {row}")
    except Exception as e:
        logging.error(f"Failed to retrieve or process data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()