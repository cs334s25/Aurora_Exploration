import concurrent.futures
import os
import threading
import boto3
import json
import time
import psycopg
from psycopg.errors import Error
import dotenv


# Lock for database connection to ensure thread safety
db_lock = threading.Lock()


def create_comments_table(conn):
    try:
        with conn.cursor() as cur:
            create_table_query = """
            CREATE TABLE comments (
                id TEXT PRIMARY KEY,
                apiurl TEXT,
                commentOn TEXT,
                commentOnDocumentId TEXT,
                duplicateComments INTEGER,
                address1 TEXT,
                address2 TEXT,
                agencyId TEXT,
                city TEXT,
                category TEXT,
                comment TEXT,
                country TEXT,
                docAbstract TEXT,
                docketId TEXT,
                documentType TEXT,
                email TEXT,
                fax TEXT,
                field1 TEXT,
                field2 TEXT,
                fileFormats TEXT,
                firstName TEXT,
                govAgency TEXT,
                govAgencyType TEXT,
                objectId TEXT,
                lastName TEXT,
                legacyId TEXT,
                modifyDate TIMESTAMP,
                organization TEXT,
                originalDocumentId TEXT,
                pageCount INTEGER,
                phone TEXT,
                postedDate TIMESTAMP,
                postmarkDate TIMESTAMP,
                reasonWithdrawn TEXT,
                receiveDate TIMESTAMP,
                restrictReason TEXT,
                restrictReasonType TEXT,
                stateProvinceRegion TEXT,
                submitterRep TEXT,
                submitterRepAddress TEXT,
                submitterRepCityState TEXT,
                subtype TEXT,
                title TEXT,
                trackingNbr TEXT,
                withdrawn BOOLEAN,
                zip TEXT,
                openForComment BOOLEAN
            );
            """
            cur.execute(create_table_query)
            conn.commit()
            print("Table 'comments' created successfully.")
    except Error as e:
        print(f"An error occurred: {e}")


def drop_comments_table(conn):
    try:
        with conn.cursor() as cur:
            drop_table_query = "DROP TABLE IF EXISTS comments;"
            cur.execute(drop_table_query)
            conn.commit()
            print("Table 'comments' dropped successfully (if it existed).")
    except Error as e:
        print(f"An error occurred: {e}")


def parse_json_to_record(json_text):
    data = json.loads(json_text)
    record = {
        "id": data["data"]["id"],
        "apiurl": data["data"]["links"]["self"],
    }
    attributes = data["data"]["attributes"]
    for key in [
        "commentOn", "commentOnDocumentId", "duplicateComments", "address1", "address2",
        "agencyId", "city", "category", "comment", "country", "docAbstract", "docketId",
        "documentType", "email", "fax", "field1", "field2", "fileFormats", "firstName",
        "govAgency", "govAgencyType", "objectId", "lastName", "legacyId", "modifyDate",
        "organization", "originalDocumentId", "pageCount", "phone", "postedDate", "postmarkDate",
        "reasonWithdrawn", "receiveDate", "restrictReason", "restrictReasonType",
        "stateProvinceRegion", "submitterRep", "submitterRepAddress", "submitterRepCityState",
        "subtype", "title", "trackingNbr", "withdrawn", "zip", "openForComment"
    ]:
        record[key] = attributes.get(key)
    return record


def batch_insert_records(records, conn):
    if not records:
        return
    columns = records[0].keys()
    query = f"""
    INSERT INTO comments ({", ".join(columns)})
    VALUES ({", ".join(["%s"] * len(columns))})
    ON CONFLICT (id) DO NOTHING;
    """
    values = [tuple(record.values()) for record in records]
    try:
        with db_lock:  # Locking for thread safety
            with conn.cursor() as cur:
                cur.executemany(query, values)
            conn.commit()
            print(f"Inserted {len(records)} records successfully.")
    except Error as e:
        print(f"Error inserting records: {e}")
        conn.rollback()


def process_files(keys_batch, conn_params, bucket_name):
    try:
        s3 = boto3.resource('s3', region_name='us-east-1')
        bucket = s3.Bucket(bucket_name)
        conn = psycopg.connect(**conn_params)
        records = []

        for key in keys_batch:
            try:
                obj = bucket.Object(key)
                json_obj = obj.get()["Body"].read().decode('utf-8')
                record = parse_json_to_record(json_obj)
                records.append(record)
            except Exception as e:
                print(f"Error processing file {key}: {e}")

        # Batch insert records into the database
        batch_insert_records(records, conn)
    finally:
        if conn:
            conn.close()


def ingest_comments(bucket_name, prefix, conn_params, max_workers):
    s3 = boto3.resource('s3', region_name='us-east-1')
    bucket = s3.Bucket(bucket_name)

    # List all file keys
    file_keys = [obj.key for obj in bucket.objects.filter(Prefix=prefix) if obj.key.endswith('.json')]

    # Split file keys into batches of 1,000
    batches = [file_keys[i:i + 1000] for i in range(0, len(file_keys), 1000)]

    # Process batches in threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_files, batch, conn_params, bucket_name) for batch in batches]
        concurrent.futures.wait(futures)


def main():
    bucket_name = 'mirrulations'
    #prefix = 'WHD/WHD-2023-0001/'
    prefix = 'WHD/WHD-2019-0003/'

    dotenv.load_dotenv()

    username = 'postgres'
    password = os.getenv('MASTER_PASSWORD')

    conn_params = {
        "dbname": "postgres",
        "user": username,
        "password": password,
        "host": "mirrulationsdb.cluster-ro-cb6gssewgl8x.us-east-1.rds.amazonaws.com",
        "port": "5432"
    }

    try:
        conn = psycopg.connect(**conn_params)
        drop_comments_table(conn)
        create_comments_table(conn)
    finally:
        if conn:
            conn.close()

    max_workers = 15
    before = time.time()
    ingest_comments(bucket_name, prefix, conn_params, max_workers)
    after = time.time()
    print(max_workers, after - before)


if __name__ == '__main__':
    main()
