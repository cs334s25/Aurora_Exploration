import boto3
import json
import psycopg
from psycopg.errors import Error

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
        with conn.cursor() as cur:
            cur.executemany(query, values)
        conn.commit()
        print(f"Inserted {len(records)} records successfully.")
    except Error as e:
        print(f"Error inserting records: {e}")
        conn.rollback()

def ingest_comments(bucket_name, prefix, conn):
    session = boto3.Session()
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)

    batch_size = 100  # Batch size for inserts
    batch = []

    for obj in bucket.objects.filter(Prefix=prefix):
        key = obj.key
        
        if key.endswith('.json'):
            try:
                parts = key.split('/')
                if 'comments' in parts:
                    print(key)
                    json_obj = obj.get()["Body"].read().decode('utf-8')
                    batch.append(parse_json_to_record(json_obj))

                # Insert in batches
                if len(batch) >= batch_size:
                    batch_insert_records(batch, conn)
                    batch.clear()
            except Exception as e:
                print(f"Error processing file {key}: {e}")
    
    # Insert any remaining records
    if batch:
        batch_insert_records(batch, conn)
        

def main():
    s3 = boto3.resource('s3')

    bucket_name = 'mirrulations'
    prefix = 'WHD/WHD-2023-0001/'

    client = boto3.client('secretsmanager')
    secret_name = "rds!cluster-60fb6e4d-4475-4da5-8fe1-945933b30166"
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])

    username = secret['username']
    password = secret['password']

    conn_params = {
        "dbname": "postgres",
        "user": username,
        "password": password,
        "host": "mirrulations.cluster-ro-cb6gssewgl8x.us-east-1.rds.amazonaws.com",
        "port": "5432"
    }

    try:
        conn = psycopg.connect(**conn_params)
        drop_comments_table(conn)
        create_comments_table(conn)
        ingest_comments(bucket_name, prefix, conn)
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
