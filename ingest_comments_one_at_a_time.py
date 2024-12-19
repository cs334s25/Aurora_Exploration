import boto3
import json
import psycopg


import psycopg

def create_comments_table(conn):
    """
    Creates the 'comments' table in the PostgreSQL database.
    
    Parameters:
        conn: psycopg.Connection
            A connection object for the PostgreSQL database.
    """
    try:
        with conn.cursor() as cur:
            # Define the CREATE TABLE query
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
            # Execute the CREATE TABLE query
            cur.execute(create_table_query)
            conn.commit()
            print("Table 'comments' created successfully.")
    except psycopg.Error as e:
        print(f"An error occurred: {e}")


def drop_comments_table(conn):
    """
    Drops the 'comments' table in the PostgreSQL database if it exists.
    
    Parameters:
        conn: psycopg.Connection
            A connection object for the PostgreSQL database.
    """
    try:
        with conn.cursor() as cur:
            drop_table_query = "DROP TABLE IF EXISTS comments;"
            
            cur.execute(drop_table_query)
            conn.commit()
            print("Table 'comments' dropped successfully (if it existed).")
    except psycopg.Error as e:
        print(f"An error occurred: {e}")


def generate_insert_sql(json_text):
    # Parse the JSON string
    data = json.loads(json_text)
    
    # Extract relevant fields
    record = {
        "id": data["data"]["id"],
        "apiurl": data["data"]["links"]["self"],
    }
    
    # Extract attributes, ignoring displayProperties
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

    # Build SQL INSERT statement
    columns = ", ".join(record.keys())
    values = ", ".join(
        [
            f"'{value}'" if value is not None else "NULL"
            for value in record.values()
        ]
    )
    
    sql = f"INSERT INTO comments ({columns}) VALUES ({values});"
    return sql


def execute_query(query, conn):
    """Executes a given SQL query using the provided database connection."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()
        print("Query executed successfully")
    except Exception as e:
        print(f"Error executing query: {e}")
        conn.rollback()


def ingest_comments(bucket_name, prefix, conn):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    
    for obj in bucket.objects.filter(Prefix=prefix):
        key = obj.key
        if key.endswith('.json'):
            print(key)
            parts = key.split('/')
            if 'comments' in parts:
                json_obj = obj.get()["Body"].read().decode('utf-8')
                execute_query(generate_insert_sql(json_obj), conn)


def main():
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
