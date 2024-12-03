
## `mirrulations_ingest`

The repo contains code to bulk ingest data from `s3://mirrulations` into the Aurora serverless instance.

## Setup

* Create a python virtual environment in the repo root

  ```
  python3 -m venv .venv
  ```
  
* Install the required libraries in the virtual environment

  ```
  .venv/bin/pip install -r requirements.txt
  ```
  
* Create a `.env` file

  ```
  MASTER_USERNAME=postgres
  MASTER_PASSWORD=<DB master password>
  DB_ENDPOINT=<URL of Aurora instance>
  ```
  
  NOTE: The master password is stored in SecretsManager 
  
* Configure your AWS credentials in `.aws`

   ```
   aws configure
   ```

* Add your IP address to the security group protecting the Aurora DB

  ```
  ./add_ip.sh
  ```
  
   
## Ingest


In `ingest_comments.py` find `main` and set the `prefix` to be the top-level folder you want to ingest.  The script will walk all files in this folder, so you can specify an agency folder or a docket folder, and it will find the comments in that folder.

Currently you cannot specify all files - `/` as a prefix fails.

To start the ingest, run

```
python ingest_comments.py
```
