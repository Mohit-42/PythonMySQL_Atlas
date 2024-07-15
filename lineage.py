import mysql.connector
import pandas as pd
import requests
import json

def upload_csv_to_mysql(csv_file, db_config, table_name):
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Load CSV into DataFrame
        df = pd.read_csv(csv_file)

        # Define table schema (assuming table exists or is created separately)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            txn_id INT PRIMARY KEY,
            last_modified_date TIMESTAMP,
            last_modified_date_bs VARCHAR(50),
            created_date TIMESTAMP,
            amount DECIMAL(10,2),
            status VARCHAR(50),
            module_id INT,
            product_id INT,
            product_type_id INT,
            payer_account_id INT,
            receiver_account_id INT,
            reward_point INT,
            cash_back_amount DECIMAL(10,5),
            revenue_amount DECIMAL(10,5),
            transactor_module_id INT,
            time TIME
        );
        """
        cursor.execute(create_table_query)

        # Insert DataFrame data into MySQL table
        for i, row in df.iterrows():
            sql = f"""
            INSERT INTO {table_name} (
                txn_id, last_modified_date, last_modified_date_bs, created_date, amount, status, 
                module_id, product_id, product_type_id, payer_account_id, receiver_account_id, 
                reward_point, cash_back_amount, revenue_amount, transactor_module_id, time
            ) VALUES ({', '.join(['%s'] * len(row))})
            """
            cursor.execute(sql, tuple(row))

        # Commit and close connection
        conn.commit()
        cursor.close()
        conn.close()
        print(f"CSV data successfully uploaded to MySQL table '{table_name}'.")

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
    except pd.errors.EmptyDataError:
        print(f"Error: No data found in CSV file '{csv_file}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

def create_entity(entity_data):
    try:
        url = "http://localhost:21000/api/atlas/v2/entity"
        headers = {"Content-Type": "application/json"}
        response = requests.post(url, data=json.dumps(entity_data), headers=headers, auth=('admin', 'admin'))
        response_json = response.json()
        if response.status_code == 200:
            print(f"Entity created successfully: {response_json}")
        else:
            print(f"Failed to create entity: {response.status_code} - {response_json}")
        return response_json

    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
    except Exception as ex:
        print(f"An error occurred: {ex}")

def create_hdfs_path_entity(csv_path):
    try:
        # Check if HDFS path entity already exists
        url = "http://localhost:21000/api/atlas/v2/search/basic"
        params = {
            "typeName": "hdfs_path",
            "query": f"qualifiedName='{csv_path}'"
        }
        response = requests.get(url, params=params, auth=('admin', 'admin'))
        response_json = response.json()

        if response.status_code == 200 and response_json.get('count', 0) == 0:
            # Create HDFS path entity if it doesn't exist
            hdfs_entity = {
                "entity": {
                    "typeName": "hdfs_path",
                    "attributes": {
                        "qualifiedName": csv_path,
                        "name": csv_path.split('/')[-1],  # Assuming file name as entity name
                        "path": csv_path,
                        "clusterName": "default"  # Update with your HDFS cluster name
                    }
                }
            }
            create_entity(hdfs_entity)
        else:
            print(f"HDFS path entity '{csv_path}' already exists in Atlas or count issue.")

    except Exception as e:
        print(f"An error occurred creating HDFS path entity: {e}")

def create_process_entity(db_name, table_name, csv_path):
    try:
        # Create process entity for data lineage
        process_entity = {
            "entity": {
                "typeName": "hive_process",
                "attributes": {
                    "qualifiedName": f"load_csv_to_mysql@process",
                    "name": "load_csv_to_mysql",
                    "description": "Load CSV data into MySQL table",
                    "userName": "gret",  # Replace with actual user name
                    "startTime": 0,      # Adjust with actual start time
                    "endTime": 0,        # Adjust with actual end time
                    "operationType": "ETL",  # Example operation type, adjust as needed
                    "queryText": "LOAD DATA",  # Example query text, adjust as needed
                    "queryPlan": "SELECT * FROM table",  # Example query plan, adjust as needed
                    "queryId": "12345",   # Placeholder query ID, adjust as needed
                    "inputs": [
                        {
                            "typeName": "hdfs_path",
                            "uniqueAttributes": {
                                "qualifiedName": csv_path
                            }
                        }
                    ],
                    "outputs": [
                        {
                            "typeName": "Table",
                            "uniqueAttributes": {
                                "qualifiedName": f"{table_name}@{db_name}@mysql"
                            }
                        }
                    ]
                }
            }
        }
        create_entity(process_entity)

    except Exception as e:
        print(f"An error occurred creating process entity: {e}")

def create_mysql_entities(db_name, table_name, csv_path):
    try:
        # Create HDFS path entity
        create_hdfs_path_entity(csv_path)

        # Create database entity
        db_entity = {
            "entity": {
                "typeName": "hive_db",
                "attributes": {
                    "qualifiedName": f"{db_name}@mysql",
                    "name": db_name,
                    "clusterName": "mysql",
                    "description": "My MySQL database"
                }
            }
        }
        db_response = create_entity(db_entity)

        # Create table entity
        table_entity = {
            "entity": {
                "typeName": "Table",
                "attributes": {
                    "qualifiedName": f"{table_name}@{db_name}@mysql",
                    "name": table_name,
                    "db": {
                        "typeName": "hive_db",
                        "uniqueAttributes": {
                            "qualifiedName": f"{db_name}@mysql"
                        }
                    },
                    "description": "My MySQL table"
                }
            }
        }
        table_response = create_entity(table_entity)

        # Create process entity
        create_process_entity(db_name, table_name, csv_path)

        return db_response, table_response

    except Exception as e:
        print(f"An error occurred: {e}")

# MySQL database configuration
db_config = {
    'user': 'root',
    'password': 'G0t0h3!!',
    'host': '127.0.0.1',
    'database': 'rw_transactions'
}

# CSV file path
csv_file = '/home/gret/rw_transaction_data_11k.csv'  # Adjusted to absolute path
table_name = 'transaction'

# Step 1: Upload CSV to MySQL
upload_csv_to_mysql(csv_file, db_config, table_name)

# Step 2: Create MySQL entities in Apache Atlas
db_response, table_response = create_mysql_entities(db_config['database'], table_name, csv_file)

print(f"Database Entity: {db_response}")
print(f"Table Entity: {table_response}")
