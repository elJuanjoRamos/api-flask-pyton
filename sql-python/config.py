import os

settings = {
    'host': os.environ.get('ACCOUNT_HOST', 'https://sopes-cosmo.documents.azure.com:443/'),
    'master_key': os.environ.get('ACCOUNT_KEY', 's959f2GU859IY9SaYbDATGcRWOZ1jmUcevSXd9n3XYQuXZMkG7SVMME3R3QPy37Uq6Y8ksG5xFo1MUhIg1HZWQ=='),
    'database_id': os.environ.get('COSMOS_DATABASE', 'Tweets'),
    'container_id': os.environ.get('COSMOS_CONTAINER', 'Tweet'),
    'CLOUD_SQL_USERNAME': os.environ.get('CLOUD_SQL_USERNAME', 'adm'),
    'CLOUD_SQL_PASSWORD':os.environ.get('CLOUD_SQL_PASSWORD', '123'),
    'CLOUD_SQL_DATABASE_NAME': os.environ.get('CLOUD_SQL_DATABASE_NAME', 'Tweets'),
    'CLOUD_SQL_CONNECTION_NAME': os.environ.get('CLOUD_SQL_CONNECTION_NAME', 'sopes-326122:us-central1:sopesdb'),
}