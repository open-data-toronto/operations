import ckanapi
from airflow.models import Variable

# init hardcoded vars
ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN_ADDRESS = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]

def connect_to_ckan():
    '''Returns RemoteCKAN object for appropriate CKAN instance'''

    return ckanapi.RemoteCKAN(apikey=CKAN_APIKEY, address=CKAN_ADDRESS)