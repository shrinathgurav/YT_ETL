import os
import pytest
import psycopg2
from unittest import mock
from airflow.sdk import Variable
from airflow.models.connection import Connection
from airflow.models.dagbag import DagBag

@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ",AIRFLOW_VAR_API_KEY="myapikey_1234") :
        yield Variable.get("API_KEY")
        
@pytest.fixture
def channel_handle():
    with mock.patch.dict("os.environ",AIRFLOW_VAR_CHANNEL_HANDLE="mymock_channel") :
        yield Variable.get("CHANNEL_HANDLE")        
        
@pytest.fixture
def mock_postgres_conn_var():
    conn = Connection(
        login="mock_username",
        password="mock_passwd",
        host="mock_myhost",
        port=6789,
        schema="mock_my_schema"
    )
    
    conn_uri = conn.get_uri()
    
    with mock.patch.dict("os.environ",AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri) :
        yield Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ELT")
        # Variable.get(conn_id="POSTGRES_DB_YT_ELT")
        
@pytest.fixture
def dagbag():
    yield DagBag()   
    
    
@pytest.fixture
def airflow_variable():
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)
    return get_airflow_variable

@pytest.fixture
def real_postgres_connection():
    conn =None
    dbname=os.getenv("ELT_DATABASE_NAME")
    user=os.getenv("ELT_DATABASE_USERNAME")
    password=os.getenv("ELT_DATABASE_PASSWORD")
    host=os.getenv("POSTGRES_CONN_HOST")
    port=os.getenv("POSTGRES_CONN_PORT")    
    try:
      conn =  psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
      
      yield conn
    except psycopg2.DatabaseError as DBErr :
        pytest.fail(f"Failed to connect to the database {DBErr}")
    finally  :
        if conn :
            conn.close()