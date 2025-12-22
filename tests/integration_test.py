import psycopg2
import requests
import pytest

from conftest import airflow_variable

def test_youtube_response(airflow_variable):
    api_key =airflow_variable("API_KEY")
    channel_handle = airflow_variable("CHANNEL_HANDLE")
    
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"
    
    try:
        response=requests.get(url)
        assert response.status_code==200        
    except requests.RequestException as reqExp:
        print(f"Request to YouTube API failed: {reqExp}")
        
def test_check_real_postgres_conn(real_postgres_connection) :
    cursor =None
    try:
        cursor = real_postgres_connection.cursor()
        # cursor = conn.cursor()
        cursor.execute("select 1 ;")
        result =cursor.fetchone()
        assert result[0] == 1
    except psycopg2.Error as Err:
        pytest.fail("Database Connection failed {Err}")
    finally:
        if cursor :
            cursor.close()
            