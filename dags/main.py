from airflow.sdk import dag
from datetime import date,datetime, timedelta
from api.youtube_statistics import get_playlist_id,get_video_ids,extract_video_data,save_json_data
import pendulum

from datawarehouse.etl_dwh import  load_core, load_staging

local_tz=pendulum.timezone("Asia/Kolkata")

defult_args ={
    'owner':'airflow',
    'depends_on_past':False,
    'email_on_failure':False,
    'max_active_tasks':1,    # Only 1 task runs at a time in this DAG
    'max_active_runs':1,     # Only 1 execution date runs at a time
    'retries':1,
    'retry_delay': timedelta(hours=1),
    'start_date':datetime(year=2025,month=12,day=17,tzinfo=local_tz)
}


@dag(dag_display_name="yt_data_etl",
     default_args=defult_args,
     description="Dag to produce json data from youtube API to local file.",
     schedule="@daily",
     catchup=False)
def yt_data_elt():
    save_json_data(extract_video_data(get_video_ids(get_playlist_id())))    
    
yt_data_elt()    

@dag(dag_display_name="yt_data_etl_database",
     default_args=defult_args,
     description="Dag to loads data int real tables.",
     schedule="@daily",
     catchup=False)
def yt_data_elt_db():
        load_data_staging = load_staging()
        load_data_core=load_core()
        load_data_staging >> load_data_core
yt_data_elt_db() 