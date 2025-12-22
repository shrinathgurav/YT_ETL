from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import date,datetime, timedelta
from dataquality.check_yt_data_quality import yt_etl_data_quality
from datawarehouse.etl_dwh import  load_core, load_staging
from api.youtube_statistics import get_playlist_id,get_video_ids,extract_video_data,save_json_data
import pendulum



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

#define schema names
staging_schema="staging"
core_schema="core"


@dag(dag_display_name="produce_data_json",
     default_args=defult_args,
     description="Dag to produce json data from youtube API to local file.",
     schedule="@daily",
     catchup=False)
def produce_data_json():
    # save_json_data(extract_video_data(get_video_ids(get_playlist_id())))   
    get_playlistids = get_playlist_id()
    get_videosids = get_video_ids(get_playlistids)
    extracted_video_data=extract_video_data(get_videosids)
    save_json_to_file=save_json_data(extracted_video_data)
    trigger_update_data_db_dag = TriggerDagRunOperator(task_id="trigger_update_data_db_dag",trigger_dag_id="update_data_db")
    
    get_playlistids >> get_videosids >> extracted_video_data >> save_json_to_file >> trigger_update_data_db_dag
 
    # # save_json_data >> get_playlistids >> get_videosids >> extracted_video_data >> save_json_to_file >> trigger_update_data_db_dag
    # save_json_data >> trigger_update_data_db_dag
   
produce_data_json()    

@dag(dag_display_name="update_data_db",
     default_args=defult_args,
     description="Dag to loads data int real tables.",
    #  schedule="@daily",
     catchup=False)
def update_data_db():
        load_data_staging = load_staging()
        load_data_core=load_core()
        trigger_check_data_quality = TriggerDagRunOperator(task_id="triger_check_data_quality",trigger_dag_id="check_data_quality")
        
        load_data_staging >> load_data_core >> trigger_check_data_quality
        
update_data_db() 

@dag(dag_display_name="check_data_quality",
     default_args=defult_args,
     description="Dag to loads data int real tables.",
    #  schedule="@daily",
     catchup=False)
def check_data_quality():
    task_stage_data_quality_check = yt_etl_data_quality(staging_schema)
    task_core_data_quality_check = yt_etl_data_quality(core_schema)
    
    # set task dependencies
    task_stage_data_quality_check >> task_core_data_quality_check
check_data_quality() 