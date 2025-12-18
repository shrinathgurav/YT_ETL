from airflow import DAG
from airflow.decorators import dag
from datetime import date,datetime, timedelta
from api.youtube_statistics import get_playlist_id,get_video_ids,extract_video_data,save_json_data
import pendulum

local_tz=pendulum.timezone("Asia/Kolkata")

defult_args ={
    'owner':'airflow',
    'depends_on_past':False,
    'email_on_failure':False,
    'retries':1,
    'retry_delay': timedelta(hours=1),
    'start_date':datetime(year=2025,month=12,day=17,tzinfo=local_tz)
}


@dag(dag_display_name="YT_DATA_ELT",
     default_args=defult_args,
     description="Dag to produce json data from youtube API to local file.",
     schedule="@daily",
     catchup=False)
def yt_data_elt():
    save_json_data(extract_video_data(get_video_ids(get_playlist_id())))    
    
yt_data_elt()    

# with DAG(dag_display_name="YT_DATA_ELT",
#          dag_id="yt_data_elt",
#          default_args=defult_args,
#          description="Dag to produce json data from youtube API to local file.",
#          schedule="@daily",
#          catchup=False) as dag :
    
#     save_json_data(extract_video_data(get_video_ids(get_playlist_id()))) 