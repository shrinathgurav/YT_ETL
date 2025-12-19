from datawarehouse.db_utils import get_pg_conn_cur,close_pg_curr_conn,get_vodeo_ids
from datawarehouse.data_load import get_data
from datawarehouse.data_modification import insert_record,update_record,delete_records
from datawarehouse.data_transformations import transform_row
from airflow.sdk import task
import logging

logger = logging.getLogger(__name__)

stag_table_name="yt_data_stage"
fact_table_name = "yt_data"

@task
def load_staging() :
    from datawarehouse.db_utils import create_schema,create_table
    schema ="staging"
    conn,cur =None,None
    
    try:
        
        raw_data = get_data()
        conn,cur = get_pg_conn_cur()
        
        create_schema(conn=conn,cur=cur,schema=schema)
        create_table(conn=conn,cur=cur,schema=schema, table_name=stag_table_name)
        
        tbl_video_ids = get_vodeo_ids(cur=cur,schema=schema,table_name=stag_table_name)
        logger.info(f"length of ids are {tbl_video_ids}")
        
        for row in raw_data :
            if len(tbl_video_ids) == 0 :
                insert_record(conn=conn,cur=cur,schema=schema,table_name=stag_table_name,row=row)
            else:
                if row["video_id"] in tbl_video_ids:
                    update_record(conn=conn,cur=cur,schema=schema,table_name=stag_table_name,row=row)
                else:
                    insert_record(conn=conn,cur=cur,schema=schema,table_name=stag_table_name,row=row)
        
        ids_in_json = {row["video_id"] for row in raw_data}
        ids_to_delete = set(tbl_video_ids) - ids_in_json
        
        if ids_to_delete:
            delete_records(conn=conn,cur=cur,schema=schema,table_name=stag_table_name,ids_to_delete=ids_to_delete)
        logger.info(f"{schema}.{stag_table_name} data update completed.")
        
    except Exception as err :
        logger.error(f"Error Occured while updating {schema}.{stag_table_name} due to {err}")
        raise err
    finally:
        if conn and cur:
            close_pg_curr_conn(cur=cur,conn=conn)
            
@task
def load_core():
    from datawarehouse.db_utils import create_schema,create_table
    schema="core"     
    conn,cur = None,None
    
    try:
        conn,cur = get_pg_conn_cur()
        
        create_schema(conn=conn,cur=cur,schema=schema)
        create_table(conn=conn,cur=cur,schema=schema, table_name=fact_table_name)
        
        table_ids = get_vodeo_ids(cur=cur,schema=schema,table_name=fact_table_name)
        current_video_ids = set()
        
        cur.execute(f"select * from staging.{stag_table_name} ;")
        rows = cur.fetchall()
        
        for row in rows :
            current_video_ids.add(row["Video_Id"])
            
            if len(table_ids) <=0 :
                tarnsformed_row = transform_row(row=row)
                insert_record(conn=conn,cur=cur,schema=schema,table_name=fact_table_name,row=tarnsformed_row)
            
            else:
                transformed_row = transform_row(row)
                if transformed_row["Video_Id"] in table_ids:
                    update_record(conn=conn,cur=cur,schema=schema,table_name=fact_table_name,row=transformed_row)
        ids_to_delete=set(table_ids) - current_video_ids
        
        if ids_to_delete :
            delete_records(conn=conn,cur=cur,schema=schema,table_name=fact_table_name,ids_to_delete=ids_to_delete)
        logger.info(f"{schema}.{fact_table_name} data update completed.")
    except Exception as err:
        logger.error(f"Error Occured while updating {schema}.{fact_table_name} due to {err}")
        raise err
    finally:
        if conn and cur :
            close_pg_curr_conn(conn=conn,cur=cur)