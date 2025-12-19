from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
import logging 

logger=logging.getLogger(__name__)

def get_pg_conn_cur() :
    pg_hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt",database="elt_db")
    conn = pg_hook.get_conn()
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    return conn,cur


def close_pg_curr_conn(cur,conn):
    cur.close()
    conn.close()


def create_schema(conn,cur,schema) :
    try:
        
        # conn,cur = get_pg_conn_cur()
        logger.info(f"creating schema {schema} if not exists into {conn.info.dbname}")
        schema_sql =f"CREATE SCHEMA IF NOT EXISTS {schema} ;"
        cur.execute(schema_sql)
        conn.commit
        logger.info(f"created schema {schema} successfully.")
    except Exception as err:
        logger.error(f"Error cocured while creating schema {schema} -{err}")
    # finally:
    #     close_pg_curr_conn(cur,conn)
    

def create_table(conn,cur,schema,table_name) :
    try:
        logger.info(f"creating table into {schema}.{table_name} if not exists.")
        conn,cur = get_pg_conn_cur()
        
        if schema=="staging" :
            table_sql =f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name}(
                    "Video_Id" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT 
                ) ;
            """
        else:
            table_sql =f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name}(
                    "Video_Id" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" TIME NOT NULL,
                    "Video_Type" VARCHAR(10) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT 
                ) ;
            """  
        
        cur.execute(table_sql)
        conn.commit()
    except Exception as err:
        logger.error(f"Error occured while creating table {schema}.{table_name} - {err}")
    # finally:
        # close_pg_curr_conn(cur,conn)
    
def get_vodeo_ids(cur,schema,table_name) :
    try:
        logger.info(f"gathering video_ids from {schema}.{table_name} .")
        cur.execute(f"""select "Video_Id" from {schema}.{table_name} ;""")   
        ids= cur.fetchall()
        
        video_ids = [row["Video_Id"] for row in ids]
        
        return video_ids
    except Exception as err :
        logger.error(f"Error occured while gathering video ids for {schema}.{table_name} - {err} ")