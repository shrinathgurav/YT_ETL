from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

def get_pg_conn_cur() :
    pg_hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt",database="elt_db")
    conn = pg_hook.get_conn()
    cur = conn.get_cursor(cursor_factory=RealDictCursor)
    
    return conn,cur


def close_pg_curr_conn(cur,conn):
    cur.close()
    conn.close()


def create_schema(schema) :
    conn,cur = get_pg_conn_cur()
    schema_sql ="CREATE SCHEMA IF NOT EXISTS {schema} ;"
    cur.execute(schema_sql)
    conn.commit
    
    close_pg_curr_conn(cur,conn)
    

def create_table(schema,table) :
    conn,cur = get_pg_conn_cur()
    
    if schema=="staging" :
        table_sql =f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name}(
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
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
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
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
    
    close_pg_curr_conn(cur,conn)
    
def get_vodeo_ids(cur,schema,table) :
    cur.execute(f"""select "Video_id" from {schema}.{table} ;""")   
    ids= cur.fetchall()
    
    video_ids = (row["Video_Id"] for row in ids)
    
    return video_ids