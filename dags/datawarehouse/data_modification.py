import logging


logger =logging.getLogger(__name__)

def insert_record(conn,cur,schema,table_name,row) :
    try:
        if schema=="staging" :
            video_id="video_id"
            
            cur.execute(f"""
                        insert into {schema}.{table_name} ("Video_Id","Video_Title","Upload_Date","Duration","Video_Views","Likes_Count","Comments_Count")
                        values(%(video_id)s,%(title)s,%(publishedAt)s,%(duration)s,%(viewCount)s,%(likeCount)s,%(commentCount)s);
                        """,row)
            conn.commit 
        else:
            video_id="Video_Id"
            
            cur.execute(f"""
                        insert into {schema}.{table_name}("Video_Id","Video_Title","Upload_Date","Duration","Video_Type","Video_Views","Likes_Count","Comments_Count")
                        values(%(Video_Id)s,%(Video_Title)s,%(Upload_Date)s,%(Duration)s,%(Video_Type)s,%(Video_Views)s,%(Likes_Count)s,%(Comments_Count)s);
                        """,row)
            conn.commit
            
            logger.info(f"Inserted row for Video_Id {row[video_id]}")       
        
    except Exception as err:
        logger.error(f"Error inserting row for Video_Id {row[video_id]} - {err}")    
        raise err
    
def update_record(conn,cur,schema,table_name,row,) :
    
    try:
        
        if schema =="staging" :
            video_id="video_id"
            video_title="title"
            upload_date="publishedAt"
            duration="duration"
            video_views="viewCount"
            likes_count="likeCount"
            comments_coun="commentCount"
        else :
            video_id="Video_Id"
            video_title="Video_Title"
            upload_date="Upload_Date"
            duration="Duration"
            video_views="Video_Views"
            likes_count="Likes_Count"
            comments_coun="Comments_Count"
            
        cur.execute( f"""
          update {schema}.{table_name}
          set  "Video_Title"=%({video_title})s,
               "Duration"=%({duration})s,
               "Video_Views" =%({video_views})s,
               "Likes_Count"=%({likes_count})s,
               "Comments_Count"=%({comments_coun})s
           where "Video_Id" = %({video_id})s and "Upload_Date"=%({upload_date})s ;
        """,row)
        
        # cur.execute(update_sql,row)
        conn.commit
        
        logger.info(f"Updated row for Video_Id {row[video_id]}")       
        
    except Exception as err:
        logger.error(f"Error updating row for Video_Id {row[video_id]} - {err}")    
        raise err

def delete_records(conn,cur,schema,table_name,ids_to_delete) :
    try:
        
        ids_to_delete= f"""({','.join(f"'{id}'" for id in ids_to_delete)})"""
        
        cur.execute(f"""
                     delete from {schema}.{table_name}
                     where "Video_Id" in {ids_to_delete}
                    """)        
        conn.commit
        logger.info(f"Deleted rows with the video ids {ids_to_delete}")       
        
    except Exception as err:
        logger.error(f"Error while deleting ids {ids_to_delete} - {err}")    
        raise err
        
