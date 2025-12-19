from datetime import date
import json
import logging

logger =logging.getLogger(__name__)


def get_data():
    
    filepath=f"./data/YT_data_{date.today()}.json"
    
    try:
        logger.info(f"Processing file YT_data_{date.today()}.json")
        with open(file=filepath,mode="r",encoding="utf-8") as file_data:
            raw_data=json.load(file_data)
        
        logger.info("returning with raw data from file.") 
        return raw_data
    except FileNotFoundError:
        logger.error(f"FIle not found at specified path. Check file path.{filepath}")
        raise
    except json.JSONDecodeError :
        logger.error("Error with json data. Pleae check the data issues in file.")
        raise
            