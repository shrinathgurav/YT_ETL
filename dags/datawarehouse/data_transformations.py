from datetime import timedelta,datetime

def extrat_duration (duration_string : str) :
    duration_string = duration_string.replace("P",'').replace("T",'')
    
    components =["D","H","M","S"]
    values = {"D":0,"H":0,"M":0,"S":0}
    
    for component in components :
        if component in duration_string:
            value,duration_string =duration_string.split(component)
            values[component] = int(value)
            
    total_duration = timedelta(days=values["D"],hours=values["H"],minutes=values["M"],seconds=values["S"])            
    
    return total_duration


def transform_row (row) :
    dur_timedelta = extrat_duration(row["Duration"])
    row["Duration"] = (datetime.min +dur_timedelta).time()
    row["Video_Type"] = "Shorts" if dur_timedelta.seconds <=60  else "Normal"
    return row
