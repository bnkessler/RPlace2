from datetime import *
import polars as pl

def parse_coordinate(coordinate):
    if len(coordinate) < 20:
        return coordinate
    else:
        coordinate = coordinate.replace(" ", "").replace(":", ",").split(",")
        x = coordinate[1]
        y = coordinate[3]
        return f"{x}" + "," + f"{y}"
 
       
def parse_timestamp(timestamp):
    # Check if the timestamp has microsecond part
    if '.' in timestamp:
        return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f %Z")
    else:
        return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S %Z")
        

def time_convert(diff):
    diff = str(diff.replace(' UTC', ''))
    diff = str(diff).replace('2023-07-20', '0')
    diff = str(diff).replace('2023-07-21', '1')
    diff = str(diff).replace('2023-07-22', '2')
    diff = str(diff).replace('2023-07-23', '3')
    (days, time) = diff.split(' ')
    (h,m,s) = time.split(':')
    results = float(days) * 86400 + float(h) * 3600 + float(m) * 60 + float(s)
    return results
    
    
def time_diff_quantile_filter(dataset, diff_column, quantile):
    quantile_set = dataset.group_by('user').agg(pl.col(diff_column).quantile(quantile, 'nearest').alias(str(quantile)))

    dataset = dataset.join(quantile_set, on = 'user')
    
    dataset = dataset.filter(pl.col(diff_column) < pl.col(str(quantile)))
    
    dataset = dataset.drop(str(quantile))
    
    return dataset
    