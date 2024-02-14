import polars as pl
import pandas as pd
import numpy as np
from Functions import *


place = pl.scan_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/CombinedPlaceData.parquet")
place2 = place.clone()
print("Casting to Datetime")
place = place.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S%.f %Z').cast(pl.Datetime, strict=False))
place = place.with_columns(
    pl.col('coordinate').map_elements(parse_coordinate, return_dtype=pl.Object)
)
print("Parsing coordinates")
place = place.with_columns([
    pl.col('coordinate').map_elements(lambda x: int(x.split(",")[0]), return_dtype=pl.Int32).alias('x'),
    pl.col('coordinate').map_elements(lambda x: int(x.split(",")[1]), return_dtype=pl.Int32).alias('y')
])

place1 = place.clone()

print(place.collect(streaming=True))

#Removing approx white tiles
place1 = place1.slice(1, 120000000)

#Count of pixels per user
pix_per_u = place.select("user").group_by("user").agg(pl.col('user').count().alias('pixels_placed')).sort('pixels_placed', descending = True)

#Sorted by user and timestamp
place1 = place1.sort(['user', 'timestamp'])

#Df with time differences between pixels placed
usertd = place1.group_by("user", maintain_order = True).agg(pl.col('timestamp').diff(null_behavior = 'drop').alias('time_diff')).explode('time_diff').drop_nulls()

#### Admin Finder ####

#Converting time difference to seconds
usertd = usertd.with_columns([
    (pl.col("time_diff").cast(pl.Int64) / 1000000).floor().alias("time_diff_seconds")])

#Filtering to less than 4 minute differences
usertdAdmin = usertd.filter(pl.col("time_diff_seconds")<240)

#grouping by user with counts of pixels placed by user with time differences less than 240
usertdAdmin = usertdAdmin.group_by("user").agg(pl.col("user").count().alias("count_of_less_than_four"))

#more than 25 pixels placed
AdminUsers = usertdAdmin.filter(pl.col("count_of_less_than_four")>=25).select("user")

#All pixels placed by admins
Admins = place2.filter(pl.col("user").is_in(AdminUsers.collect(streaming=True)))


####Plot Creation###

#load in admin pixels
Admins = pl.scan_parquet("/home/ec2-user/environment/RedditPlacePresentation_Phase2/Jake_Exports/AdminPixels.parquet")

print("Casting to Datetime")
Admins = Admins.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S%.f %Z').cast(pl.Datetime, strict=False))
Admins = Admins.with_columns(
    pl.col('coordinate').map_elements(parse_coordinate, return_dtype=pl.Object)
)
print("Parsing coordinates")
Admins = Admins.with_columns([
    pl.col('coordinate').map_elements(lambda x: int(x.split(",")[0]), return_dtype=pl.Int32).alias('x'),
    pl.col('coordinate').map_elements(lambda x: int(x.split(",")[1]), return_dtype=pl.Int32).alias('y')
])

################Ending Plot##############################

#getting the top coord
top_coord = Admins.group_by(["x", "y"]).agg(pl.col('timestamp').arg_max().alias('tmax'),pl.col("pixel_color").last().alias("top_color"))

print(top_coord.collect(streaming=True))
AdminPlot = (ggplot(top_coord.collect(streaming=True), aes(x = "x", y = "y", color = "top_color")) + 
    geom_point(shape = "s", size = .5) +
    theme(figure_size=[15, 10]) +
    scale_y_reverse() +
    scale_color_identity()
    )


#def index_of_start_white(df):
#    for row in df.iter_rows():
#        if df.item([row, "pixel_color"]) != "#FFFFFF":
#            return row
#    return row

#index = index_of_start_white(place)
#print(index)
#print(place.collect(streaming=True))

#user_attr = usertd.group_by("user", maintain_order = True).agg(pl.col("time_diff").max().alias('max_diff'),
#pl.col("time_diff").min().alias('min_diff'),
#pl.col("time_diff").mean().alias('mean_diff'),
#pl.col("time_diff").std().alias('std_diff')
#)

#user_attr = user_attr.sort("std_diff", descending = True)

#print(user_attr.select("pixels_placed").quantile(quantile=.99).collect(streaming=True))

#Classify
#Bot = user_attr.filter(pl.col("pixels_placed") >= user_attr.select("pixels_placed").quantile(quantile=.99)).collect(streaming=True)

#user_attr = user_attr.with_columns(pl.when(pl.col("user").is_in(Bot.select("user").collect(streaming=True))).then(1).otherwise(0).alias("Bot"))

#print(pix_per_u.collect(streaming = True).head())
#print(user_attr.collect(streaming = True).head())

#AdminUsers.collect(streaming = True).write_parquet("/home/ec2-user/environment/RedditPlacePresentation_Phase2/Jake_Exports/AdminUsers.parquet")

