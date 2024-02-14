
import pandas as pd
import polars as pl
import numpy as np
from Functions import *
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from plotnine import *

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
#print(Admins.collect(streaming=True))

################Ending Plot##############################

top_coord = Admins.group_by(["x", "y"]).agg(pl.col('timestamp').arg_max().alias('tmax'),pl.col("pixel_color").last().alias("top_color"))

print(top_coord.collect(streaming=True))
AdminPlot = (ggplot(top_coord.collect(streaming=True), aes(x = "x", y = "y", color = "top_color")) + 
    geom_point(shape = "s", size = .5) +
    theme(figure_size=[15, 10]) +
    scale_y_reverse() +
    scale_color_identity()
    )


AdminPlot.save(limitsize=False, filename="Admin_Init_PlotJL.png")

##############Plot by Time Stamp###########
