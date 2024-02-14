import pandas as pd
import polars as pl
import datetime as datetime
from Functions import parse_coordinate

print("Scanning Parquet")
df = pl.scan_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/CombinedPlaceData.parquet")

print("Casting to date")
df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S%.f %Z').cast(pl.Datetime, strict=False))

print("Parsing Coordinates")
df = df.with_columns(
    pl.col('coordinate').map_elements(parse_coordinate, return_dtype=pl.Object)
)
df = df.with_columns([
    pl.col('coordinate').map_elements(lambda x: int(x.split(",")[0]), return_dtype=pl.Int32).alias('x'),
    pl.col('coordinate').map_elements(lambda x: int(x.split(",")[1]), return_dtype=pl.Int32).alias('y')
])

print("Extracting Day, Hour, Minute")
df = df.with_columns(
    pl.col("timestamp").dt.date().alias('day'),
    pl.col("timestamp").dt.hour().alias('hour'),
    pl.col("timestamp").dt.minute().alias('minute')
)

# Dropping coordinate
df = df.drop(["coordinate", "timestamp"])

# Creating x and y bins
bin_size = 10
df = df.with_columns((pl.col("x") // bin_size).alias("x-bin"))
df = df.with_columns((pl.col("y") // bin_size).alias("y-bin"))

# Dropping coordinate (again)
df = df.drop(["x", "y"])

# Grouping by day, hour, minute, usernmae
print("Grouping")
grouped_df = df.group_by(["day", "hour", "minute", "x-bin", "y-bin"])

# 90th quantile of mass placement per minute is 5 for a 10x10 square
# 95th quantile is 9 for a 10x10 square
# 99th quantile is 23 for a 10x10 square
# Max is 4480, top left corner
print("Counting")
threshold = 25
counts = grouped_df.agg(pl.len().alias("count")).sort(by = pl.col("count"), descending=True).filter(pl.col("count") >= threshold)



print("Joining")
joined_df = df.join(counts, on = ["day", "hour", "minute", "x-bin", "y-bin"], how = "inner")

pb_th25 = joined_df.select(pl.col("user")).unique()

# Users by number of pixels placed
users_df = df.group_by("user").len().sort(by = "len", descending=True)

# single_users_df = users_df.with_columns(pl.col("len")).filter(pl.col("len") == 1)
# ten_users_df = users_df.with_columns(pl.col("len")).filter((pl.col("len") > 1) & (pl.col("len") <= 10))
# tf_users_df = users_df.with_columns(pl.col("len")).filter((pl.col("len") > 10) & (pl.col("len") <= 25))
# otf_users_df = users_df.with_columns(pl.col("len")).filter(pl.col("len") > 25)

single_many_users_df = users_df.with_columns(pl.col("len")).filter((pl.col("len") == 1) | (pl.col("len") > 25)).select(pl.col("user"))

# print(single_many_users_df.collect(streaming=True))

print("Joining 2")
possible_bots = pb_th25.join(single_many_users_df, on = "user", how = "inner")

print("Collecting")
# print(possible_bots.collect(streaming=True))

possible_bots.collect(streaming=True).write_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Ben Possible Bots.parquet")
