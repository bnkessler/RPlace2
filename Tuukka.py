import pandas as pd
import polars as pl
from Functions import *
from plotnine import *

# Import Data
data = pl.scan_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/CombinedPlaceData.parquet")

print("Getting user counts")
user_counts = data.group_by("user").agg(pl.col('user').count().alias('count'))

# Filter users by count
user_counts = user_counts.filter(pl.col('count') > 20)

# Join with count numbers
data = data.join(user_counts, on = 'user')

# Convert Coordinates
print("Parsing Coordinates")
data = data.with_columns(
    pl.col('coordinate').map_elements(parse_coordinate, return_dtype=pl.Object)
)
data = data.with_columns([
    pl.col('coordinate').map_elements(lambda x: int(x.split(",")[0]), return_dtype=pl.Int32).alias('x'),
    pl.col('coordinate').map_elements(lambda x: int(x.split(",")[1]), return_dtype=pl.Int32).alias('y')
])

data = data.drop('coordinate')
data = data.drop('pixel_color')

# Convert Date Column
print("Casting to date")
data = data.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S%.f %Z').cast(pl.Datetime, strict=False))

print("Sorting user, timestamp")
data = data.sort('user', 'timestamp')

# Take Date Difference for each row based on user
data_diff = data.group_by('user', maintain_order = True).agg(pl.col('timestamp').diff(null_behavior = 'drop').alias('diff')).explode('diff').drop_nulls()

# Drop the 80th 
data_diff = time_diff_quantile_filter(data_diff, 'diff', 0.80)

# Take the statistical values for each user
data_mean_std = data_diff.group_by('user').agg([
    pl.col('diff').mean().alias('mean'),
    pl.col('diff').std().alias('std'),
    pl.col('diff').median().alias('median')
    ])

# Join with user counts for no real utility reason but it's nice to see
stat_data = user_counts.join(data_mean_std, on = 'user')

# Set filter for Median and STD
low_numbers = stat_data.filter((pl.col('std') < pl.duration(minutes = 0, seconds = 30)) | (pl.col('median') < pl.duration(minutes = 2, seconds = 30)))

# Below is just code for writing outputs

#data = data.filter(pl.col('user').is_in(low_numbers.select('user').collect(streaming=True)))

#data.collect(streaming=True).write_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Tuukka_Exports/Tuukka_Plot.parquet")

# for row in data.iter_rows():
#     x, y, color = row[4], -row[5], row[2]
#     pixel_dict[(x, y)] = color
    
# for coord, color in pixel_dict.items():
#     x, y = coord
#     x, y = place_to_pil_coordinates(x, y, (3000, 2000))
#     rgb_color = hex_to_rgb(color)
    
#     # Set pixel color
#     image.putpixel((x, y), rgb_color)

# image.save("TuukaImage.png")

#low_numbers.collect(streaming = True).write_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Tuukka_Exports/Users_Filtered_SD_Median.parquet")

#low_number_desc = low_numbers.describe().drop('user')



#stat_data_desc = stat_data.describe(percentiles = (0.001,0.01,0.1,0.25,0.5,0.75)).drop('user')
#low_number_desc.write_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Tuukka_Exports/Filtered_Described.parquet")
#stat_data_desc.write_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Tuukka_Exports/Stat_Data_Described.parquet")
#stat_data_desc.write_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Tuukka_Exports/Combined_Desc.parquet")
