import pandas as pd
import polars as pl
import datetime as datetime
from Functions import parse_coordinate

# If using read_parquet, do print(df). If using scan_parquet do print(df.collect(streaming=True))
#load data
df = pl.scan_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/CombinedPlaceData.parquet")

# prepare to parse
df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S%.f %Z').cast(pl.Datetime, strict=False))

# print(df.collect(streaming=True))
# print(df)

print("Real User Check: Remaining users = Total Unique Users minus bot check and admin check user counts")

# Getting unique users and identifiers 
# unique_users_count = df.select(pl.col("user")).unique().height
# checking unique users count
# print(f"Unique users: {unique_users_count}")

df_unique = pl.read_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/CombinedPlaceData.parquet")

# Unique user dataframe
unique_users = df_unique.select(pl.col("user")).unique()
# Adding unique identifier
unique_users = unique_users.with_columns(pl.arange(0, unique_users.height, eager=True).alias("user_id") + 1)
# merge back on original file
df_unique = df_unique.join(unique_users, on="user", how="left")
df_unique = df_unique.drop("user")
# print(df_unique)

# sanity check for user_id
# max_user_id = df.select(pl.col("user_id").max()).to_numpy()[0]
# print(f"The largest value in the user_id column is: {max_user_id}")





print("Bot Checks: Users with 20+ placements & only 1 color. Users with 20+ placements and only 1 coordinate. Users who placed tiles for 16+ hours a day. Users with the lowest variance and 20+ placements. Mods/admins are those who placed more than what is possible in set timeframe")


# 20 placements and 1 color bot detection
user_color_counts = df_unique.group_by(["user_id", "pixel_color"]).agg([
    pl.count().alias("count")
])
# 20+ frequency filter
user_color_counts_20 = user_color_counts.filter(pl.col("count") >= 20)
# one color filter
users_with_single_color = user_color_counts_20.group_by("user_id").agg(
    pl.col("pixel_color").count().alias("unique_colors")
).filter(pl.col("unique_colors") <= 1)
# List of user_ids meeting the criteria above
user_ids = users_with_single_color.select("user_id")
print(user_ids)
# 348,172 users detected as bots based on criteria above.
total_colors_by_users = df_unique.join(user_ids, on="user_id", how="inner")
# Counting the total number of color used by who placed 20 times with it
total_colors_by_users_count = total_colors_by_users.select(pl.count()).to_numpy()[0]
print(f"Total pixels placed by users who only placed pixels at one coordinate 20+ times: {total_colors_by_users_count}")
# 22,820,612 pixels were placed by these users



# Users who only placed in 1 coordinate and at least 20 times
user_coordinate_counts = df_unique.group_by(["user_id", "coordinate"]).agg([
    pl.len().alias("count")  # Using pl.len() to count occurrences
])
# first filter by frequency
user_coordinate_counts_20 = user_coordinate_counts.filter(pl.col("count") >= 20)
# then filter by only coordinate
users_with_single_coordinate = user_coordinate_counts_20.group_by("user_id").agg([
    pl.col("coordinate").count().alias("unique_coordinates")
]).filter(pl.col("unique_coordinates") == 1)
# Looking at user_id count
user_ids2 = users_with_single_coordinate.select("user_id")
print(user_ids2)
# 4,684 users placed 25+ times in the same spot and only one spot.
total_pixels_by_users = df_unique.join(user_ids2, on="user_id", how="inner")
# Counting the total number of pixels placed by users who placed 25+ times in only one spot.
total_pixel_count = total_pixels_by_users.select(pl.count()).to_numpy()[0]
print(f"Total pixels placed by users who only placed pixels at one coordinate 20+ times: {total_pixel_count}")
# 614,772 pixels were placed by these users



# users that placed 16+ hours worth of time in any given day.
df200 = df_unique.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S%.f %Z', strict=False))
df200 = df200.with_columns(df200["timestamp"].dt.date().alias("date"))
# group by user and date
user_daily_counts = df200.groupby(["user_id", "date"]).agg([
    pl.count().alias("daily_count")
])
# 192+ placements in a day. 16hours*12placements per hour
users_with_200_placements = user_daily_counts.filter(pl.col("daily_count") >= 192)
print(users_with_200_placements)
# 6,190 users with 192+ placements
total_placements_by_users = df_unique.join(users_with_200_placements, on="user_id", how="inner")
# Counting the total number of pixels placed by users who placed 25+ times in only one spot.
total_placement_count = total_placements_by_users.select(pl.count()).to_numpy()[0]
print(f"Total pixels placed by users who placed for 16+ hours or 192+ times: {total_placement_count}")
# 4,926,743 placements by the above users


# users with very low variance between tile placements and 20+ placements
df_unique2 = df_unique.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S%.f %Z').cast(pl.Datetime, strict=False))
df_diff = df_unique2.with_columns(
    pl.col("timestamp").diff().over("user_id").alias("time_diff")
)

# Group by user_id to calculate mean time difference and count the number of placements
df_aggregated = df_diff.groupby("user_id").agg([
    pl.col("time_diff").mean().alias("mean_time_diff"),
    pl.count("time_diff").alias("placements_count")
])

# Filter to include only users with more than one placement
df_var= df_aggregated.filter((pl.col("placements_count") > 20) & (pl.col("mean_time_diff") < datetime.timedelta(minutes=1)))

print(df_var)



print("Combining all detected bots into one list and getting rid of duplicates")

all_user_ids = pl.concat([
    user_ids.select("user_id"),
    user_ids2.select("user_id"),
    users_with_200_placements.select("user_id"),
    df_var.select("user_id")
], how="vertical")
unique_user_ids = all_user_ids.unique()
print(unique_user_ids)
# 353,744 bots after combining all methods, about 28,000,000 million placed.





print("Admins/mods check: Number of users with 20+ placements in any 1 hour period. More than what is possible for users and bots")

df_admin = df_unique.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S%.f %Z', strict=False))
# pulling date and hour combinations
df_admin = df_admin.with_columns([
    df_admin["timestamp"].dt.date().alias("date"),
    df_admin["timestamp"].dt.hour().alias("hour")
])
# grouping date and hour for each user
hourly_counts = df_admin.groupby(["user_id", "date", "hour"]).agg([
    pl.count().alias("count")
])
# Users with 50+ placements in an hour
users_with_50_plus_per_hour = hourly_counts.filter(pl.col("count") >=50)
unique_users_with_50_plus_per_hour = users_with_50_plus_per_hour.select("user_id").unique()
print(unique_users_with_50_plus_per_hour)
# 5,787 admins/mods detected

placements_50_users = df_unique.join(unique_users_with_50_plus_per_hour, on="user_id", how="inner")
# Counting the total number of pixels placed by mods/admins
placement_50count = placements_50_users.select(pl.count()).to_numpy()[0]
print(f"Total pixels placed by admins/mods: {placement_50count}")
# 1,334,310 placements by mods/admins

# Exporting list of bot, admin, and unique file to folder
unique_user_ids.write_parquet("/home/ec2-user/environment/RedditPlacePresentation_Phase2/Tony_Exports/BotUsers.parquet")
unique_users_with_50_plus_per_hour.write_parquet("/home/ec2-user/environment/RedditPlacePresentation_Phase2/Tony_Exports/AdminUsers.parquet")
unique_users.write_parquet("/home/ec2-user/environment/RedditPlacePresentation_Phase2/Tony_Exports/UserID.parquet")

df = pl.read_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/CombinedPlaceData.parquet")
# df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S%.f %Z').cast(pl.Datetime, strict=False))

#load data
df2 = pl.read_parquet("/home/ec2-user/environment///RedditPlacePresentation_Phase2/Tony_Exports/UserID.parquet")
df3 = pl.read_parquet("/home/ec2-user/environment///RedditPlacePresentation_Phase2/Tony_Exports/BotUsers.parquet")
df4 = pl.read_parquet("/home/ec2-user/environment///RedditPlacePresentation_Phase2/Tony_Exports/AdminUsers.parquet")


df = df.with_columns(pl.lit("static_value").alias("user_id"))
df = df.with_columns(pl.lit("static_value").alias("bot"))
df = df.with_columns(pl.lit("static_value").alias("admin"))
df = df.with_columns(pl.lit("static_value").alias("normal"))


joined_df = df.join(df2, on="user", how="left", suffix="_df2")

# replace 'user_id' in df with 'user_id' from df2
result_df = joined_df.select([
    # Select all columns from df except the original 'user_id'
    *[col for col in df.columns if col != "user_id"],
    # Replace 'user_id' with the one from df2
    pl.col("user_id_df2").alias("user_id")
])

# making new column for megring later
df_joined = result_df.join(df3.with_columns(pl.lit(1).alias("is_bot")), on="user_id", how="left")
# matching user with user_id
df_updated = df_joined.with_columns(
    pl.when(pl.col("is_bot") == 1)
    .then(1)  # User_id is found in df3, mark as bot
    .otherwise(0)  # User_id not found in df3, not a bot
    .alias("bot")
)
# dropping duplicate column
df_final = df_updated.drop("is_bot")



# making new column to merge later
df_joined = df_final.join(df4.with_columns(pl.lit(1).alias("is_admin")), on="user_id", how="left")
# mathcing user with user_id based on dictionary
df_updated = df_joined.with_columns(
    pl.when(pl.col("is_admin") == 1)
    .then(1)  # User_id is found in df4, mark as admin
    .otherwise(0)  # User_id not found in df4, not an admin
    .alias("admin")
)

# dropping unnecesary columns
df_final = df_updated.drop("is_admin")


df = df_final.with_columns(
    pl.when(
        (pl.col("bot") == 0) & (pl.col("admin") == 0)
    ).then(1)
    .otherwise(0)
    .alias("normal")
)

df = df.drop("user")

df.write_parquet("/home/ec2-user/environment/RedditPlacePresentation_Phase2/Tony_Exports/OKdata.parquet")