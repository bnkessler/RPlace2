import pandas as pd
import polars as pl
import datetime as datetime
from Functions import parse_coordinate


ben_pbs = pl.scan_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Ben Possible Bots.parquet")
tuukka_pbs = pl.scan_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Tuukka_Exports/Users_Filtered_SD_Median.parquet")
tuukka_pbs = tuukka_pbs.select("user")
jake_admins = pl.scan_parquet("/home/ec2-user/environment/RedditPlacePresentation_Phase2/Jake_Exports/AdminUsers.parquet")
tb_pbs = pl.scan_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Tuukka_Exports/BenUsers_Filtered_SD_Median.parquet")
tb_pbs = tb_pbs.select("user")

pbs = ben_pbs.join(tb_pbs, on = "user", how = "inner")

pbs_na = pbs.join(jake_admins, on = "user", how = "left")

pbs_na.collect(streaming=True).write_parquet("/home/ec2-user/environment/RedditPlacePresentation_Phase2/BelievedBots2.parquet")


