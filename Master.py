# Compile Final Code Here
import polars as pl

data = pl.read_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Tuukka_Exports/Users_Filtered_SD_Median.parquet")

sum_ = data.select('count').sum()

print(sum_)



# Tuukka
print(pl.read_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Tuukka_Exports/Users_Filtered_SD_Median.parquet"))
print(pl.read_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Tuukka_Exports/Combined_Desc.parquet").head(10))
print(pl.read_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Tuukka_Exports/Filtered_Described.parquet"))
print(pl.read_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Tuukka_Exports/Stat_Data_Described.parquet"))