import pandas as pd
import polars as pl
from datetime import datetime, timedelta
import time as te
from Functions import parse_coordinate
from PIL import Image
import matplotlib.colors as mcolors

def hex_to_rgb(hex_code):
    rgb_tuple = mcolors.hex2color(hex_code)
    rgb_values = [int(val * 255) for val in rgb_tuple]
    return tuple(rgb_values)
    
def place_to_pil_coordinates(x, y, image_size):
    # Convert place coordinates to PIL coordinates
    image_width, image_height = image_size
    pil_x = x + image_width // 2
    pil_y = image_height // 2 - y
    return pil_x, pil_y


df = pl.scan_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/CombinedPlaceData.parquet")
pbs = pl.scan_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/BelievedBots2.parquet")
# pbs = pl.scan_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Ben Possible Bots.parquet")
# df = pl.scan_parquet("/home/ec2-user/environment//RedditPlacePresentation_Phase2/Jake_Exports/AdminPixels.parquet")



print("Joining df")
df = df.join(pbs, on = "user", how = "inner")

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


print("Creating Date list")
start_date = datetime(2023, 7, 20)
end_date = datetime(2023, 7, 25)

# Initialize list to store tuples
date_list = []

# Iterate over each date between start_date and end_date
current_date = start_date
while current_date <= end_date:
    # Iterate over each hour (0-23)
    for hour in range(24):
        # Iterate over each 15 minutes (0-59)
        for minute in range(0, 60, 15):
            # Append tuple of (day, hour, minute) to list
            date_list.append((2023, 7, current_date.day, hour, minute))
    # Move to the next day
    current_date += timedelta(days=1)



# Correct date_list
# date_list = date_list[52:567]

# Test date_list
# Need to redo images up to 2023-07-20 17:15
date_list = date_list[52:567]

# Last pixel placed at 07-25 21:38:35


def create_image(df):
    
    pixel_dict = {}
    print("Collecting Data")
    df = df.drop("user", "coordinate")
    data = pd.DataFrame(df.collect(streaming=True))
    data.columns = ["timestamp", "color", "x", "y"]
    
    for time in date_list:
        tic = te.time()
        print("Filtering Data")
        end_time = (time[0], time[1], time[2], time[3], time[4] + 14, 59, 999)
        img_data = data[(data["timestamp"] >= datetime(*time)) & (data["timestamp"] < datetime(*end_time))]

        print("Parsing Data")
        for row in img_data.iterrows():
            x, y, color = row[1][2], -row[1][3], row[1][1]
            pixel_dict[(x, y)] = color
        
        print("Checking Image Size")
        is_greater_than_499 = any(x > 499 or y > 499 for x, y in pixel_dict.keys())
        is_greater_than_999 = any(x > 999 or y > 999 for x, y in pixel_dict.keys())

        if is_greater_than_499 and not is_greater_than_999:
            image_size = (2000, 1000)
        elif is_greater_than_999:
            image_size = (3000, 2000)
        else:
            image_size = (1000, 1000)
        
        print(f"Creating Image for {datetime(*time)}")
        image = Image.new('RGB', image_size, 'white')
        
        print(f"Pixels to place: {len(pixel_dict)}")
        for coord, color in pixel_dict.items():
            x, y = coord
            x, y = place_to_pil_coordinates(x, y, image_size)
            rgb_color = hex_to_rgb(color)
            
            # Set pixel color
            image.putpixel((x, y), rgb_color)
        
        toc = te.time()
        print(f"Took {toc-tic} seconds to generate")
        print(f"Saving Image tl_image_{time}.png")
        image.save(f"/home/ec2-user/environment//RedditPlacePresentation_Phase2/Timelapse Images 2/tl_image_{time}.png")
            

    return pixel_dict


placed_pixels = create_image(df)

# image = Image.new('RGB', (3000, 2000), 'white')



# for row in data.iter_rows():
#     x, y, color = row[4], -row[5], row[3]
#     x, y = place_to_pil_coordinates(x, y, (3000, 2000))
#     rgb_color = hex_to_rgb(color)
#     image.putpixel((x, y), rgb_color)


