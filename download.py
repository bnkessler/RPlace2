import polars as pl
import pandas as pd
import numpy as np
import requests
import pyarrow.parquet as pq

# If file is corrupted the URL needs to be changed, it is a temporary download link
url = "https://rplace-bttj.s3.us-east-2.amazonaws.com/combined_place_data.parquet?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEHMaCXVzLXdlc3QtMSJHMEUCIQD6Ezk9RsITy43G%2FoZMvgOhgF6Mw4jBwX%2BeHbtU1fOlBQIgA5r45BfZ%2FqAZy%2BN5LQvoo3cxTlVMuRNldLq9Q91aKZcqxwMITBADGgw5NDM2ODY4MDcxODkiDFAJ411MWYkYCGfljCqkA%2FpLE1DTDWr8I2ZGE7EE00zTlMlKZCdax0HXTAWNSPalTYc1QQfOAQ%2Bf3QV5MrgGmV9HFW5evxdXpgB0MFQ8dGO8SJKAgiuTrb3BP1hrU2B8d0%2Bk9X3Mu8vVS792eU4EfSMR4KQzydNIKea%2BZYxw41dR4Wa4CqdPcqUOrtuUdSOBBEJHReZ0T%2Bia1fOr%2FVrvUyNwyT6Rr%2FzP3l%2BSTvESkpgDQZHhqPWTi8%2FnCaYmVafQiRzOlnjvgXCRaYJPWdIwS7s6d3H2LLk1U%2FxIbMKGm8PTnaKv%2BubvcQud5gavfV7%2B6Q11NTcmAKV0ilzzFTGJIifiUX%2BNQax3p9jCtvlrdkHKuX%2B7KdUtUaR5Thazg90o0plYaLSy0z4zEIjIhcUxVN%2FhTebCf2oXU2WxFDDpHBLMr4AqA%2FKsFIPt3hmhWj9%2F2dmL9XNzFY%2BBFTMZXeLTMYmabZx3ybMmAQKpuGuHUUni6P6iRAl8p8gKDGYlCtz6%2B4ojz1xNc66kyU%2BmPZBRWer%2BhzXAUL1bikoKHqXIqNFEI3u4b%2BOMtxu89DIN3%2F9i5tiR1jCeo6SuBjqUArDfPhixg1k2DyaGWX%2BJ5%2BR8rHnyYplIc9A2okFM3K32PhtDStMDJqGMp73YBEyGmO3t7v3xzHX0tTQ1azqTsXN%2Fi9PdJOq%2F3%2B0qv0X8fYlQ44UqOxWbh5N4LMwD9tpd2k4mGFrFt2%2Fw1trjV9WPICHqhKhBbGmo8P25tHetFVjfG4Sk7YoWaJWFGX244t1KlHiPNaJQ%2Fk9W%2F1zXTM%2BZiVhzqw%2FVt0R0co6gc%2FvFQCo8L00eiOfhNt9m7q0OtkfSaQd1uFyT3bMLF%2FIBfvoOHMWyWBEVFsEn7x45U4R8eWBnRth9pTgLEC86GrR8kglXtuNwfTimW%2BiFbKvHw%2Fu5mO1%2BZuuLj2lTnAZ3QB%2BxbvTR1wE7Eg%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20240211T182842Z&X-Amz-SignedHeaders=host&X-Amz-Expires=3600&X-Amz-Credential=ASIA5XOA552KUOTZLDMO%2F20240211%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Signature=7d7a62ffd15585462c4bea344eaf9178988dbd8b99530d390358ffb49086c21d"

try:
    response = requests.get(url)
    response.raise_for_status()
    with open('CombinedPlaceData.parquet', 'wb') as f:
        f.write(response.content)
    print("File downloaded successfully")

except requests.exceptions.RequestException as e:
    print("Failed to download file:", e)
except Exception as e:
    print("Error:", e)