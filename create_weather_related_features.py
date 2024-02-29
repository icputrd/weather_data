# This script computes weather-history-related image features per image (row of img_file).
# To run: python3 create_weather_related_features.py
import pandas as pd
import numpy as np
import datetime
import sys

img_file = '/data/anau/PMI_estimation/data/img_PMIs_no_negs.csv'  # row=image
weather_file = './data/LCD/lcd_hourly.pkl'  # contains hourly temp and humidity data

imgs_df = pd.read_csv(img_file, usecols=['new_path', 'new_img', 'old_date', 'date_placed_ARF'])
weather_df = pd.read_pickle(weather_file)  # columns: datetime, temp, humidity
print(imgs_df.shape, weather_df.shape)

# rename columns
weather_df.rename(columns={"HourlyDryBulbTemperature": "temp_C",
    "HourlyRelativeHumidity": "rel_humidity"}, inplace=True)

# convert date columsn to datetime dtype
imgs_df['old_date'] = pd.to_datetime(imgs_df['old_date'], format='%Y-%m-%d', errors='coerce')
imgs_df['date_placed_ARF'] = pd.to_datetime(imgs_df['date_placed_ARF'], format='%Y-%m-%d', errors='coerce')
weather_df['date_time'] = pd.to_datetime(weather_df['date_time'], format='%Y-%m-%d', errors='coerce')

imgs_dict = imgs_df.to_dict('records')
row_counter = 0
# for each image
for row in imgs_dict:
    placement_date = row['date_placed_ARF']
    img_date = row['old_date']
    
    ADD = 0
    ADH = 0
    start_date = placement_date
    while (start_date <= img_date):
        print(start_date, img_date) 
        # get 
        weather_current_df = weather_df[weather_df.date_time.dt.date == start_date]
        #print(weather_current_df)

        # calculate ADD
        avg_temp = weather_current_df.loc[:, 'temp_C'].mean()
        print('daily_avg_temp:', avg_temp)
        if avg_temp >= 0:
            ADD += avg_temp

        #calculate ADH
        total_temp = weather_current_df[weather_current_df.temp_C >= 0]['temp_C'].sum()
        print('daily_total_temp:', total_temp)
        ADH += total_temp
    
        # go to the next day
        start_date += datetime.timedelta(days=1)
    
    # add to dict
    row['ADD'] = ADD
    row['ADH'] = ADH
    print(row)
    
    # write to file every 10k iterations
    if row_counter % 10000 == 0:
        print('row_no:', row_counter)
        pd.DataFrame.from_dict(imgs_dict).to_pickle('./data/out/imgs_df_'+str(row_counter)+'.pkl')
    row_counter += 1
    print()
    print()
    print()

# convert dict to dataframe
imgs_df = pd.DataFrame.from_dict(imgs_dict)
# write to pickle
imgs_df.to_pickle('./data/out/imgs_df.pkl')
