
import pandas as pd
import requests
import json
import matplotlib.pyplot as plt
import datetime
import sqlite3


endpoint = 'https://api.openaq.org/v1/measurements'
city = 'Beijing'
parameter = 'pm25'
date_from = '2022-01-01'
date_to = '2023-01-31'

# Set the query parameters for the API request
params = {
    'city': city,
    'parameter': parameter,
    'date_from': date_from,
    'date_to': date_to,
    'limit': 10000 # maximum number of results per request
}

# Send the API request and retrieve the response
response = requests.get(endpoint, params=params)

# Extract the measurement data from the response JSON
measurements = response.json()['results']

# Convert the measurement data to a Pandas DataFrame
df = pd.DataFrame(measurements)

#print(df)
# seprated the values of coordinate into longitude and latitude

df['latitude'] = df['coordinates'].apply(lambda x: x['latitude'])
df['longitude'] = df['coordinates'].apply(lambda x: x['longitude'])

# extracting date from date time

df['datetime'] = pd.to_datetime(df['date'].apply(lambda x: x['utc']))

# Extract date component and store in new column
df['date'] = df['datetime'].dt.date

# Time is extracted
df['time'] = df['datetime'].dt.time

#datetime column
df['Date_Time'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))



#checking the null data
null_data = df[df.isnull().any(axis=1)]

df['Date_Time'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))

# Drop the separate date and time columns
df = df.drop(['date', 'time'], axis=1)

df.drop(['datetime', 'coordinates','city'], axis=1, inplace=True)

print(df)

def insert_data_into_db(df):
    con =sqlite3.connect('airquality.db')
    print("Database Connected !! ")
    cur =con.cursor()
    #cur.execute('DROP TABLE IF EXISTS air_quality_index')
    #print('Table dropped successfully')

    #created the table 
    cur.execute('''CREATE TABLE IF NOT EXISTS air_quality_index
                (Date_Time PRIMARY KEY,
                    parameter TEXT,
                    value FLOAT,
                    unit TEXT,
                    latitude FLOAT,
                    longitude FLOAT,
                    country TEXT,
                    location TEXT)''')
    df.to_sql('air_quality_index', con, if_exists='replace', index=False)
    #data = [(row['Date_Time'], row['parameter'], row['value'], row['unit'], row['latitude'], row['longitude'], row['country'], row['location']) for _, row in df.iterrows()]

    # Convert datetime object to string format
    data = [(str(row['Date_Time']), row['parameter'], row['value'], row['unit'], row['latitude'], row['longitude'], row['country'], row['location']) for _, row in df.iterrows()]

    # Create the SQL query
    query = "INSERT INTO air_quality_index (Date_Time, parameter, value, unit, latitude, longitude, country, location) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING"

    # Execute the query in bulk
    cur.executemany(query, data)

    # Execute the query in bulk
    cur.executemany(query, data)
    # Commit the changes an
    # # Commit the changes and close the connection
    con.commit()
    con.close()

insert_data_into_db(df)