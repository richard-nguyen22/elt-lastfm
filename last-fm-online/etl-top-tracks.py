import requests
import time

import requests_cache
import datetime
import json
import pandas as pd

from sqlalchemy import create_engine
from sql_queries import create_table

# Information to access PostgresSQL database
user = 'api1'
passwd = 'Open API@1'
host = 'localhost'
port = 5432
db = 'openapi'
schema = 'lastfm'
table_name = 'top_tracks'

# Required information to exact from LastFM API
payload = {
  'api_key': 'your_lastfm_api_key',
  'format': 'json',
  'method': 'chart.gettoptracks',
  'limit': 500
}

def main():
  begin = time.time()

  # Setup connection to PostgreSQL server
  url_connection = 'postgresql+psycopg2://{user}:{passwd}@{host}:{port}/{db}'\
    .format(user=user, passwd=passwd, host=host, port=port, db=db)
  
  try:
    engine = create_engine(url_connection)
    db_connection = engine.connect()
    print("Connected to {} in PostgreSQL database!".format(db))
  except IOError:
    print("Failed to get database connection!")
  
  # Check table_name if exists
  if not engine.has_table(table_name, schema=schema):
    create_table(engine, schema, table_name)
    print('New table is created: {}.{}'.format(schema, table_name))

  # Create cache for new API requests
  requests_cache.install_cache('lastfm_api')

  page = 1
  # Initialize any value of total_pages
  total_pages = 100
  total_rows = 0
  extracted_tracks = []

  while page <= total_pages:
    payload['page'] = page

    # make the API call & extract data
    response = extract(payload)
    # if getting an error, print the response and halt the loop
    if response.status_code != 200:
      print(response.text)
      break

    # transform
    top_tracks = transform(response, extracted_tracks)

    # load
    rows = load(db_connection, schema, table_name, top_tracks)
    if rows == 0:
      # No more data
      print("No more data")
      break
    total_rows += rows

    # extract pagination info from response
    total_pages = int(response.json()['tracks']['@attr']['totalPages'])
    # print current page status
    print("Top tracks: Requested page {}/{}".format(page, total_pages))

    # if it's not a cached result, sleep
    if not getattr(response, 'from_cache', False):
      time.sleep(0.35)

    # increment the page number
    page += 1

  # Close connection to PostgreSQL
  db_connection.close()
  requests_cache.clear()
  end = time.time()

  print("## FINISH ETL {} from LastFM API".format(table_name))
  print("## {} rows were added".format(total_rows))
  print("## Total runtime: {} minutes".format(round((end - begin)/60, 2)))


def extract(payload):
  # define headers and URL
  headers = {'user-agent': 'richard'}
  url = 'http://ws.audioscrobbler.com/2.0/'

  # Add API key and format to the payload
  response = requests.get(url, headers=headers, params=payload)
  return response

def transform(response, extracted_tracks):
  top_tracks = []
  for track in response.json()['tracks']['track']:
    # name_id = track['name'] + '##' + track['mbid']
    if track['url'] not in extracted_tracks:
      top_tracks.append({
        'name': track['name'],
        'duration': int(track['duration']),
        'playcount': int(track['playcount']),
        'listeners': int(track['listeners']),
        'mbid':track['mbid'],
        'extract_date':datetime.date.today(),
        'url':track['url']
      })
      extracted_tracks.append(track['url'])
  return top_tracks

def load(db_connection, schema, table_name, data):
  """
  load() uses db_connection to append data into table_name table in 
  PostgresSQL server
  """
  # Convert list into data frame
  data = pd.DataFrame.from_dict(data, orient='columns')
  data.drop_duplicates()
  try:
    data.to_sql(table_name, db_connection, schema=schema, 
      if_exists='append', index=False)
  except ValueError as vx:
    print(vx)
  except Exception as ex:
    print(ex)
  print("{} rows are added into {} table".format(data.shape[0], table_name))
  return data.shape[0]

if __name__ == '__main__':
  main()
