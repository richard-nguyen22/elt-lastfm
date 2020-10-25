"""
  All sql queries to create tables in lastfm schema
"""
from sqlalchemy import MetaData, Table, Column, Integer, String, Date

def create_table(sql_engine, schema, table_name):
  meta = MetaData(schema=schema)

  if table_name == 'top_artists':
    # top_artists table
    Table('top_artists', meta,
      Column('name', String),
      Column('playcount', Integer),
      Column('listeners', Integer),
      Column('mbid', String),
      Column('extract_date', Date),
      Column('url', String)
    )
  elif table_name == 'top_tracks':
    # top_artists table
    Table('top_tracks', meta,
      Column('name', String),
      Column('duration', Integer),
      Column('playcount', Integer),
      Column('listeners', Integer),
      Column('mbid', String),
      Column('extract_date', Date),
      Column('url', String)
    )

  # Use meta.create() to create all tables
  meta.create_all(sql_engine)
