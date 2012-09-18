from os import environ
from urlparse import urlparse

import psycopg2

# helper method for connecting to postgres library using urls
def pg_connect(url):
  url = urlparse(url)
  dsn = "dbname={} host={}".format(url.path[1:], url.hostname)
  if url.port is not None: dsn += " port={}".format(url.port)
  if url.username is not None: dsn += " user={}".format(url.username)
  if url.password is not None: dsn += " password={}".format(url.password)
  connection = psycopg2.connect(dsn)
  return connection
  
SETTINGS = {
  'development': {
    'DATABASE_URL': 'tcp://superuser@localhost/data25c_development',
    'DATABASE_WEB_URL': 'tcp://superuser@localhost/web25c_development',
    'REDIS_URL': 'redis://localhost:6379/'
  },
  'test': {
    'DATABASE_URL': 'tcp://superuser@localhost/data25c_test',
    'DATABASE_WEB_URL': 'tcp://superuser@localhost/web25c_test',
    'REDIS_URL': 'redis://localhost:6379/'
  },
  'staging': {
    'DATABASE_URL': environ['DATABASE_URL'] if 'DATABASE_URL' in environ else '',
    'DATABASE_WEB_URL': environ['DATABASE_DATA_URL'] if 'DATABASE_DATA_URL' in environ else '',
    'REDIS_URL': environ['REDISTOGO_URL'] if 'REDISTOGO_URL' in environ else ''
  },
  'production': {
    'DATABASE_URL': environ['DATABASE_URL'] if 'DATABASE_URL' in environ else '',
    'DATABASE_WEB_URL': environ['DATABASE_DATA_URL'] if 'DATABASE_DATA_URL' in environ else '',
    'REDIS_URL': environ['REDISTOGO_URL'] if 'REDISTOGO_URL' in environ else ''
  }
}[environ['PYTHON_ENV'] if 'PYTHON_ENV' in environ else 'development']

SETTINGS['PYTHON_ENV'] = environ['PYTHON_ENV'] if 'PYTHON_ENV' in environ else 'development'
SETTINGS['AIRBRAKE_API_KEY'] = '25f60a0bcd9cc454806be6824028a900'
