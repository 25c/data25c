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
    'URL_BASE_WEB': 'http://tunnel.plus25c.com',
    'URL_BASE_TIP': 'http://tunnel.plus25c.com',
    'DATABASE_URL': 'tcp://superuser@localhost/data25c_development',
    'DATABASE_WEB_URL': 'tcp://superuser@localhost/web25c_development',
    'REDIS_URL': 'redis://localhost:6379/',
    'REDIS_WEB_URL': 'redis://localhost:6379/',
    'FACEBOOK_APP_TOKEN': '259751957456159|ZRMlX9RvgCMW5v0SpKIoDg3n8aE',
    'FACEBOOK_NAMESPACE': 'twentyfivec-dev'
  },
  'test': {
    'URL_BASE_WEB': 'http://tunnel.plus25c.com',
    'URL_BASE_TIP': 'http://tunnel.plus25c.com',
    'DATABASE_URL': 'tcp://superuser@localhost/data25c_test',
    'DATABASE_WEB_URL': 'tcp://superuser@localhost/web25c_test',
    'REDIS_URL': 'redis://localhost:6379/',
    'REDIS_WEB_URL': 'redis://localhost:6379/',
    'FACEBOOK_APP_TOKEN': '259751957456159|ZRMlX9RvgCMW5v0SpKIoDg3n8aE',
    'FACEBOOK_NAMESPACE': 'twentyfivec-dev'
  },
  'staging': {
    # most set from heroku config environment variables below
    'URL_BASE_WEB': 'https://www.plus25c.com',
    'URL_BASE_TIP': 'https://tip.plus25c.com',
    'FACEBOOK_NAMESPACE': 'twentyfivec-staging'
  },
  'production': {
    # most set from heroku config environment variables below
    'URL_BASE_WEB': 'https://www.25c.com',
    'URL_BASE_TIP': 'https://tip.25c.com',
    'FACEBOOK_NAMESPACE': 'twentyfivec'
  }
}[environ['PYTHON_ENV'] if 'PYTHON_ENV' in environ else 'development']

if 'DATABASE_URL' in environ:
  SETTINGS['DATABASE_URL'] = environ['DATABASE_URL']
if 'DATABASE_WEB_URL' in environ:
  SETTINGS['DATABASE_WEB_URL'] = environ['DATABASE_WEB_URL']
if 'REDISTOGO_URL' in environ:
  SETTINGS['REDIS_URL'] = environ['REDISTOGO_URL']
if 'REDISTOGO_WEB_URL' in environ:
  SETTINGS['REDIS_WEB_URL'] = environ['REDISTOGO_WEB_URL']
if 'FACEBOOK_APP_TOKEN' in environ:
  SETTINGS['FACEBOOK_APP_TOKEN'] = environ['FACEBOOK_APP_TOKEN']

SETTINGS['PYTHON_ENV'] = environ['PYTHON_ENV'] if 'PYTHON_ENV' in environ else 'development'
SETTINGS['AIRBRAKE_API_KEY'] = '25f60a0bcd9cc454806be6824028a900'
