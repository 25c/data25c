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
  
def database_url(dbname):
  return 'tcp://%(username)s:%(password)s@%(host)s:%(port)s/%(dbname)s' % \
    { 
      'username': environ['ENV_25C_DB_USERNAME'], 
      'password': environ['ENV_25C_DB_PASSWORD'], 
      'host': environ['ENV_25C_DB_HOST'], 
      'port': environ['ENV_25C_DB_PORT'],
      'dbname': dbname
    }

def url_base(port):
  return 'http://localhost:%s' % port

def redis_url():
  return 'redis://%(host)s:%(port)s/' % \
    { 'host': environ['ENV_25C_REDIS_HOST'], 'port': environ.get('ENV_25C_REDIS_PORT', 6379) }

SETTINGS = {
  'development': {
    'URL_BASE_WEB': url_base(environ.get('PORT', 5300)),
    'URL_BASE_TIP': url_base(environ.get('PORT', 5300)),
    'DATABASE_URL': database_url('data25c_development'),
    'DATABASE_WEB_URL': database_url('web25c_development'),
    'REDIS_URL': redis_url(),
    'REDIS_WEB_URL': redis_url(),
    'FACEBOOK_APP_TOKEN': '259751957456159|ZRMlX9RvgCMW5v0SpKIoDg3n8aE',
    'FACEBOOK_NAMESPACE': 'twentyfivec-dev',
    'STRIPE_API_KEY': 'sk_test_9GECsLndO8PHDgQcAnRIIFFL'
  },
  'test': {
    'URL_BASE_WEB': url_base(environ.get('PORT', 5300)),
    'URL_BASE_TIP': url_base(environ.get('PORT', 5300)),
    'DATABASE_URL': database_url('data25c_test'),
    'DATABASE_WEB_URL': database_url('web25c_test'),
    'REDIS_URL': redis_url(),
    'REDIS_WEB_URL': redis_url(),
    'FACEBOOK_APP_TOKEN': '259751957456159|ZRMlX9RvgCMW5v0SpKIoDg3n8aE',
    'FACEBOOK_NAMESPACE': 'twentyfivec-dev',
    'STRIPE_API_KEY': 'sk_test_9GECsLndO8PHDgQcAnRIIFFL'
  },
  'staging': {
    'URL_BASE_WEB': url_base(environ.get('PORT', 5300)),
    'URL_BASE_TIP': url_base(environ.get('PORT', 5300)),
    'DATABASE_URL': database_url('data25c_staging'),
    'DATABASE_WEB_URL': database_url('web25c_staging'),
    'REDIS_URL': redis_url(),
    'REDIS_WEB_URL': redis_url(),
    'FACEBOOK_APP_TOKEN': '259751957456159|ZRMlX9RvgCMW5v0SpKIoDg3n8aE',
    'FACEBOOK_NAMESPACE': 'twentyfivec-dev',
    'STRIPE_API_KEY': 'sk_test_9GECsLndO8PHDgQcAnRIIFFL'
  },
  'production': {
    'URL_BASE_WEB': url_base(environ.get('PORT', 5300)),
    'URL_BASE_TIP': url_base(environ.get('PORT', 5300)),
    'DATABASE_URL': database_url('data25c_production'),
    'DATABASE_WEB_URL': database_url('web25c_production'),
    'REDIS_URL': redis_url(),
    'REDIS_WEB_URL': redis_url(),
    'FACEBOOK_APP_TOKEN': '259751957456159|ZRMlX9RvgCMW5v0SpKIoDg3n8aE',
    'FACEBOOK_NAMESPACE': 'twentyfivec-dev',
    'STRIPE_API_KEY': 'sk_test_9GECsLndO8PHDgQcAnRIIFFL'
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
if 'STRIPE_API_KEY' in environ:
  SETTINGS['STRIPE_API_KEY'] = environ['STRIPE_API_KEY']

SETTINGS['PYTHON_ENV'] = environ['PYTHON_ENV'] if 'PYTHON_ENV' in environ else 'development'
SETTINGS['AIRBRAKE_API_KEY'] = '25f60a0bcd9cc454806be6824028a900'
