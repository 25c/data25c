from airbrakepy.logging.handlers import AirbrakeHandler
from config import SETTINGS, pg_connect
from datetime import datetime

import click
import isodate
import json
import logging
import psycopg2
import redis
import sys
import uuid

# initialize logger
logging.basicConfig()
logger = logging.getLogger("processor")
if SETTINGS['PYTHON_ENV'] == 'development' or SETTINGS['PYTHON_ENV'] == 'test':
  logger.setLevel(logging.DEBUG)
else:
  logger.setLevel(logging.INFO)
  handler = AirbrakeHandler(SETTINGS['AIRBRAKE_API_KEY'], environment=SETTINGS['PYTHON_ENV'], component_name='processor', node_name='data25c')
  handler.setLevel(logging.ERROR)
  logger.addHandler(handler)
  
# initialize redis connection
redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])

def process_message(message):
  try:
    # parse JSON data
    data = json.loads(message)
  except ValueError:
    logger.warn('Unparseable message=' + message)
    return
    
  # insert
  created_at = isodate.parse_datetime(data['created_at'])
  if created_at.tzinfo is not None:
    created_at = created_at.astimezone(isodate.tzinfo.UTC).replace(tzinfo=None)
  click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], created_at)
    
def process_queue():
  # block and wait for click data, pushing into processing queue
  message = redis_data.brpoplpush('QUEUE', 'QUEUE_PROCESSING', 0)
  # process message
  process_message(message)
  # remove from processing queue
  redis_data.lrem('QUEUE_PROCESSING', 0, message)

if __name__ == '__main__':
  logger.info("Starting processor...")
  while True:
    process_queue()
    