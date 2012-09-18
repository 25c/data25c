from airbrakepy.logging.handlers import AirbrakeHandler
from config import SETTINGS, pg_connect
from datetime import datetime

import isodate
import json
import logging
import psycopg2
import redis
import uuid

# initialize logger
logging.basicConfig()
logger = logging.getLogger("validator")
if SETTINGS['PYTHON_ENV'] == 'development' or SETTINGS['PYTHON_ENV'] == 'test':
  logger.setLevel(logging.DEBUG)
else:
  logger.setLevel(logging.INFO)
  handler = AirbrakeHandler(SETTINGS['AIRBRAKE_API_KEY'], environment=SETTINGS['PYTHON_ENV'], component_name='validator', node_name='data25c')
  handler.setLevel(logging.ERROR)
  logger.addHandler(handler)
  
# initialize redis connection
redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])

# initialize postgres connections, turn on autocommit
pg_data = pg_connect(SETTINGS['DATABASE_URL'])
pg_data.autocommit = True
pg_web = pg_connect(SETTINGS['DATABASE_WEB_URL'])
pg_web.autocommit = True

def validate_click(data):
  cursor = None
  try:
    cursor = pg_web.cursor()
        
    # validate user uuid
    cursor.execute("SELECT id FROM users WHERE LOWER(uuid) = LOWER(%s)", (data['user_uuid'],))
    result = cursor.fetchone()
    if result is None:
      logger.warn(data['uuid'] + ':invalid user_uuid=' + data['user_uuid'])
      return
    user_id = result[0]
    
    # validate button uuid
    cursor.execute("SELECT id, user_id FROM buttons WHERE LOWER(uuid) = LOWER(%s)", (data['button_uuid'],))
    result = cursor.fetchone()
    if result is None:
      logger.warn(data['uuid'] + ':invalid button_uuid=' + data['button_uuid'])
      return          
    # drop click if user is clicking on own button!
    if user_id == result[1]:
      logger.warn(data['uuid'] + ':invalid user_uuid=' + data['user_uuid'] + ' is owner of button_uuid=' + data['button_uuid'])
      return
    button_id = result[0]
  
    # validate referrer user uuid, if present
    referrer_user_id = None
    if 'referrer_user_uuid' in data and data['referrer_user_uuid'] is not None:
      cursor.execute("SELECT id FROM users WHERE LOWER(uuid) = LOWER(%s)", (data['referrer_user_uuid'],))
      result = cursor.fetchone()
      if result is not None:
        referrer_user_id = result[0]
      else:
        logger.warn(data['uuid'] + ':invalid referrer_user_uuid=' + data['referrer_user_uuid'])
        
    # return user/button/referrer ids
    return (user_id, button_id, referrer_user_id)
  finally:
    if cursor is not None:
      cursor.close()
  
def insert_click(data):
  ids = validate_click(data)
  if ids is not None:
    cursor = None
    try:
      cursor = pg_data.cursor()
      created_at = isodate.parse_datetime(data['created_at'])
      updated_at = datetime.now()    
      cursor.execute("INSERT INTO clicks (uuid, user_id, button_id, referrer_user_id, ip_address, user_agent, referrer, state, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (data['uuid'], ids[0], ids[1], ids[2], data['ip_address'], data['user_agent'], data['referrer'], 0, created_at, updated_at))
      logger.info(data['uuid'] + ':inserted')
    except psycopg2.IntegrityError:
      # this should only be happening on duplicate uuid, drop
      logger.warn(data['uuid'] + ':dropped as duplicate')
    finally:
      if cursor is not None:
        cursor.close()
      
def process_message(message):
  try:
    # parse JSON data
    data = json.loads(message)
  except ValueError:
    logger.warn('Unparseable message=' + message)
    return
    
  # validate and insert click data
  insert_click(data)
    
def process_queue():
  # block and wait for click data, pushing into processing queue
  message = redis_data.brpoplpush('QUEUE', 'QUEUE_PROCESSING', 0)
  # process message
  process_message(message)
  # remove from processing queue, add to deduct queue in transaction
  pipe = redis_data.pipeline()
  pipe.lrem('QUEUE_PROCESSING', 0, message)
  pipe.lpush('QUEUE_DEDUCT', message)
  pipe.execute()

if __name__ == '__main__':
  logger.info("Starting validator...")
  while True:
    process_queue()