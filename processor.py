from airbrakepy.logging.handlers import AirbrakeHandler
from config import SETTINGS, pg_connect
from datetime import datetime

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
  handler = AirbrakeHandler(SETTINGS['AIRBRAKE_API_KEY'], environment=SETTINGS['PYTHON_ENV'], component_name='validator', node_name='data25c')
  handler.setLevel(logging.ERROR)
  logger.addHandler(handler)
  
# initialize redis connection
redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])

# initialize postgres connections
pg_data = pg_connect(SETTINGS['DATABASE_URL'])
pg_web = pg_connect(SETTINGS['DATABASE_WEB_URL'])

def deduct_click(data):
  xid_web = data['uuid'] + '-user'
  xid_data = data['uuid'] + '-click'
  web_cursor = None
  data_cursor = None
  try:
    # start tpc transaction on web
    pg_web.tpc_begin(xid_web)
    web_cursor = pg_web.cursor()    
    # check and update balance
    web_cursor.execute("SELECT id, balance FROM users WHERE LOWER(uuid) = LOWER(%s) FOR UPDATE", (data['user_uuid'],))
    result = web_cursor.fetchone()
    if result is None:
      raise Exception(data['uuid'] + ':invalid user_uuid=' + data['user_uuid'])
    balance = result[1]
    if balance <= -40:
      raise Exception(data['uuid'] + ':overdraft by user_uuid=' + data['user_uuid'])
    balance -= 1
    web_cursor.execute("UPDATE users SET balance=%s WHERE id=%s", (balance, result[0]))
    web_cursor.close()
    web_cursor = None
    # prepare tpc transaction on web
    pg_web.tpc_prepare()
    try:
      # check and update click state
      pg_data.tpc_begin(xid_data)
      data_cursor = pg_data.cursor()    
      data_cursor.execute("SELECT id, state FROM clicks WHERE LOWER(uuid) = LOWER(%s) FOR UPDATE", (data['uuid'],))
      result = data_cursor.fetchone()
      if result is None:
        raise Exception(data['uuid'] + ':invalid click')
      state = result[1]
      if state != 0:
        raise Exception(data['uuid'] + ':click already state=' + str(state))
      state = 1
      data_cursor.execute("UPDATE clicks SET state=%s WHERE id=%s", (state, result[0]))
      data_cursor.close()
      data_cursor = None
      # prepare tpc transaction
      pg_data.tpc_prepare()
      # finally, try to commit both
      pg_data.tpc_commit()
      pg_web.tpc_commit()
      logger.info(data['uuid'] + ':click processed, balance=' + str(balance) + ' for user_uuid=' + data['user_uuid'])
    except:
      e = sys.exc_info()[1]
      logger.warning(str(e))
      # close cursors and rollback
      if data_cursor is not None:
        data_cursor.close()
      pg_data.tpc_rollback()
      pg_web.tpc_rollback()
  except:
    e = sys.exc_info()[1]
    logger.warning(str(e))
    # close cursors and rollback
    if web_cursor is not None:
      web_cursor.close()
    pg_web.tpc_rollback()
            
def process_message(message):
  try:
    # parse JSON data
    data = json.loads(message)
  except ValueError:
    logger.warn('Unparseable message=' + message)
    return
    
  # deduct from user balance
  deduct_click(data)
    
def process_queue():
  # block and wait for click data, pushing into processing queue
  message = redis_data.brpoplpush('QUEUE_DEDUCT', 'QUEUE_DEDUCT_PROCESSING', 0)
  # process message
  process_message(message)
  # remove from processing queue
  redis_data.lrem('QUEUE_DEDUCT_PROCESSING', 0, message)

if __name__ == '__main__':
  logger.info("Starting processor...")
  while True:
    process_queue()
    