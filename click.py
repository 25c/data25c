from airbrakepy.logging.handlers import AirbrakeHandler
from config import SETTINGS, pg_connect
from datetime import datetime

import logging
import psycopg2
import redis
import sys

# initialize logger
logging.basicConfig()
logger = logging.getLogger('click')
if SETTINGS['PYTHON_ENV'] == 'development' or SETTINGS['PYTHON_ENV'] == 'test':
  logger.setLevel(logging.DEBUG)
else:
  logger.setLevel(logging.INFO)
  handler = AirbrakeHandler(SETTINGS['AIRBRAKE_API_KEY'], environment=SETTINGS['PYTHON_ENV'], component_name='validator', node_name='data25c')
  handler.setLevel(logging.ERROR)
  logger.addHandler(handler)

# initialize postgres connections
pg_data = pg_connect(SETTINGS['DATABASE_URL'])
pg_web = pg_connect(SETTINGS['DATABASE_WEB_URL'])
  
# initialize redis connection
redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])
  
def validate_click(uuid, user_uuid, button_uuid, referrer_user_uuid):
  cursor = None
  try:
    pg_web.autocommit = True
    cursor = pg_web.cursor()
            
    # validate user uuid
    cursor.execute("SELECT id FROM users WHERE LOWER(uuid) = LOWER(%s)", (user_uuid,))
    result = cursor.fetchone()
    if result is None:
      logger.warn(uuid + ':invalid user_uuid=' + user_uuid)
      return
    user_id = result[0]
    
    # validate button uuid
    cursor.execute("SELECT id, user_id FROM buttons WHERE LOWER(uuid) = LOWER(%s)", (button_uuid,))
    result = cursor.fetchone()
    if result is None:
      logger.warn(uuid + ':invalid button_uuid=' + button_uuid)
      return          
    # drop click if user is clicking on own button!
    if user_id == result[1]:
      logger.warn(uuid + ':invalid user_uuid=' + user_uuid + ' is owner of button_uuid=' + button_uuid)
      return
    button_id = result[0]
  
    # validate referrer user uuid, if present
    referrer_user_id = None
    if referrer_user_uuid is not None:
      cursor.execute("SELECT id FROM users WHERE LOWER(uuid) = LOWER(%s)", (referrer_user_uuid,))
      result = cursor.fetchone()
      if result is not None:
        referrer_user_id = result[0]
      else:
        logger.warn(uuid + ':invalid referrer_user_uuid=' + referrer_user_uuid)
        
    # return user/button/referrer ids
    return (user_id, button_id, referrer_user_id)
  finally:
    if cursor is not None:
      cursor.close()
    pg_web.autocommit = False
  
def update_click(uuid, amount, created_at):
  xid_data = uuid + '-' + str(created_at) + '-update-click'
  xid_web = uuid + '-' + str(created_at) + '-update-user'
  data_cursor = None
  web_cursor = None
  try:
    # start tpc transaction on data
    pg_data.tpc_begin(xid_data)
    data_cursor = pg_data.cursor() 
    # get previous value
    data_cursor.execute("SELECT id, state, user_id, amount FROM clicks WHERE LOWER(uuid) = LOWER(%s) AND created_at<=%s FOR UPDATE", (uuid, created_at))
    result = data_cursor.fetchone()
    if result is None:
      raise Exception(uuid + ':click not found')
    state = result[1]
    if state != 1:
      raise Exception(uuid + ':click already state=' + str(state))
    user_id = result[2]
    old_amount = result[3]
    # update click
    data_cursor.execute("UPDATE clicks SET amount=%s, updated_at=%s WHERE id=%s", (amount, datetime.now(), result[0]))
    logger.info(uuid + ':updated')
    data_cursor.close()
    data_cursor = None
    # prepare tpc transaction
    pg_data.tpc_prepare()
    try:
      # update user balance
      pg_web.tpc_begin(xid_web)
      web_cursor = pg_web.cursor()    
      # check and update balance
      web_cursor.execute("SELECT uuid, balance FROM users WHERE id=%s FOR UPDATE", (user_id,))
      result = web_cursor.fetchone()
      if result is None:
        raise Exception(uuid + ':invalid user_id=' + user_id)
      user_uuid = result[0]
      balance = result[1] - old_amount + amount
      web_cursor.execute("UPDATE users SET balance=%s WHERE id=%s", (balance, user_id))
      web_cursor.close()
      web_cursor = None
      # prepare tpc transaction on web
      pg_web.tpc_prepare()
      # finally, try to commit both
      pg_web.tpc_commit()
      try:
        pg_data.tpc_commit()
        try:
          logger.info(uuid + ':click updated, balance=' + str(balance) + ' for user_uuid=' + user_uuid)
          # update redis balance cache for user
          redis_data.set('user:' + user_uuid, balance)
        except:
          logger.exception(uuid + ':unexpected exception after successful commits, redis balance cache out of sync?')
      except:
        logger.exception(uuid + ':MANUAL ROLLBACK AND DB CONSISTENCY FIX REQUIRED')
    except:
      e = sys.exc_info()[1]
      logger.exception(uuid + ':' + str(e))
      # close cursors and rollback
      if web_cursor is not None:
        web_cursor.close()
      pg_web.tpc_rollback()
      pg_data.tpc_rollback()    
  except:
    e = sys.exc_info()[1]
    logger.exception(uuid + ':' + str(e))
    # close cursors and rollback
    if data_cursor is not None:
      data_cursor.close()
    pg_data.tpc_rollback()
    
def insert_click(uuid, user_id, button_id, amount, referrer_user_id, ip_address, user_agent, referrer, created_at):
  xid_data = uuid + '-' + str(created_at) + '-insert-click'
  xid_web = uuid + '-' + str(created_at) + '-insert-user'
  data_cursor = None
  web_cursor = None
  try:
    # start tpc transaction on data
    pg_data.tpc_begin(xid_data)
    data_cursor = pg_data.cursor() 
    # attempt insert 
    data_cursor.execute("INSERT INTO clicks (uuid, user_id, button_id, amount, referrer_user_id, ip_address, user_agent, referrer, state, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (uuid, user_id, button_id, amount, referrer_user_id, ip_address, user_agent, referrer, 1, created_at, datetime.now()))
    logger.info(uuid + ':inserted')
    data_cursor.close()
    data_cursor = None
    # prepare tpc transaction
    pg_data.tpc_prepare()
    try:
      # update user balance
      pg_web.tpc_begin(xid_web)
      web_cursor = pg_web.cursor()    
      # check and update balance
      web_cursor.execute("SELECT uuid, balance FROM users WHERE id=%s FOR UPDATE", (user_id,))
      result = web_cursor.fetchone()
      if result is None:
        raise Exception(uuid + ':invalid user_id=' + user_id)
      user_uuid = result[0]
      balance = result[1] + amount
      web_cursor.execute("UPDATE users SET balance=%s WHERE id=%s", (balance, user_id))
      web_cursor.close()
      web_cursor = None
      # prepare tpc transaction on web
      pg_web.tpc_prepare()
      # finally, try to commit both
      pg_web.tpc_commit()
      try:
        pg_data.tpc_commit()
        try:
          logger.info(uuid + ':click inserted, balance=' + str(balance) + ' for user_uuid=' + user_uuid)
          # update redis balance cache for user
          redis_data.set('user:' + user_uuid, balance)
        except:
          logger.exception(uuid + ':unexpected exception after successful commits, redis balance cache out of sync?')
      except:
        logger.exception(uuid + ':MANUAL ROLLBACK AND DB CONSISTENCY FIX REQUIRED')
    except:
      e = sys.exc_info()[1]
      logger.exception(uuid + ':' + str(e))
      # close cursors and rollback
      if web_cursor is not None:
        web_cursor.close()
      pg_web.tpc_rollback()
      pg_data.tpc_rollback()    
  except psycopg2.IntegrityError:
    # this should only be happening on duplicate uuid, update
    logger.warn(uuid + ':exists, will update')
    if data_cursor is not None:
      data_cursor.close()
    pg_data.tpc_rollback()
    update_click(uuid, amount, created_at)
  except:
    e = sys.exc_info()[1]
    logger.exception(uuid + ':' + str(e))
    # close cursors and rollback
    if data_cursor is not None:
      data_cursor.close()
    pg_data.tpc_rollback()
