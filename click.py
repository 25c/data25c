from airbrakepy.logging.handlers import AirbrakeHandler
from config import SETTINGS, pg_connect

import logging
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

def update_click_state_and_user_balance(uuid, old_state, new_state, balance_delta, desc):
  xid_web = uuid + '-' + desc + '-user'
  xid_data = uuid + '-' + desc + '-click'
  web_cursor = None
  data_cursor = None
  try:
    # start tpc transaction on data
    pg_data.tpc_begin(xid_data)
    data_cursor = pg_data.cursor() 
    # fetch click and check state   
    data_cursor.execute("SELECT id, state, user_id FROM clicks WHERE LOWER(uuid) = LOWER(%s) FOR UPDATE", (uuid,))
    result = data_cursor.fetchone()
    if result is None:
      raise Exception(uuid + ':invalid click')
    state = result[1]
    if state != old_state:
      raise Exception(uuid + ':click already state=' + str(state))
    user_id = result[2]
    # set to fund state
    state = new_state
    data_cursor.execute("UPDATE clicks SET state=%s WHERE id=%s", (state, result[0]))
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
      balance = result[1] + balance_delta
      web_cursor.execute("UPDATE users SET balance=%s WHERE id=%s", (balance, user_id,))
      web_cursor.close()
      web_cursor = None
      # prepare tpc transaction on web
      pg_web.tpc_prepare()
      # finally, try to commit both
      pg_web.tpc_commit()
      try:
        pg_data.tpc_commit()
        try:
          logger.info(uuid + ':click ' + desc + ', balance=' + str(balance) + ' for user_uuid=' + user_uuid)
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

def fund_click(uuid):
  update_click_state_and_user_balance(uuid, 1, 2, 1, 'fund')

def undo_click(uuid):
  update_click_state_and_user_balance(uuid, 1, 5, 1, 'undo')
