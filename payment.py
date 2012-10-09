from airbrakepy.logging.handlers import AirbrakeHandler
from config import SETTINGS, pg_connect
from datetime import datetime

import logging
import psycopg2
import redis
import sys

# initialize logger
logging.basicConfig()
logger = logging.getLogger('payment')
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
    
def process_payment(uuid):
  xid_web = uuid + '-' + str(datetime.utcnow()) + '-process-user'
  xid_data = uuid + '-' + str(datetime.utcnow()) + '-process-click'
  web_cursor = None
  data_cursor = None
  try:
    # start tpc transaction on web
    pg_web.tpc_begin(xid_web)
    web_cursor = pg_web.cursor()
    # get payment info
    web_cursor.execute("SELECT id, state, user_id, amount FROM payments WHERE LOWER(uuid) = LOWER(%s) FOR UPDATE", (uuid,))
    result = web_cursor.fetchone()
    if result is None:
      raise Exception(uuid + ':invalid payment uuid')
    payment_id = result[0]
    state = result[1]
    user_id = result[2]
    amount = result[3]
    if state != 0:
      raise Exception(uuid + ':payment already state=' + str(state))
    # get user balance, verify match with amount
    web_cursor.execute("SELECT uuid, balance FROM users WHERE id=%s FOR UPDATE", (user_id,))
    result = web_cursor.fetchone()
    if result is None:
      raise Exception(uuid + ':invalid user_id=' + str(user_id))
    user_uuid = result[0]
    balance = result[1]
    if amount != balance:
      raise Exception(uuid + ':payment amount=' + str(amount) + ' is not equal to user balance=' + str(balance))
    # update payment state
    web_cursor.execute("UPDATE payments SET state=2 WHERE id=%s", (payment_id,))
    # update user balance
    web_cursor.execute("UPDATE users SET balance=0 WHERE id=%s", (user_id,))
    web_cursor.close()
    web_cursor = None
    # prepare tpc transaction on web
    pg_web.tpc_prepare()
    try:
      # start tpc transaction on data
      pg_data.tpc_begin(xid_data)
      data_cursor = pg_data.cursor()
      # get all current deducted clicks
      click_ids = []
      total_amount = 0
      data_cursor.execute("SELECT id, amount FROM clicks WHERE state=%s AND user_id=%s FOR UPDATE", (1, user_id))
      for result in data_cursor:
        click_ids.append(result[0])
        total_amount += result[1]
      # verify amount matches
      if total_amount != amount:
        raise Exception(uuid + ':payment/user balance amount=' + str(amount) + ' is not equal to deducted click total amount=' + str(total_amount))
      # update state of all clicks
      data_cursor.execute("UPDATE clicks SET state=2, funded_at=%s WHERE id IN %s", (datetime.utcnow(), tuple(click_ids)))
      data_cursor.close()
      data_cursor = None
      # prepare tpc transaction
      pg_data.tpc_prepare()
      # finally, try to commit both
      pg_data.tpc_commit()
      try:
        pg_web.tpc_commit()
        try:
          logger.info(uuid + ':payment processed')
          # update redis balance cache for user
          redis_data.set('user:' + user_uuid, 0)
        except:
          logger.exception(uuid + ':unexpected exception after successful commits, redis balance cache out of sync?')
      except:
        logger.exception(uuid + ':MANUAL ROLLBACK AND DB CONSISTENCY FIX REQUIRED')
    except:
      e = sys.exc_info()[1]
      logger.exception(uuid + ':' + str(e))
      # close cursors and rollback
      if data_cursor is not None:
        data_cursor.close()
      pg_data.tpc_rollback()
      pg_web.tpc_rollback()
  except:
    e = sys.exc_info()[1]
    logger.exception(uuid + ':' + str(e))
    # close cursors and rollback
    if web_cursor is not None:
      web_cursor.close()
    pg_web.tpc_rollback()
