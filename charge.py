from airbrakepy.logging.handlers import AirbrakeHandler
from config import SETTINGS, pg_connect
from datetime import datetime, timedelta
  
import json
import logging
import psycopg2
import redis
import stripe
import sys
import uuid as uuid_mod

# initialize logger
logging.basicConfig()
logger = logging.getLogger("charge")
if SETTINGS['PYTHON_ENV'] == 'development' or SETTINGS['PYTHON_ENV'] == 'test':
  logger.setLevel(logging.DEBUG)
else:
  logger.setLevel(logging.INFO)
  handler = AirbrakeHandler(SETTINGS['AIRBRAKE_API_KEY'], environment=SETTINGS['PYTHON_ENV'], component_name='charge', node_name='data25c')
  handler.setLevel(logging.ERROR)
  logger.addHandler(handler)
  
# set up stripe library
stripe.api_key = SETTINGS['STRIPE_API_KEY']

# connect to data and web postgres databases
pg_data = pg_connect(SETTINGS['DATABASE_URL'])
pg_web = pg_connect(SETTINGS['DATABASE_WEB_URL'])

# connect to web postgres (for enqueueing emails)
redis_web = redis.StrictRedis.from_url(SETTINGS['REDIS_WEB_URL'])

# get ids of users with balance over limit
def get_user_ids():
  cursor_web = pg_web.cursor()
  cursor_web.execute("SELECT id FROM users WHERE balance>%s", (500*1000000,))
  results = cursor_web.fetchall()
  cursor_web.close()
  pg_web.commit()
  return results
  
def send_invoice_email(user_id, payment_id):
  data = { 'class': 'UserMailer', 'args':[ 'new_invoice', user_id, payment_id ] }
  redis_web.rpush('resque:queue:mailer', json.dumps(data))
  
def charge_user(user_id):
  xid_data = str(user_id) + '-' + str(datetime.utcnow()) + '-process-click'
  xid_web = str(user_id) + '-' + str(datetime.utcnow()) + '-process-user'
  data_cursor = None
  try:
    pg_data.tpc_begin(xid_data)
    data_cursor = pg_data.cursor()
    # get all unfunded tips past undo grace period
    data_cursor.execute("SELECT id, parent_click_id, amount FROM clicks WHERE user_id=%s AND state=%s AND created_at<%s FOR UPDATE", (user_id, 1, datetime.utcnow() - timedelta(hours=1)))
    click_ids = []
    total_amount = 0
    # sum tip amounts
    for result in data_cursor:
      click_ids.append(result[0])
      if result[1] is None:
        total_amount += result[2]
    # verify charge threshold (some of user balance may still be in undo grace period)
    if total_amount < 500*1000000:
      raise Exception("total_amount=$%s is below charge threshold" % (total_amount / 100000000.0,))
    web_cursor = None
    try:
      web_cursor = pg_web.cursor()
      # get user info
      web_cursor.execute("SELECT stripe_id FROM users WHERE id=%s", (user_id, ))
      result = web_cursor.fetchone()
      if result is None:
        raise Exception("unexpected error could not retrieve user stripe id")
      if result[0] is None:
        raise Exception("no stored stripe id")
      stripe_id = result[0]
      web_cursor.close()
      pg_web.commit()
              
      # charge stored card
      charge = stripe.Charge.create(
        amount=total_amount/1000000,
        currency='usd',
        customer=stripe_id,
        description="User ID=%s" % (user_id,)
      )
    
      # start tpc in web db
      pg_web.tpc_begin(xid_web)
      web_cursor = pg_web.cursor()
      
      # create payment object record in web25c
      now = datetime.utcnow()
      web_cursor.execute("INSERT INTO payments (uuid, user_id, amount, state, payment_type, transaction_id, updated_at, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (uuid_mod.uuid4().hex, user_id, total_amount, 2, 'payin', charge['id'], now, now))
      result = web_cursor.fetchone()
      payment_id = result[0]
      
      # update user balance
      web_cursor.execute("UPDATE users SET balance=balance-%s, updated_at=%s WHERE id=%s", (total_amount, datetime.utcnow(), user_id))

      # prepare web transaction for commit
      pg_web.tpc_prepare()

      # update click states
      data_cursor.execute("UPDATE clicks SET state=2, payment_id=%s, updated_at=%s WHERE id IN %s", (payment_id, now, tuple(click_ids)))
      
      # prepare data transaction for commit
      pg_data.tpc_prepare()
      # finally, try to commit both
      pg_data.tpc_commit()
      try:
        pg_web.tpc_commit()
        try:
          logger.info("%s:charge completed" % (user_id,))
          # send email invoice
          send_invoice_email(user_id, payment_id)
        except:
          logger.exception("%s:unexpected exception sending email after successful charge" % (user_id,))
      except:
        logger.exception("%s:MANUAL ROLLBACK AND DB CONSISTENCY FIX REQUIRED" % (user_id,))
    except:
      e = sys.exc_info()[1]
      logger.exception("%s:%s" % (user_id, e))
      # close cursors and rollback
      if web_cursor is not None:
        web_cursor.close()
      pg_web.tpc_rollback()      
      data_cursor.close()
      pg_data.tpc_rollback()
  except:
    e = sys.exc_info()[1]
    logger.exception("%s:%s" % (user_id, e))
    # close cursors and rollback
    if data_cursor is not None:
      data_cursor.close()
    pg_data.tpc_rollback()


if __name__ == '__main__':
  logger.info("Starting charge process...")
  user_ids = get_user_ids()
  for user_id in user_ids:
    charge_user(user_id[0])
    
