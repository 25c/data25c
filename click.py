from airbrakepy.logging.handlers import AirbrakeHandler
from config import SETTINGS, pg_connect
from datetime import datetime

import facebook
import json
import logging
import psycopg2
import redis
import sys
import uuid as uuid_mod

# initialize logger
logging.basicConfig()
logger = logging.getLogger('click')
if SETTINGS['PYTHON_ENV'] == 'development' or SETTINGS['PYTHON_ENV'] == 'test':
  logger.setLevel(logging.DEBUG)
else:
  logger.setLevel(logging.INFO)
  handler = AirbrakeHandler(SETTINGS['AIRBRAKE_API_KEY'], environment=SETTINGS['PYTHON_ENV'], component_name='click', node_name='data25c')
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
    cursor.execute("SELECT id, facebook_uid FROM users WHERE LOWER(uuid) = LOWER(%s)", (user_uuid,))
    result = cursor.fetchone()
    if result is None:
      logger.warn(uuid + ':invalid user_uuid=' + user_uuid)
      return
    user_id = result[0]
    facebook_uid = result[1]
    
    # validate button uuid
    cursor.execute("SELECT buttons.id, user_id, nickname, share_users FROM buttons JOIN users ON users.id=buttons.user_id WHERE LOWER(buttons.uuid) = LOWER(%s)", (button_uuid,))
    result = cursor.fetchone()
    if result is None:
      logger.warn(uuid + ':invalid button_uuid=' + button_uuid)
      return    
    # drop click if user is clicking on own button!
    button_id = result[0]
    button_user_id = result[1]
    button_user_nickname = result[2]
    share_users = result[3]
    if user_id == button_user_id:
      logger.warn(uuid + ':invalid user_uuid=' + user_uuid + ' is owner of button_uuid=' + button_uuid)
      return
  
    # validate referrer user uuid, if present
    referrer_user_id = None
    if referrer_user_uuid is not None:
      cursor.execute("SELECT id FROM users WHERE LOWER(uuid) = LOWER(%s)", (referrer_user_uuid,))
      result = cursor.fetchone()
      if result is not None:
        referrer_user_id = result[0]
      else:
        logger.warn(uuid + ':invalid referrer_user_uuid=' + referrer_user_uuid)
        
    # see if a rev share has been set up
    share = None
    if share_users is not None and share_users != '':
      try:
        share = json.loads(share_users)
      except:
        logger.exception(uuid + ':invalid button revenue share')
        pass
        
    # return user/button/referrer ids
    return (user_id, facebook_uid, button_id, referrer_user_id, button_user_id, button_user_nickname, share)
  finally:
    if cursor is not None:
      cursor.close()
    pg_web.autocommit = False
  
def update_click(uuid, user_id, facebook_uid, button_id, button_user_id, button_user_nickname, amount, created_at):
  xid_data = uuid + '-' + str(created_at) + '-update-click'
  xid_web = uuid + '-' + str(created_at) + '-update-user'
  data_cursor = None
  web_cursor = None
  try:
    # start tpc transaction on data
    pg_data.tpc_begin(xid_data)
    data_cursor = pg_data.cursor() 
    # get previous value
    data_cursor.execute("SELECT id, state, amount, share_users, fb_action_id FROM clicks WHERE LOWER(uuid) = LOWER(%s) AND created_at<=%s FOR UPDATE", (uuid, created_at))
    result = data_cursor.fetchone()
    if result is None:
      raise Exception(uuid + ':click not found')
    click_id = result[0]
    state = result[1]
    if state != 1 and state != 5:
      raise Exception(uuid + ':click already state=' + str(state))
    old_amount = result[2]
    if amount > 0:
      state = 1
    else:
      state = 5
    share_users = result[3]
    fb_action_id = result[4]
    
    # check if we need to delete/re-publish facebook action
    if amount > 0 and old_amount == 0 and facebook_uid is not None:
      # republish facebook action
      fb_action_id = publish_facebook_action_pledge(uuid, facebook_uid, button_user_nickname)
    elif amount == 0 and old_amount > 0 and fb_action_id is not None:
      # delete facebook action
      if delete_facebook_action(uuid, fb_action_id):
        fb_action_id = None
      
    # update click
    data_cursor.execute("UPDATE clicks SET state=%s, amount=%s, fb_action_id=%s, updated_at=%s WHERE id=%s", (state, amount, fb_action_id, datetime.utcnow(), click_id))
    if share_users is not None:
      # iterate over and update share amount
      try:
        share_users = json.loads(share_users)
        remainder = 100
        for share in share_users:
          data_cursor.execute("UPDATE clicks SET state=%s, amount=%s, updated_at=%s WHERE parent_click_id=%s AND receiver_user_id=%s", (state, amount*share['share_amount']/100, datetime.utcnow(), click_id, share['user']))
          remainder -= share['share_amount']
        data_cursor.execute("UPDATE clicks SET state=%s, amount=%s, updated_at=%s WHERE parent_click_id=%s AND receiver_user_id=%s", (state, amount*remainder/100, datetime.utcnow(), click_id, button_user_id))
      except ValueError:
        logger.exception(uuid + ': could not parse revenue share definition')
        
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
    
def undo_click(uuid):  
  user_id = None
  button_id = None
  try:
    pg_data.autocommit = True
    cursor = pg_data.cursor()
    # get user id and button id from click
    cursor.execute("SELECT user_id, button_id FROM clicks WHERE LOWER(uuid) = LOWER(%s)", (uuid,))
    result = cursor.fetchone()
    if result is None:
      logger.warn(uuid + ':invalid, not found')
      return
    user_id = result[0]
    button_id = result[1]
  finally:
    cursor.close()
    pg_data.autocommit = False
    
  if user_id is not None and button_id is not None:
    try:
      pg_web.autocommit = True
      cursor = pg_web.cursor()
      # get button user id and nickname
      cursor.execute("SELECT user_id, nickname FROM buttons JOIN users ON users.id=buttons.user_id WHERE buttons.id=%s", (button_id,))
      result = cursor.fetchone()
      if result is None:
        raise Exception(uuid + ': could not look up button user id and nickname')
      button_user_id = result[0]
      button_user_nickname = result[1]      
      # get user facebook uid, if any
      cursor.execute("SELECT facebook_uid FROM users WHERE id=%s", (user_id,))
      result = cursor.fetchone()
      if result is None:
        raise Exception(uuid + ': could not look up user facebook uid')
      facebook_uid = result[0]
    finally:
      cursor.close()
      pg_web.autocommit = False
    if button_user_id is not None and button_user_nickname is not None:
      update_click(uuid, user_id, facebook_uid, button_id, button_user_id, button_user_nickname, 0, datetime.utcnow())
  
def insert_click(uuid, user_uuid, button_uuid, referrer_user_uuid, amount, ip_address, user_agent, referrer, created_at):
  ids = validate_click(uuid, user_uuid, button_uuid, referrer_user_uuid)
  if ids is None:
    return
  user_id = ids[0]
  facebook_uid = ids[1]
  button_id = ids[2]
  referrer_user_id = ids[3]
  button_user_id = ids[4]
  button_user_nickname = ids[5]
  share_users = ids[6]
  
  xid_data = uuid + '-' + str(created_at) + '-insert-click'
  xid_web = uuid + '-' + str(created_at) + '-insert-user'
  data_cursor = None
  web_cursor = None
  try:
    # start tpc transaction on data
    pg_data.tpc_begin(xid_data)
    data_cursor = pg_data.cursor() 
    # attempt insert 
    if share_users is None:
      # no share, so just insert this click with the button owner as the receiver of the full amount
      data_cursor.execute("INSERT INTO clicks (uuid, user_id, button_id, receiver_user_id, amount, referrer_user_id, ip_address, user_agent, referrer, state, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (uuid, user_id, button_id, button_user_id, amount, referrer_user_id, ip_address, user_agent, referrer, 1, created_at, datetime.utcnow()))
      result = data_cursor.fetchone()
      click_id = result[0]
    else:
      # first insert the click with the full amount and no receiver- this will be the parent click
      data_cursor.execute("INSERT INTO clicks (uuid, user_id, button_id, receiver_user_id, amount, referrer_user_id, ip_address, user_agent, referrer, state, share_users, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (uuid, user_id, button_id, None, amount, referrer_user_id, ip_address, user_agent, referrer, 1, json.dumps(share_users), created_at, datetime.utcnow()))
      result = data_cursor.fetchone()
      click_id = result[0]
      # create a click for each user in the share, giving them their amount
      remainder = 100
      for share in share_users:
        data_cursor.execute("INSERT INTO clicks (uuid, parent_click_id, user_id, button_id, receiver_user_id, amount, referrer_user_id, ip_address, user_agent, referrer, state, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (str(uuid_mod.uuid4()), click_id, user_id, button_id, share['user'], amount * share['share_amount'] / 100, referrer_user_id, ip_address, user_agent, referrer, 1, created_at, datetime.utcnow()))
        remainder -= share['share_amount']
      # finally, give the remainder to the button owner
      data_cursor.execute("INSERT INTO clicks (uuid, parent_click_id, user_id, button_id, receiver_user_id, amount, referrer_user_id, ip_address, user_agent, referrer, state, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (str(uuid_mod.uuid4()), click_id, user_id, button_id, button_user_id, amount * remainder / 100, referrer_user_id, ip_address, user_agent, referrer, 1, created_at, datetime.utcnow()))
    logger.info(uuid + ':inserted')
    # publish a facebook timeline action, if connected, and save resulting id with click
    fb_action_id = None
    if facebook_uid is not None:
      fb_action_id = publish_facebook_action_pledge(uuid, facebook_uid, button_user_nickname)
      if fb_action_id is not None:
        try:
          data_cursor.execute("UPDATE clicks SET fb_action_id=%s WHERE id=%s", (fb_action_id, click_id))
        except:
          logger.exception(uuid + ':unable to store fb_action_id, deleting')
          delete_facebook_action(uuid, fb_action_id)
          fb_action_id = None
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
          delete_facebook_action(uuid, fb_action_id)
      except:
        logger.exception(uuid + ':MANUAL ROLLBACK AND DB CONSISTENCY FIX REQUIRED')
        delete_facebook_action(uuid, fb_action_id)
    except:
      logger.exception(uuid + ':unexpected exception, rolling back')
      # close cursors and rollback
      if web_cursor is not None:
        web_cursor.close()
      pg_web.tpc_rollback()
      pg_data.tpc_rollback()
      delete_facebook_action(uuid, fb_action_id)
  except psycopg2.IntegrityError:
    # this should only be happening on duplicate uuid, update
    logger.warn(uuid + ':exists, will update')
    if data_cursor is not None:
      data_cursor.close()
    pg_data.tpc_rollback()
    update_click(uuid, user_id, facebook_uid, button_id, button_user_id, button_user_nickname, amount, created_at)
  except:
    e = sys.exc_info()[1]
    logger.exception(uuid + ':' + str(e))
    # close cursors and rollback
    if data_cursor is not None:
      data_cursor.close()
    pg_data.tpc_rollback()
    delete_facebook_action(uuid, fb_action_id)

def publish_facebook_action_pledge(uuid, facebook_uid, button_user_nickname):
  graph = facebook.GraphAPI(SETTINGS['FACEBOOK_APP_TOKEN'])
  fb_action_id = None
  try:
    result = graph.put_object(facebook_uid, SETTINGS['FACEBOOK_NAMESPACE'] + ':pledge_to', publisher=SETTINGS['URL_BASE_TIP'] + '/' + button_user_nickname)
    fb_action_id = result['id']
  except:
    logger.exception(uuid + ':unable to publish facebook action')
  return fb_action_id

def delete_facebook_action(uuid, fb_action_id):
  if id is not None:
    graph = facebook.GraphAPI(SETTINGS['FACEBOOK_APP_TOKEN'])
    try:
      graph.delete_object(fb_action_id)
      return True
    except:
      logger.exception(uuid + ':could not delete Facebook action with fb_action_id=' + str(fb_action_id))
  return False
  
      