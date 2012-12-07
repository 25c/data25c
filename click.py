from airbrakepy.logging.handlers import AirbrakeHandler
from config import SETTINGS, pg_connect
from datetime import datetime, timedelta

import facebook
import json
import logging
import psycopg2
import redis
import scraper
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
  
# initialize redis connections
redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])
redis_web = redis.StrictRedis.from_url(SETTINGS['REDIS_WEB_URL'])
  
def validate_click(uuid, user_uuid, button_uuid, url, comment_uuid, referrer_user_uuid):
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
      
    # validate url
    url_id = None
    if url is not None:
      cursor_data = None
      try:
        pg_data.autocommit = True
        cursor_data = pg_data.cursor()
        cursor_data.execute("SELECT id FROM urls WHERE url=%s", (url,))
        result = cursor_data.fetchone()
        if result is None:
          # insert and enqueue for scrape
          now = datetime.utcnow()
          cursor_data.execute("INSERT INTO urls (uuid, url, created_at, updated_at) VALUES (%s, %s, %s, %s) RETURNING id", (uuid_mod.uuid4().hex, url, now, now))
          result = cursor_data.fetchone()
          scraper.enqueue_url(url)
        url_id = result[0]
      finally:
        pg_data.autocommit = False
        if cursor_data is not None:
          cursor_data.close()

    # validate comment uuid, if present
    comment_id = None
    comment_user_id = None
    if comment_uuid is not None:
      cursor_data = None
      try:
        pg_data.autocommit = True
        cursor_data = pg_data.cursor()
        cursor_data.execute("SELECT id, user_id FROM comments WHERE LOWER(uuid)=LOWER(%s)", (comment_uuid,))
        result = cursor_data.fetchone()
        if result is not None:
          comment_id = result[0]
          comment_user_id = result[1]
        else:
          logger.warn(uuid + ':invalid comment_uuid=' + str(comment_uuid))
      finally:
        pg_data.autocommit = False
        if cursor_data is not None:
          cursor_data.close()
  
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
    return (user_id, facebook_uid, button_id, url_id, comment_id, comment_user_id, referrer_user_id, button_user_id, button_user_nickname, share)
  finally:
    if cursor is not None:
      cursor.close()
    pg_web.autocommit = False
  
def update_click(uuid, user_id, facebook_uid, button_id, button_user_id, button_user_nickname, comment_id, comment_user_id, comment_text, amount, created_at):
  xid_data = uuid + '-' + str(created_at) + '-update-click'
  xid_web = uuid + '-' + str(created_at) + '-update-user'
  data_cursor = None
  web_cursor = None
  try:
    # start tpc transaction on data and web
    pg_data.tpc_begin(xid_data)
    data_cursor = pg_data.cursor() 
    pg_web.tpc_begin(xid_web)
    web_cursor = pg_web.cursor()    
    
    # update comment, if applicable
    if comment_id is not None and user_id == comment_user_id and comment_text is not None:
      data_cursor.execute("UPDATE comments SET content=%s, updated_at=%s WHERE id=%s", (comment_text, datetime.utcnow(), comment_id))
    # get previous value
    data_cursor.execute("SELECT id, state, amount, amount_paid, amount_free, share_users, fb_action_id, url_id, created_at FROM clicks WHERE LOWER(uuid) = LOWER(%s) FOR UPDATE", (uuid, ))
    result = data_cursor.fetchone()
    if result is None:
      raise Exception(uuid + ':click not found')
    click_id = result[0]
    state = result[1]
    if state != 'given':
      raise Exception(uuid + ':click already state=' + str(state))
    old_amount = result[2]
    amount_paid = old_amount_paid = result[3]
    amount_free = old_amount_free = result[4]
    share_users = result[5]
    fb_action_id = result[6]
    url_id = result[7]
    old_created_at = result[8].replace(tzinfo=created_at.tzinfo)
    # check if newer than this
    if old_created_at > created_at:
      raise Exception(uuid + ':out of order message dropped')
    # check if within 1 hour grace period
    if old_created_at < (datetime.utcnow() - timedelta(hours=1)):
      raise Exception(uuid + ':past edit/undo grace period for update')
    
    # check if this is a comment tip, and if we need to cascade an undo to subsequent comment promotion tips
    cascade_undo_uuids = []
    if amount == 0 and old_amount > 0 and comment_id is not None:
      # check if this is the original commenter
      data_cursor.execute("SELECT click_id FROM comments WHERE id=%s", (comment_id,))
      result = data_cursor.fetchone()
      if result is None:
        raise Exception(uuid + ':click comment not found')
      if result[0] == click_id:
        # fetch all the click uuids to undo
        data_cursor.execute("SELECT uuid FROM clicks WHERE comment_id=%s AND id<>%s", (comment_id, click_id))
        for result in data_cursor:
          cascade_undo_uuids.append(result[0])
    
    # check if we need to delete/re-publish facebook action
    if amount > 0 and old_amount == 0 and facebook_uid is not None:
      # republish facebook action
      fb_action_id = publish_facebook_action_pledge(uuid, facebook_uid, button_user_nickname)
    elif amount == 0 and old_amount > 0 and fb_action_id is not None:
      # delete facebook action
      if delete_facebook_action(uuid, fb_action_id):
        fb_action_id = None
    
    # get the user's paid/free balances
    web_cursor.execute("SELECT uuid, balance_paid, balance_free, total_given FROM users WHERE id=%s FOR UPDATE", (user_id,))
    result = web_cursor.fetchone()
    if result is None:
      raise Exception(uuid + ':invalid user_id=' + user_id)
    user_uuid = result[0]
    balance_paid = old_balance_paid = result[1]
    balance_free = old_balance_free = result[2]
    total_given = result[3] - old_amount + amount
      
    # calculate new paid/free amounts
    if old_amount < amount:
      amount_diff = amount - old_amount
      # check if free credit available to handle additional amount
      amount_free_diff = min(balance_free, amount_diff)
      amount_paid_diff = amount_diff - amount_free_diff
      amount_paid += amount_paid_diff
      amount_free += amount_free_diff
      balance_free -= amount_free_diff
      balance_paid -= amount_paid_diff
    elif old_amount > amount:
      amount_diff = old_amount - amount
      # check paid credit to refund first
      amount_paid_diff = min(amount_paid, amount_diff)
      amount_free_diff = amount_diff - amount_paid_diff
      amount_paid -= amount_paid_diff
      amount_free -= amount_free_diff
      balance_paid += amount_paid_diff
      balance_free += amount_free_diff
      
    # check if balance is sufficient for amount
    if balance_paid < 0:
      raise Exception("%s: insufficient balance (free=%s, paid=%s) for amount=%s" % (uuid, old_balance_free + old_amount_paid, old_balance_paid + old_amount_paid, amount))
      
    # update click
    data_cursor.execute("UPDATE clicks SET amount=%s, amount_paid=%s, amount_free=%s, fb_action_id=%s, updated_at=%s WHERE id=%s", (amount, amount_paid, amount_free, fb_action_id, datetime.utcnow(), click_id))
    if share_users is not None:
      # iterate over and update share amount
      try:
        share_users = json.loads(share_users)
        remainder = 100
        for share in share_users:
          data_cursor.execute("UPDATE clicks SET amount=%s, amount_paid=%s, amount_free=%s, updated_at=%s WHERE parent_click_id=%s AND receiver_user_id=%s", (amount*share['share_amount']/100, amount_paid*share['share_amount']/100, amount_free*share['share_amount']/100, datetime.utcnow(), click_id, share['user']))
          remainder -= share['share_amount']
        data_cursor.execute("UPDATE clicks SET amount=%s, amount_paid=%s, amount_free=%s, updated_at=%s WHERE parent_click_id=%s AND receiver_user_id=%s", (amount*remainder/100, amount_paid*remainder/100, amount_free*remainder/100, datetime.utcnow(), click_id, button_user_id))
      except ValueError:
        logger.exception(uuid + ': could not parse revenue share definition')
        
    # update user balance
    web_cursor.execute("UPDATE users SET balance_paid=%s, balance_free=%s, total_given=%s, updated_at=%s WHERE id=%s", (balance_paid, balance_free, total_given, datetime.utcnow(), user_id))
    
    # prepare tpc transaction
    data_cursor.close()
    data_cursor = None
    pg_data.tpc_prepare()
    # prepare tpc transaction on web
    web_cursor.close()
    web_cursor = None
    pg_web.tpc_prepare()
    try:
      # finally, try to commit both
      pg_web.tpc_commit()
      try:
        pg_data.tpc_commit()
        try:
          logger.info(uuid + ':click updated, balance_paid=' + str(balance_paid) + ', balance_free=' + str(balance_free) + ' for user_uuid=' + user_uuid)
          # if any other clicks to undo, process them now
          for uuid in cascade_undo_uuids:
            undo_click(uuid)
          # update redis widget data cache
          (widget_type, before, after) = update_widget(button_id, url_id)
          # send widget notifications
          send_widget_notifications(widget_type, button_id, url_id, before, after)
        except:
          logger.exception(uuid + ':unexpected exception after successful commits')
      except:
        logger.exception(uuid + ':MANUAL ROLLBACK AND DB CONSISTENCY FIX REQUIRED')
    except:
      e = sys.exc_info()[1]
      logger.exception(uuid + ':' + str(e))
      # rollback
      pg_web.tpc_rollback()
      pg_data.tpc_rollback()    
  except:
    e = sys.exc_info()[1]
    logger.exception(uuid + ':' + str(e))
    # close cursors and rollback
    if data_cursor is not None:
      data_cursor.close()
    pg_data.tpc_rollback()
    if web_cursor is not None:
      web_cursor.close()
    pg_web.tpc_rollback()
    
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
      update_click(uuid, user_id, facebook_uid, button_id, button_user_id, button_user_nickname, None, None, None, 0, datetime.utcnow())
  
def insert_click(uuid, user_uuid, button_uuid, url, comment_uuid, comment_text, referrer_user_uuid, amount, ip_address, user_agent, referrer, created_at):
  ids = validate_click(uuid, user_uuid, button_uuid, url, comment_uuid, referrer_user_uuid)
  if ids is None:
    return
  user_id = ids[0]
  facebook_uid = ids[1]
  button_id = ids[2]
  url_id = ids[3]
  comment_id = ids[4]
  comment_user_id = ids[5]
  referrer_user_id = ids[6]
  button_user_id = ids[7]
  button_user_nickname = ids[8]
  share_users = ids[9]
  
  # check if click already exists
  found = False
  try:
    data_cursor = pg_data.cursor()
    data_cursor.execute("SELECT id FROM clicks WHERE LOWER(uuid)=LOWER(%s)", (uuid,))
    result = data_cursor.fetchone()
    found = result is not None
  finally:
    pg_data.commit()
    
  if found:
    update_click(uuid, user_id, facebook_uid, button_id, button_user_id, button_user_nickname, comment_id, comment_user_id, comment_text, amount, created_at)
    return
  
  xid_data = uuid + '-' + str(created_at) + '-insert-click'
  xid_web = uuid + '-' + str(created_at) + '-insert-user'
  data_cursor = None
  web_cursor = None
  try:
    # update user balance
    pg_web.tpc_begin(xid_web)
    web_cursor = pg_web.cursor()    
    # check and update balance
    web_cursor.execute("SELECT uuid, balance_paid, balance_free, total_given FROM users WHERE id=%s FOR UPDATE", (user_id,))
    result = web_cursor.fetchone()
    if result is None:
      raise Exception(uuid + ':invalid user_id=' + user_id)
    user_uuid = result[0]
    balance_free = result[2]
    balance_paid = result[1]
    total_given = result[3]
    # check if balance is sufficient for amount
    if amount > (balance_free + balance_paid):
      raise Exception("%s: insufficient balance (free=%s, paid=%s) for amount=%s" % (uuid, balance_free, balance_paid, amount))
    # add to the total amount
    total_given += amount
    # calculate amount paid/free
    amount_free = 0
    amount_paid = amount
    # use any free points available
    if balance_free > 0:
      amount_free = min(balance_free, amount)
      amount_paid = amount - amount_free
    # adjust corresponding balances
    balance_free -= amount_free    
    balance_paid -= amount_paid
    web_cursor.execute("UPDATE users SET balance_paid=%s, balance_free=%s, total_given=%s, updated_at=%s WHERE id=%s", (balance_paid, balance_free, total_given, datetime.utcnow(), user_id))
    web_cursor.close()
    web_cursor = None
    # prepare tpc transaction on web
    pg_web.tpc_prepare()
    try:
      # start tpc transaction on data
      pg_data.tpc_begin(xid_data)
      data_cursor = pg_data.cursor()
      # attempt insert
      if share_users is None:
        # no share, so just insert this click with the button owner as the receiver of the full amount
        data_cursor.execute("INSERT INTO clicks (uuid, user_id, button_id, url_id, comment_id, receiver_user_id, amount, amount_paid, amount_free, referrer_user_id, ip_address, user_agent, referrer, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (uuid, user_id, button_id, url_id, comment_id, button_user_id, amount, amount_paid, amount_free, referrer_user_id, ip_address, user_agent, referrer, created_at, datetime.utcnow()))
        result = data_cursor.fetchone()
        click_id = result[0]
      else:
        # first insert the click with the full amount and no receiver- this will be the parent click
        data_cursor.execute("INSERT INTO clicks (uuid, user_id, button_id, url_id, comment_id, receiver_user_id, amount, amount_paid, amount_free, referrer_user_id, ip_address, user_agent, referrer, share_users, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (uuid, user_id, button_id, url_id, comment_id, None, amount, amount_paid, amount_free, referrer_user_id, ip_address, user_agent, referrer, json.dumps(share_users), created_at, datetime.utcnow()))
        result = data_cursor.fetchone()
        click_id = result[0]
        # create a click for each user in the share, giving them their amount
        remainder = 100
        for share in share_users:
          data_cursor.execute("INSERT INTO clicks (uuid, parent_click_id, user_id, button_id, url_id, receiver_user_id, amount, amount_paid, amount_free, referrer_user_id, ip_address, user_agent, referrer, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (str(uuid_mod.uuid4()), click_id, user_id, button_id, url_id, share['user'], amount * share['share_amount'] / 100.0, amount_paid * share['share_amount'] / 100.0, amount_free * share['share_amount'] / 100.0, referrer_user_id, ip_address, user_agent, referrer, created_at, datetime.utcnow()))
          remainder -= share['share_amount']
        # finally, give the remainder to the button owner
        data_cursor.execute("INSERT INTO clicks (uuid, parent_click_id, user_id, button_id, url_id, receiver_user_id, amount, amount_paid, amount_free, referrer_user_id, ip_address, user_agent, referrer, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (str(uuid_mod.uuid4()), click_id, user_id, button_id, url_id, button_user_id, amount * remainder / 100.0, amount_paid * remainder / 100.0, amount_free * remainder / 100.0, referrer_user_id, ip_address, user_agent, referrer, created_at, datetime.utcnow()))
      # insert comment, if any
      if comment_id is None and comment_text is not None:
        # insert
        now = datetime.utcnow()
        data_cursor.execute("INSERT INTO comments (uuid, user_id, button_id, url_id, click_id, content, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (comment_uuid, user_id, button_id, url_id, click_id, comment_text, now, now))
        result = data_cursor.fetchone()
        comment_id = result[0]
        # update click with comment id
        data_cursor.execute("UPDATE clicks SET comment_id=%s WHERE id=%s", (comment_id, click_id))
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
      # check if a title exists for the referrer url, if any
      data_cursor.execute("SELECT updated_at FROM urls WHERE url=%s", (referrer,))
      result = data_cursor.fetchone()
      title_updated_at = None
      if result is not None:
        title_updated_at = result[0]
      # prepare tpc transaction
      data_cursor = None
      pg_data.tpc_prepare()
      # finally, try to commit both
      pg_data.tpc_commit()
      try:
        pg_web.tpc_commit()
        try:
          logger.info(uuid + ':click inserted, balance_paid=' + str(balance_paid) + ', balance_free=' + str(balance_free) + ' for user_uuid=' + user_uuid)
          # enqueue url scrape on referrer if necessary
          # TODO also enqueue if older than a certain age
          if title_updated_at is None:
            scraper.enqueue_url(referrer)

          # send first and second click emails 
          cursor = pg_data.cursor()
          cursor.execute("SELECT COUNT(*) FROM clicks WHERE user_id = %s AND parent_click_id IS NULL", (user_id,))
          result = cursor.fetchone()
          logger.warn(str(result[0]) + ' clicks for this user')
          cursor.close();
          pg_data.commit()
          if result[0] == 1:
            logger.warn('sending first click email for user ' + str(user_id))
            send_new_user_FirstClick_email(user_id, url_id)
          elif result[0] == 2:
            logger.warn('sending second click email for user ' + str(user_id))
            send_new_user_SecondClick_email(user_id, url_id)

          # update redis widget data cache
          (widget_type, before, after) = update_widget(button_id, url_id)
          # send widget notifications
          send_widget_notifications(widget_type, button_id, url_id, before, after)
            
          # send email for comment promotion
          if comment_id is not None and comment_text is None and user_id != comment_user_id:
            # find position of comment
            position = 1
            for comment in after:
              if comment['uuid'] == comment_uuid:
                break
              position += 1              
            send_testimonial_promoted_email(comment_id, user_id, amount, position)
        except:
          logger.exception(uuid + ':unexpected exception after successful commits')
          delete_facebook_action(uuid, fb_action_id)
      except:
        logger.exception(uuid + ':MANUAL ROLLBACK AND DB CONSISTENCY FIX REQUIRED')
        delete_facebook_action(uuid, fb_action_id)
    except psycopg2.IntegrityError:
      # this should only be happening on duplicate uuid, update
      logger.warn(uuid + ':exists, will update')
      pg_data.tpc_rollback()
      pg_web.tpc_rollback()
      update_click(uuid, user_id, facebook_uid, button_id, button_user_id, button_user_nickname, comment_id, comment_user_id, comment_text, amount, created_at)
    except:
      logger.exception(uuid + ':unexpected exception, rolling back')
      # rollback
      pg_data.tpc_rollback()
      pg_web.tpc_rollback()
      delete_facebook_action(uuid, fb_action_id)
  except:
    logger.exception(uuid + ':unexpected exception, rolling back')
    # rollback
    pg_web.tpc_rollback()
    
def insert_title(url, title):
  try:
    now = datetime.utcnow()
    data_cursor = pg_data.cursor()
    data_cursor.execute("INSERT INTO urls (uuid, url, title, updated_at, created_at) VALUES (%s, %s, %s, %s, %s)", (uuid_mod.uuid4().hex, url, title, now, now))
  except psycopg2.IntegrityError:
    pg_data.commit()    
    data_cursor.execute("UPDATE urls SET title=%s, updated_at=%s WHERE url=%s", (title, now, url))
  except:
    logger.exception("Unable to insert (url, title)=(%s, %s)", url, title)
  finally:    
    data_cursor.close()
    pg_data.commit()

def publish_facebook_action_pledge(uuid, facebook_uid, button_user_nickname):
  if SETTINGS['PYTHON_ENV'] == 'test':
    return
    
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
  
def send_fund_reminder_email(user_id):
  data = { 'class': 'UserMailer', 'args':[ 'fund_reminder', user_id ] }
  redis_web.rpush('resque:queue:mailer', json.dumps(data))

def send_new_position_in_fanbelt_email(user_uuid, url_id, prev_pos, cur_pos):
  data = { 'class': 'UserMailer', 'args':[ 'new_position_in_fanbelt', user_uuid, url_id, prev_pos, cur_pos ] }
  redis_web.rpush('resque:queue:mailer', json.dumps(data))

def send_new_position_in_testimonial_email(comment_uuid, prev_pos, cur_pos):
  data = { 'class': 'CommentMailer', 'args':[ 'new_position_in_testimonial', comment_uuid, prev_pos, cur_pos ] }
  redis_web.rpush('resque:queue:mailer', json.dumps(data))
    
def send_new_user_FirstClick_email(user_id, url_id):
  data = { 'class': 'UserMailer', 'args':[ 'new_user_FirstClick', user_id, url_id ] }
  redis_web.rpush('resque:queue:mailer', json.dumps(data))

def send_new_user_SecondClick_email(user_id, url_id):
  data = { 'class': 'UserMailer', 'args':[ 'new_user_SecondClick', user_id, url_id ] }
  redis_web.rpush('resque:queue:mailer', json.dumps(data))
  
def send_testimonial_promoted_email(comment_id, tipper_user_id, amount, position):
  data = { 'class': 'CommentMailer', 'args':[ 'testimonial_promoted', comment_id, tipper_user_id, amount, position ] }
  redis_web.rpush('resque:queue:mailer', json.dumps(data))

def send_new_unmoderated_comment_email(user_id, tipper_id, comment_id, url_title, promoted_amount):
  data = { 'class': 'UserMailer', 'args':[ 'new_unmoderated_comment', user_id, tipper_id, comment_id, url_title, promoted_amount ] }
  redis_web.rpush('resque:queue:mailer', json.dumps(data))

def send_widget_notifications(widget_type, widget_id, url_id, before, after):
  if widget_type == 'testimonials':
    new_position = 1
    for comment in after:
      # look for comment in before state for previous position
      found = False
      prev_position = 1
      if before is not None:
        for prev_comment in before:
          if prev_comment['uuid'] == comment['uuid']:
            found = True
            break
          prev_position += 1
      if not found:
        prev_position = -1
      # send new position email if changed
      if prev_position != new_position:
        send_new_position_in_testimonial_email(comment['uuid'], prev_position, new_position)
      new_position += 1      
  elif widget_type == 'fan_belt':
    new_position = 1
    for user in after:
      # look for user in before state for previous position
      found = False
      prev_position = 1
      if before is not None:
        for prev_user in before:
          if prev_user['uuid'] == user['uuid']:
            found = True
            break
          prev_position += 1
      if not found:
        prev_position = -1
      if prev_position != new_position:
        send_new_position_in_fanbelt_email(user['uuid'], url_id, prev_position, new_position)
      new_position += 1
      
    prev_position = 1
    for user in before:
      # check if user was pushed out
      found = False
      for new_user in after:
        if new_user['uuid'] == user['uuid']:
          found = True
          break
      if not found:
        send_new_position_in_fanbelt_email(user['uuid'], url_id, prev_position, -1)
      prev_position += 1
        
def update_widget(widget_id, url_id):
  if url_id is None:
    return (None, None, None)
    
  web_cursor = None
  data_cursor = None
  try:
    web_cursor = pg_web.cursor()
    web_cursor.execute("SELECT uuid,widget_type FROM buttons WHERE id=%s", (widget_id,))
    result = web_cursor.fetchone()
    if result is None:
      raise Exception("Widget not found")
    widget_uuid = result[0]
    widget_type = result[1]
    
    data_cursor = pg_data.cursor()
    data_cursor.execute("SELECT url FROM urls WHERE id=%s", (url_id,))
    result = data_cursor.fetchone()
    if result is None:
      raise Exception("URL not found with url_id=%s" % (url_id,))
    url = result[0]
  
    before = None
    data = None
    if widget_type == 'testimonials':
      data = []
      comments = {}
      users = {}
      comment_ids = set()
      user_ids = set()
      # fetch all tips for comments in this widget, collecting the user ids and comment ids in the process
      data_cursor.execute("SELECT comment_id,user_id,SUM(amount),MIN(created_at) FROM clicks WHERE button_id=%s AND url_id=%s AND amount>0 GROUP BY comment_id,user_id", (widget_id, url_id))
      for result in data_cursor:
        comment_id = result[0]
        user_id = result[1]
        amount = result[2]
        created_at = result[3]
        comment_ids.add(comment_id)
        user_ids.add(user_id)
        if comment_id not in comments:
          comments[comment_id] = { 'amount':0, 'promoters':[] }
        comments[comment_id]['amount'] += long(amount)
        comments[comment_id]['promoters'].append({'id':user_id, 'amount':long(amount), 'created_at':created_at})
      # now fetch comment data
      if len(comment_ids) > 0:
        data_cursor.execute("SELECT id,uuid,user_id,content,created_at FROM comments WHERE id IN %s", (tuple(comment_ids),))
        for result in data_cursor:
          comment_id = result[0]
          comments[comment_id]['uuid'] = result[1]
          comments[comment_id]['owner_id'] = result[2]
          comments[comment_id]['content'] = result[3]
          comments[comment_id]['created_at'] = result[4]
        # now fetch user data
        web_cursor.execute("SELECT id,uuid,pledge_name FROM users WHERE id IN %s", (tuple(user_ids),))
        for result in web_cursor:
          user_id = result[0]
          users[user_id] = { 'uuid':result[1], 'name':result[2] }
        # now combine and sort
        for comment_id in comments:
          comment = comments[comment_id]
          # remove owner from promoters list
          comment['promoters'] = [promoter for promoter in comment['promoters'] if promoter['id'] != comment['owner_id']]
          # remove owner id from metadata
          comment['owner'] = users[comment['owner_id']]
          del comment['owner_id']
          # set uuid and name for each promoter, remove id
          for promoter in comment['promoters']:
            user = users[promoter['id']]
            promoter['uuid'] = user['uuid']
            promoter['name'] = user['name']
            del promoter['id']
          # sort by amount, date
          comment['promoters'].sort(key=lambda x: (-x['amount'],x['created_at']))
          # add to final list
          data.append(comment)
        # sort final list by amount, date
        data.sort(key=lambda x: (-x['amount'],x['created_at']))
      # serialize and store
      before = redis_data.getset("%s:%s" % (widget_uuid,url), json.dumps(data, default= lambda obj: obj.isoformat() if isinstance(obj, datetime) else None))
      # return data for notification comparison
      if before is not None:
        before = json.loads(before) 
      else:
        before = []
    elif widget_type == 'fan_belt':
      # fetch top 5 users for this fan belt
      data_cursor.execute("SELECT user_id,SUM(amount) AS total_amount,MIN(created_at) AS first_created_at FROM clicks WHERE button_id=%s AND url_id=%s AND amount>0 GROUP BY user_id ORDER BY total_amount DESC, first_created_at ASC LIMIT 5", (widget_id, url_id))
      data = []
      user_ids = []
      for result in data_cursor:
        user_ids.append(result[0])
        data.append({ 'id':result[0], 'amount':long(result[1]), 'created_at':result[2] })
      if len(user_ids) > 0:
        # get detailed user data
        users = {}
        web_cursor.execute("SELECT id,uuid,pledge_name FROM users WHERE id IN %s", (tuple(user_ids),))
        for result in web_cursor:
          user_id = result[0]
          users[user_id] = { 'uuid':result[1], 'name':result[2] }
        # combine
        for user in data:
          user['uuid'] = users[user['id']]['uuid']
          user['name'] = users[user['id']]['name']
          del user['id']
      before = redis_data.getset("%s:%s" % (widget_uuid,url), json.dumps(data, default= lambda obj: obj.isoformat() if isinstance(obj, datetime) else None))
      if before is not None:
        before = json.loads(before)
      else:
        before = []
    return (widget_type, before, data)
  except:
    logger.exception("Unexpected error updating widget with button_id=%s" % (widget_id,))
  finally:
    if web_cursor is not None:
      web_cursor.close()
      pg_web.commit()
    if data_cursor is not None:
      data_cursor.close()
      pg_data.commit()
