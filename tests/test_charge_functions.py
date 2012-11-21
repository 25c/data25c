from config import SETTINGS, pg_connect
from datetime import datetime,timedelta
import isodate
import json
import redis
import unittest
import uuid

import click
import charge

class TestChargeFunctions(unittest.TestCase):
  
  def setUp(self):
    self.pg_web = pg_connect(SETTINGS['DATABASE_WEB_URL'])
    self.pg_data = pg_connect(SETTINGS['DATABASE_URL'])
    self.redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])
    self.redis_web = redis.StrictRedis.from_url(SETTINGS['REDIS_WEB_URL'])
    # turn on autocommit
    self.pg_data.autocommit = True
    self.pg_web.autocommit = True
    
    # always start with an empty click database
    cursor = self.pg_data.cursor()
    cursor.execute('DELETE FROM clicks;')
    cursor.execute('DELETE FROM urls;')
    cursor.execute('DELETE FROM comments;')
    cursor.close()
    
    # make sure the test click user starts with 0 balance
    cursor = self.pg_web.cursor()
    cursor.execute("UPDATE users SET balance=0 WHERE uuid=%s", ("3dd80d107941012f5e2c60c5470a09c8",))
    self.redis_data.set('user:3dd80d107941012f5e2c60c5470a09c8', 0)
    
    # and no payments are in the db
    cursor.execute('DELETE FROM payments;')
    
    # set up a revenue share on another button
    cursor.execute("SELECT id FROM users WHERE uuid=%s", ("439bdb807941012f5e2d60c5470a09c8",))
    result = cursor.fetchone()
    share_users = json.dumps([{'user':result[0],'share_amount':10}])
    cursor.execute("UPDATE buttons SET share_users=%s WHERE uuid=%s", (share_users, "92d1cdb0f60c012f5f3960c5470a09c8",))
    
    # clear the redis/resque mail queue
    self.redis_web.delete('resque:queue:mailer')
    # clear the url scrape queue
    self.redis_data.delete('QUEUE_SCRAPER')
    
  def tearDown(self):
    self.pg_data.close()
    self.pg_web.close()
    self.redis_data.connection_pool.disconnect()
    self.redis_web.connection_pool.disconnect()
    
  def test_charge_user(self):
    cursor_data = self.pg_data.cursor()
    cursor_web = self.pg_web.cursor()
    
    # assert starting 0 balance
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(0, result[0])
    
    # insert old (i.e. past undo grace period) clicks just below $5 threshold
    for i in range(9):
      click_uuid = uuid.uuid4().hex
      click.insert_click(click_uuid, "3dd80d107941012f5e2c60c5470a09c8", "a4b16a40dff9012f5efd60c5470a09c8", None, None, None, None, 50*1000000, "127.0.0.1", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "http://localhost:3000/thisisfrancis", 1)
      # manually update db to make them past grace period
      old_date = datetime.utcnow() - timedelta(hours=2)
      cursor_data.execute("UPDATE clicks SET created_at=%s, updated_at=%s WHERE uuid=%s", (old_date, old_date, click_uuid))
    
    # assert ending balance of $4.50
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(450*1000000, result[0])
    
    # assert that this user does not get processed for charge
    user_ids = charge.get_user_ids()
    self.assertEqual([], user_ids)
    
    # insert a new click (still within grace period) to push balance past $5
    click.insert_click(uuid.uuid4().hex, "3dd80d107941012f5e2c60c5470a09c8", "a4b16a40dff9012f5efd60c5470a09c8", None, None, None, None, 100*1000000, "127.0.0.1", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "http://localhost:3000/thisisfrancis", 1)
    
    # now the user id will be returned
    user_ids = charge.get_user_ids()
    self.assertEqual([(568334,)], user_ids)
    
    # but, if you process this user, no payment will be made since the click that pushed the balance past the threshold is still within 1 hour
    charge.charge_user(568334)
    cursor_web.execute('SELECT COUNT(*) FROM payments;')
    result = cursor_web.fetchone()
    self.assertEqual(0, result[0])
    
    # insert an old click that WILL put total chargeable balance over $5
    click_uuid = uuid.uuid4().hex
    click.insert_click(click_uuid, "3dd80d107941012f5e2c60c5470a09c8", "a4b16a40dff9012f5efd60c5470a09c8", None, None, None, None, 75*1000000, "127.0.0.1", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "http://localhost:3000/thisisfrancis", 1)
    old_date = datetime.utcnow() - timedelta(hours=2)
    cursor_data.execute("UPDATE clicks SET created_at=%s, updated_at=%s WHERE uuid=%s", (old_date, old_date, click_uuid))
    # total balance shold now be $6.25
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(625*1000000, result[0])
    
    # now a charge should be made
    charge.charge_user(568334)
    cursor_web.execute('SELECT COUNT(*) FROM payments;')
    result = cursor_web.fetchone()
    self.assertEqual(1, result[0])
    
    # assert payment details
    cursor_web.execute('SELECT id,user_id,amount,state,payment_type,transaction_id FROM payments;')
    result = cursor_web.fetchone()
    self.assertEqual((568334,525*1000000,2,'payin'), result[1:-1])
    # transaction id from stripe should be set
    self.assertIsNotNone(result[5])
    payment_id = result[0]
    
    # assert balance update- should now just be the new $1.00 click still in undo grace period
    cursor_web.execute("SELECT balance FROM users WHERE id=%s", (568334,))
    result = cursor_web.fetchone()
    self.assertEqual(100*1000000, result[0])
    
    # assert click status change
    cursor_data.execute("SELECT COUNT(*) FROM clicks WHERE user_id=%s AND state=2 AND payment_id=%s", (568334, payment_id))
    result = cursor_data.fetchone()
    self.assertEqual(10, result[0])
    
    # invoice email should be queued up
    self.assertEqual(1, self.redis_web.llen('resque:queue:mailer'))
    data = json.loads(self.redis_web.lindex('resque:queue:mailer', 0))
    self.assertEqual(['new_invoice', 568334, payment_id], data['args'])
        
if __name__ == '__main__':
  unittest.main()
