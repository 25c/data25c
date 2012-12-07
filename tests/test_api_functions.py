from config import SETTINGS, pg_connect
from datetime import datetime
from decimal import Decimal
import json
import redis
import unittest
import uuid

import api
import click
import processor

class TestApiFunctions(unittest.TestCase):
    
  def setUp(self):
    self.app = api.app.test_client()  
    self.pg_web = pg_connect(SETTINGS['DATABASE_WEB_URL'])
    self.pg_data = pg_connect(SETTINGS['DATABASE_URL'])
    self.redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])
    # turn on autocommit
    self.pg_data.autocommit = True
    self.pg_web.autocommit = True
    
    
    # always start with an empty click database
    cursor = self.pg_data.cursor()
    cursor.execute('DELETE FROM clicks;')
    cursor.execute('DELETE FROM urls;')
    cursor.execute('DELETE FROM comments;')
    cursor.close()
      
    # make sure the all test users start with 0 balance, no free credit
    cursor = self.pg_web.cursor()
    cursor.execute("UPDATE users SET balance_paid=0, balance_free=100, total_given=0")
    cursor.execute("SELECT id FROM users WHERE uuid=%s", ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor.fetchone()
    self.user_id = result[0]
    cursor.execute('DELETE FROM payments')
    
    # insert and process a valid click
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    processor.process_message(message)
  
    # clear redis
    self.redis_data.flushdb()
    
  def tearDown(self):
    self.pg_data.close()
    self.pg_web.close()
    self.redis_data.connection_pool.disconnect()
    
  def test_undo_click(self):
    cursor_web = self.pg_web.cursor()
    cursor_data = self.pg_data.cursor()
    
    # assert starting state and balance
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0, 75, 25), result)
    
    # undo the click
    self.app.post('/api/clicks/undo', data='uuids[]=a2afb8a0-fc6f-11e1-b984-eff95004abc9', content_type='application/x-www-form-urlencoded')
    
    # assert ending state and balance
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0, 100, 0), result)
    
if __name__ == '__main__':
  unittest.main()
