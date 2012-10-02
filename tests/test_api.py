from config import SETTINGS, pg_connect
import json
import redis
import unittest

import api
import click
import processor
import validator

class TestClickFunctions(unittest.TestCase):
  
  app = api.app.test_client()  
  pg_data = pg_connect(SETTINGS['DATABASE_URL'])
  pg_web = pg_connect(SETTINGS['DATABASE_WEB_URL'])
  redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])
  
  def setUp(self):
    # turn on autocommit
    self.pg_data.autocommit = True
    self.pg_web.autocommit = True
    
    # always start with an empty click database
    cursor = self.pg_data.cursor()
    cursor.execute('DELETE FROM clicks;')
    cursor.close()
    
    # make sure the test click user starts with 0 balance
    cursor = self.pg_web.cursor()
    cursor.execute("UPDATE users SET balance=0 WHERE uuid=%s", ("3dd80d107941012f5e2c60c5470a09c8",))
    self.redis_data.set('user:3dd80d107941012f5e2c60c5470a09c8', 0)
    
    # insert and process a valid click
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    validator.insert_click(data)
    processor.deduct_click(data)
    
  def test_undo_click(self):
    cursor_web = self.pg_web.cursor()
    cursor_data = self.pg_data.cursor()
    
    # assert starting state and balance
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(-1, result[0])
    self.assertEqual(-1, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))
    
    cursor_data.execute('SELECT state FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertEqual(1, result[0])
    
    # undo the click
    self.app.post('/api/clicks/undo', data='uuids[]=a2afb8a0-fc6f-11e1-b984-eff95004abc9', content_type='application/x-www-form-urlencoded')
    
    # assert ending state and balance
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(0, result[0])
    self.assertEqual(0, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))
    
    cursor_data.execute('SELECT state FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertEqual(5, result[0])
    
  def test_fund_click(self):
    cursor_web = self.pg_web.cursor()
    cursor_data = self.pg_data.cursor()
    
    # assert starting state and balance
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(-1, result[0])
    self.assertEqual(-1, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))
    
    cursor_data.execute('SELECT state FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertEqual(1, result[0])
    
    # undo the click
    self.app.post('/api/clicks/fund', data='uuids[]=a2afb8a0-fc6f-11e1-b984-eff95004abc9', content_type='application/x-www-form-urlencoded')
    
    # assert ending state and balance
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(0, result[0])
    self.assertEqual(0, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))
    
    cursor_data.execute('SELECT state FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertEqual(2, result[0])
    
if __name__ == '__main__':
  unittest.main()
