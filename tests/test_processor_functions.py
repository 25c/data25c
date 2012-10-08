from config import SETTINGS, pg_connect
import json
import processor
import redis
import unittest

class TestProcessorFunctions(unittest.TestCase):
    
  def setUp(self):
    self.pg_web = pg_connect(SETTINGS['DATABASE_WEB_URL'])
    self.pg_data = pg_connect(SETTINGS['DATABASE_URL'])
    self.redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])
    
    # turn on autocommit
    self.pg_web.autocommit = True
    self.pg_data.autocommit = True
    
    # always start with an empty click database
    cursor = self.pg_data.cursor()
    cursor.execute('DELETE FROM clicks;')
    cursor.close()
    
    # make sure the test click user starts with 0 balance
    cursor = self.pg_web.cursor()
    cursor.execute("UPDATE users SET balance=0 WHERE uuid=%s", ("3dd80d107941012f5e2c60c5470a09c8",))
    self.redis_data.set('user:3dd80d107941012f5e2c60c5470a09c8', 0)
    
    # clear the redis queues
    self.redis_data.delete('QUEUE')
    self.redis_data.delete('QUEUE_PROCESSING')
    
  def tearDown(self):
    self.pg_data.close()
    self.pg_web.close()
  
  def test_process_message(self):
    cursor_web = self.pg_web.cursor()
    cursor_data = self.pg_data.cursor()
    
    # invalid user_id, should be dropped
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"invaliduuid", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    processor.process_message(message)
    
    # balance still 0 at this point
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(0, result[0])
    self.assertEqual(0, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))
      
    # valid message, balance should be deducted and state changed    
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    processor.process_message(message)
    
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(25, result[0])
    self.assertEqual(25, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))
    
    cursor_data.execute('SELECT state FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertEqual(1, result[0])
    
    # process again, balance and state should be unchanged
    processor.process_message(message)
    
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(25, result[0])
    self.assertEqual(25, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))
    
    cursor_data.execute('SELECT state FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertEqual(1, result[0])
    
    # process a new message that changes the amount
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":1000, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    processor.process_message(message)
    
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(1000, result[0])
    self.assertEqual(1000, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))
    
    cursor_data.execute('SELECT state FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertEqual(1, result[0])
  
    
  def test_process_queue(self):
    # insert a valid click
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    self.redis_data.lpush('QUEUE', message)
    
    # test click should be inserted onto deduct queue
    self.assertEqual(1, self.redis_data.llen('QUEUE'))
    self.assertEqual(0, self.redis_data.llen('QUEUE_PROCESSING'))
    
    # process queue
    processor.process_queue()
    
    # both queues should now be empty
    self.assertEqual(0, self.redis_data.llen('QUEUE'))
    self.assertEqual(0, self.redis_data.llen('QUEUE_PROCESSING'))
    
if __name__ == '__main__':
  unittest.main()
