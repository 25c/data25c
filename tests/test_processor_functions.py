from config import SETTINGS, pg_connect
import json
import processor
import redis
import unittest
import validator

class TestProcessorFunctions(unittest.TestCase):
  
  pg_web = pg_connect(SETTINGS['DATABASE_WEB_URL'])
  pg_data = pg_connect(SETTINGS['DATABASE_URL'])
  redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])
  
  def setUp(self):
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
    
    # clear the redis queues
    self.redis_data.delete('QUEUE')
    self.redis_data.delete('QUEUE_PROCESSING')
    self.redis_data.delete('QUEUE_DEDUCT')
    self.redis_data.delete('QUEUE_DEDUCT_PROCESSING')
    
    # insert a valid click
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    self.redis_data.lpush('QUEUE', message)
    validator.process_queue()
  
  def test_deduct_click(self):
    cursor_web = self.pg_web.cursor()
    cursor_data = self.pg_data.cursor()
    
    # invalid user_id, should be dropped
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"invaliduuid", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    processor.deduct_click(data)
    
    # invalid click uuid, should be dropped
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(0, result[0])
    
    message = '{"uuid":"invaliduuid", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    processor.deduct_click(data)
    
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(0, result[0])
  
    # valid message, balance should be deducted and state changed
    
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(0, result[0])
    
    cursor_data.execute('SELECT state FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertEqual(0, result[0])
    
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    processor.deduct_click(data)
    
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(-1, result[0])
    
    cursor_data.execute('SELECT state FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertEqual(1, result[0])
    
    # process again, balance and state should be unchanged
    processor.deduct_click(data)
    
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(-1, result[0])
    
    cursor_data.execute('SELECT state FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertEqual(1, result[0])
    
  def test_process_queue(self):
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    
    # test click should be inserted onto deduct queue
    self.assertEqual(1, self.redis_data.llen('QUEUE_DEDUCT'))
    self.assertEqual(0, self.redis_data.llen('QUEUE_DEDUCT_PROCESSING'))
    self.assertEqual(message, self.redis_data.lindex('QUEUE_DEDUCT', -1))
    
    # process queue
    processor.process_queue()
    
    # both queues should now be empty
    self.assertEqual(0, self.redis_data.llen('QUEUE_DEDUCT'))
    self.assertEqual(0, self.redis_data.llen('QUEUE_DEDUCT_PROCESSING'))
    
if __name__ == '__main__':
  unittest.main()
