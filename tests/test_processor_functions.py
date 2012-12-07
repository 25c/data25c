from config import SETTINGS, pg_connect
from datetime import datetime
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
    
    # make sure the test click users start with 0 balance
    cursor = self.pg_web.cursor()
    cursor.execute("UPDATE users SET balance_paid=0,balance_free=100,total_given=0")
    
    # clear the redis queues
    self.redis_data.delete('QUEUE')
    self.redis_data.delete('QUEUE_PROCESSING')
    
  def tearDown(self):
    self.pg_data.close()
    self.pg_web.close()
    self.redis_data.connection_pool.disconnect()
  
  def test_process_message(self):
    cursor_web = self.pg_web.cursor()
    cursor_data = self.pg_data.cursor()
    
    # invalid user_id, should be dropped
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"invaliduuid", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    processor.process_message(message)
    
    # balance still 0 at this point
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0, 100, 0), result)
      
    # valid message, balance should be deducted and state changed    
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    processor.process_message(message)
    
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0, 75, 25), result)
    
    # process again, balance and state should be unchanged
    processor.process_message(message)
    
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0, 75, 25), result)
    
    # process a new message that changes the amount
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":50, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    processor.process_message(message)
    
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0, 50, 50), result)
  
    
  def test_process_queue(self):
    # insert a valid click
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
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
