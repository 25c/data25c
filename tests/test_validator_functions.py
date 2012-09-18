from config import SETTINGS, pg_connect
import json
import redis
import unittest
import validator

class TestValidatorFunctions(unittest.TestCase):
  
  pg_data = pg_connect(SETTINGS['DATABASE_URL'])
  redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])
  
  def setUp(self):
    # turn on autocommit
    self.pg_data.autocommit = True
    
    # always start with an empty click database
    cursor = self.pg_data.cursor()
    cursor.execute('DELETE FROM clicks;')
    cursor.close()
    
    # clear the redis queues
    self.redis_data.delete('QUEUE')
    self.redis_data.delete('QUEUE_PROCESSING')
    self.redis_data.delete('QUEUE_DEDUCT')
    self.redis_data.delete('QUEUE_DEDUCT_PROCESSING')
  
  def test_validate_click(self):    
    # invalid user_id
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"invaliduuid", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    result = validator.validate_click(data)
    self.assertIsNone(result)
    
    # invalid button_id
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"invaliduuid", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    result = validator.validate_click(data)
    self.assertIsNone(result)
    
    # user is owner of button- not allowed
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"9a7ba1b0dff9012f5efc60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    result = validator.validate_click(data)
    self.assertIsNone(result)
        
    # valid click, no referrer- should return user_id and button_id for uuids
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    result = validator.validate_click(data)
    self.assertTupleEqual((568334, 702273458, None), result)
    
    # valid click, invalid referrer- should still return user_id and button_id
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":"invaliduuid", "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    result = validator.validate_click(data)
    self.assertTupleEqual((568334, 702273458, None), result)
    
    # valid click, valid referrer- should return all ids
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":"4b7172007941012f5e2f60c5470a09c8", "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    result = validator.validate_click(data)
    self.assertTupleEqual((568334, 702273458, 755095536), result)
    
  def test_process_message(self):
    cursor = self.pg_data.cursor()
    # assert start with empty database
    cursor.execute('SELECT COUNT(*) FROM clicks;');
    result = cursor.fetchone()
    self.assertEqual(0, result[0])
    
    # valid click, no referrer- should insert all relevant data
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    validator.process_message(message)
    cursor.execute('SELECT COUNT(*) FROM clicks;');
    result = cursor.fetchone()
    self.assertEqual(1, result[0])
    
    data = json.loads(message)
    cursor.execute('SELECT uuid, referrer, user_agent, ip_address FROM clicks;');
    result = cursor.fetchone()
    self.assertEqual((data['uuid'], data['referrer'], data['user_agent'], data['ip_address']), result)
    
    # duplicate insert should be dropped
    validator.process_message(message)
    cursor.execute('SELECT COUNT(*) FROM clicks;');
    result = cursor.fetchone()
    self.assertEqual(1, result[0])
    
    # unparseable message should be dropped
    validator.process_message('{invalid syntax!]')
    cursor.execute('SELECT COUNT(*) FROM clicks;');
    result = cursor.fetchone()
    self.assertEqual(1, result[0])
    
  def test_process_queue(self):
    cursor = self.pg_data.cursor()
    
    # assert start with empty database
    cursor.execute('SELECT COUNT(*) FROM clicks;');
    result = cursor.fetchone()
    self.assertEqual(0, result[0])
    
    # valid click, no referrer- should insert all relevant data
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    self.redis_data.lpush('QUEUE', message)
    self.assertEqual(1, self.redis_data.llen('QUEUE'))
    
    # process the queue
    validator.process_queue()
    
    # assert inserted
    cursor.execute('SELECT COUNT(*) FROM clicks;');
    result = cursor.fetchone()
    self.assertEqual(1, result[0])
    
    data = json.loads(message)
    cursor.execute('SELECT uuid, referrer, user_agent, ip_address FROM clicks;');
    result = cursor.fetchone()
    self.assertEqual((data['uuid'], data['referrer'], data['user_agent'], data['ip_address']), result)
    
    # initial queues should now be empty
    self.assertEqual(0, self.redis_data.llen('QUEUE'))
    self.assertEqual(0, self.redis_data.llen('QUEUE_PROCESSING'))
    # and now inserted onto deduct queue
    self.assertEqual(1, self.redis_data.llen('QUEUE_DEDUCT'))
    self.assertEqual(message, self.redis_data.lindex('QUEUE_DEDUCT', -1))
    
if __name__ == '__main__':
  unittest.main()
