from config import SETTINGS
import redis
import unittest
import monitor

class TestMonitorFunctions(unittest.TestCase):
    
  def setUp(self):    
    self.redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])
    # clear the redis queues
    self.redis_data.delete('QUEUE')
    self.redis_data.delete('QUEUE_PROCESSING')
    
  def test_process_queue(self):    
    # insert a valid click into processing queue
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    self.redis_data.lpush('QUEUE_PROCESSING', message)
    
    # assert starting state of queues
    self.assertEqual(1, self.redis_data.llen('QUEUE_PROCESSING'))
    self.assertEqual(message, self.redis_data.lindex('QUEUE_PROCESSING', -1))
    self.assertEqual(0, self.redis_data.llen('QUEUE'))
    
    # simulate starting first pass over queue with None previous message
    result = monitor.process_queue(None, 'QUEUE', 'QUEUE_PROCESSING')
    # should return the message at the head of the queue
    self.assertEqual(message, result)
    # queues should be the same
    self.assertEqual(1, self.redis_data.llen('QUEUE_PROCESSING'))
    self.assertEqual(message, self.redis_data.lindex('QUEUE_PROCESSING', -1))
    self.assertEqual(0, self.redis_data.llen('QUEUE'))
    
    # now process again with the message
    result = monitor.process_queue(result, 'QUEUE', 'QUEUE_PROCESSING')
    # return value should be new head of processing queue (in this case None)
    self.assertIsNone(result)
    # message should be removed from processing queue and back in main queue
    self.assertEqual(0, self.redis_data.llen('QUEUE_PROCESSING'))
    self.assertEqual(1, self.redis_data.llen('QUEUE'))
    self.assertEqual(message, self.redis_data.lindex('QUEUE', -1))
