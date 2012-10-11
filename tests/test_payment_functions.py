from config import SETTINGS, pg_connect
from datetime import datetime
import isodate
import json
import redis
import unittest
import uuid

import click
import payment
import processor

class TestPaymentFunctions(unittest.TestCase):
  
  def setUp(self):
    self.pg_web = pg_connect(SETTINGS['DATABASE_WEB_URL'])
    self.pg_data = pg_connect(SETTINGS['DATABASE_URL'])
    self.redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])
    
    # turn on autocommit
    self.pg_data.autocommit = True
    self.pg_web.autocommit = True
    
    # always start with an empty click and payment database
    cursor = self.pg_data.cursor()
    cursor.execute('DELETE FROM clicks;')
    cursor.close()

    cursor = self.pg_web.cursor()
    cursor.execute('DELETE FROM payments;')
    
    # make sure the test click user starts with 0 balance
    cursor.execute("SELECT id FROM users WHERE uuid=%s", ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor.fetchone()
    self.user_id = result[0]
    cursor.execute("UPDATE users SET balance=0 WHERE uuid=%s", ("3dd80d107941012f5e2c60c5470a09c8",))
    self.redis_data.set('user:3dd80d107941012f5e2c60c5470a09c8', 0)
    
    # now insert a series of test clicks
    for i in range(50):
      message = '{"uuid":"' + str(uuid.uuid4()) + '", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
      processor.process_message(message)
      
    # and a payment for these clicks
    cursor.execute("INSERT INTO payments (uuid, user_id, amount, payment_type, updated_at, created_at) VALUES (%s, %s, %s, %s, %s, %s)", ('5698bd9c-7406-4a2c-854c-5943c017c944', self.user_id, 1250000000, 'payin', datetime.utcnow(), datetime.utcnow()))
    cursor.close()
    
  def tearDown(self):
    self.pg_data.close()
    self.pg_web.close()
    self.redis_data.connection_pool.disconnect()
    
  def test_process_payment(self):
    web_cursor = self.pg_web.cursor()
    data_cursor = self.pg_data.cursor()
    
    # assert starting states- payment amount, balance, state of clicks
    web_cursor.execute("SELECT state, amount FROM payments WHERE uuid=%s", ('5698bd9c-7406-4a2c-854c-5943c017c944',))
    result = web_cursor.fetchone()
    self.assertEqual(0, result[0])
    self.assertEqual(1250000000, result[1])
    
    web_cursor.execute("SELECT balance FROM users WHERE id=%s", (self.user_id,))
    result = web_cursor.fetchone()
    self.assertEqual(1250000000, result[0])
    self.assertEqual(1250000000, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))
    
    data_cursor.execute("SELECT COUNT(*) FROM clicks WHERE state=1 AND funded_at IS NULL AND user_id=%s", (self.user_id,))
    result = data_cursor.fetchone()
    self.assertEqual(50, result[0])
    
    # process payment
    payment.process_payment('5698bd9c-7406-4a2c-854c-5943c017c944')
    
    # assert ending states
    web_cursor.execute("SELECT state FROM payments WHERE uuid=%s", ('5698bd9c-7406-4a2c-854c-5943c017c944',))
    result = web_cursor.fetchone()
    self.assertEqual(2, result[0])
    
    web_cursor.execute("SELECT balance FROM users WHERE id=%s", (self.user_id,))
    result = web_cursor.fetchone()
    self.assertEqual(0, result[0])
    self.assertEqual(0, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))
    
    data_cursor.execute("SELECT COUNT(*) FROM clicks WHERE state=2 AND funded_at IS NOT NULL AND user_id=%s", (self.user_id,))
    result = data_cursor.fetchone()
    self.assertEqual(50, result[0])
    
    
if __name__ == '__main__':
  unittest.main()