from config import SETTINGS, pg_connect
from datetime import datetime
import isodate
import json
import redis
import unittest

import api
import click

class TestClickFunctions(unittest.TestCase):
  
  def setUp(self):
    self.pg_web = pg_connect(SETTINGS['DATABASE_WEB_URL'])
    self.pg_data = pg_connect(SETTINGS['DATABASE_URL'])
    self.redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])
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
    
    # set up a revenue share on another button
    cursor.execute("SELECT id FROM users WHERE uuid=%s", ("439bdb807941012f5e2d60c5470a09c8",))
    result = cursor.fetchone()
    share_users = json.dumps([{'user':result[0],'share_amount':10}])
    cursor.execute("UPDATE buttons SET share_users=%s WHERE uuid=%s", (share_users, "92d1cdb0f60c012f5f3960c5470a09c8",))
    
  def tearDown(self):
    self.pg_data.close()
    self.pg_web.close()
    self.redis_data.connection_pool.disconnect()
    
  def test_validate_click(self):    
    # invalid user_id
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"invaliduuid", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data['referrer_user_uuid'])
    self.assertIsNone(result)
    
    # invalid button_id
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"invaliduuid", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data['referrer_user_uuid'])
    self.assertIsNone(result)
    
    # user is owner of button- not allowed
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"9a7ba1b0dff9012f5efc60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data['referrer_user_uuid'])
    self.assertIsNone(result)
        
    # valid click, no referrer- should return user_id and button_id for uuids
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data['referrer_user_uuid'])
    self.assertTupleEqual((568334, 702273458, None, 659867728, None), result)
    
    # valid click, invalid referrer- should still return user_id and button_id
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":"invaliduuid", "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data['referrer_user_uuid'])
    self.assertTupleEqual((568334, 702273458, None, 659867728, None), result)
    
    # valid click, valid referrer- should return all ids
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":"4b7172007941012f5e2f60c5470a09c8", "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data['referrer_user_uuid'])
    self.assertTupleEqual((568334, 702273458, 755095536, 659867728, None), result)
    
    # valid click on button with share, no referrer, should also return share hash
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"92d1cdb0f60c012f5f3960c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data['referrer_user_uuid'])
    self.assertTupleEqual((568334, 749341768, None, 1005146552), result[:-1])
    self.assertIsNotNone(result[4])
    self.assertEqual(659867728, result[4][0]['user']) 
    self.assertEqual(10, result[4][0]['share_amount']) 
    
  def test_insert_click(self):
    cursor_data = self.pg_data.cursor()
    cursor_web = self.pg_web.cursor()
    
    # assert starting balance
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(0, result[0])
    self.assertEqual(0, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))
    
    # insert a valid click
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    ids = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data['referrer_user_uuid'])
    click.insert_click(data['uuid'], ids[0], ids[1], ids[2], ids[3], ids[4], data['amount']*1000000, data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    
    # assert ending balance and click presence
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(25000000, result[0])
    self.assertEqual(25000000, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))    
    cursor_data.execute('SELECT state, receiver_user_id, parent_click_id, amount FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertTupleEqual((1, ids[3], None, 25000000), result)
    
    # try inserting again, should overwrite and end up with same result
    click.insert_click(data['uuid'], ids[0], ids[1], ids[2], ids[3], ids[4], data['amount']*1000000, data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(25000000, result[0])
    self.assertEqual(25000000, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))    
    cursor_data.execute('SELECT state, receiver_user_id, parent_click_id, amount FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertTupleEqual((1, ids[3], None, 25000000), result)
    
    # try inserting with an earlier timestamp, should be ignored
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":0, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:18.882Z"}'
    data = json.loads(message)
    ids = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data['referrer_user_uuid'])
    click.insert_click(data['uuid'], ids[0], ids[1], ids[2], ids[3], ids[4], data['amount']*1000000, data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(25000000, result[0])
    self.assertEqual(25000000, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))    
    cursor_data.execute('SELECT state, receiver_user_id, parent_click_id, amount FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertTupleEqual((1, ids[3], None, 25000000), result)
    
    # try again with a later timestamp, should be applied- 0 amount effectively is an "undo", verify state change
    cursor_data.execute("SELECT state FROM clicks WHERE uuid=%s", ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertEqual(1, result[0])
    
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":0, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:20.882Z"}'
    data = json.loads(message)
    ids = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data['referrer_user_uuid'])
    click.insert_click(data['uuid'], ids[0], ids[1], ids[2], ids[3], ids[4], data['amount']*1000000, data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(0, result[0])
    self.assertEqual(0, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))    

    cursor_data.execute('SELECT state, receiver_user_id, parent_click_id, amount FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertTupleEqual((5, ids[3], None, 0), result)
    
    # insert AGAIN, with positive amount, verify state change
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":1234, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:22.882Z"}'
    data = json.loads(message)
    ids = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data['referrer_user_uuid'])
    click.insert_click(data['uuid'], ids[0], ids[1], ids[2], ids[3], ids[4], data['amount']*1000000, data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(1234000000, result[0])
    self.assertEqual(1234000000, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))    

    cursor_data.execute('SELECT state, receiver_user_id, parent_click_id, amount FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertTupleEqual((1, ids[3], None, 1234000000), result)
    
  def test_insert_and_update_click_with_share(self):
    cursor_data = self.pg_data.cursor()
    cursor_web = self.pg_web.cursor()
    
    # assert starting balance
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(0, result[0])
    self.assertEqual(0, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))
    
    # insert a valid click
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"92d1cdb0f60c012f5f3960c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:19.882Z"}'
    data = json.loads(message)
    ids = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data['referrer_user_uuid'])
    click.insert_click(data['uuid'], ids[0], ids[1], ids[2], ids[3], ids[4], data['amount']*1000000, data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    
    # assert ending balance and click presence
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(25000000, result[0])
    self.assertEqual(25000000, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))
    cursor_data.execute('SELECT id, state, receiver_user_id, parent_click_id, amount, share_users FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    click_id = result[0]
    self.assertTupleEqual((click_id, 1, None, None, 25000000), result[:-1])
    self.assertIsNotNone(result[5])
    
    # should also be two "child" click objects, representing the split
    cursor_data.execute('SELECT COUNT(*) FROM clicks WHERE parent_click_id=%s', (click_id,))
    result = cursor_data.fetchone()
    self.assertEqual(2, result[0])
    
    # one for the share recipient
    cursor_data.execute('SELECT state, amount FROM clicks WHERE parent_click_id=%s AND receiver_user_id=%s', (click_id, 659867728))
    result = cursor_data.fetchone()
    self.assertTupleEqual((1, 2500000), result)
    
    # one for the button owner
    cursor_data.execute('SELECT state, amount FROM clicks WHERE parent_click_id=%s AND receiver_user_id=%s', (click_id, 1005146552))
    result = cursor_data.fetchone()
    self.assertTupleEqual((1, 22500000), result)
    
    # now try "undoing" the click by updating it with a 0 amount
    click.update_click(data['uuid'], None, 0, datetime.now())
    
    # assert ending balance and click presence
    cursor_web.execute('SELECT balance FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual(0, result[0])
    self.assertEqual(0, int(self.redis_data.get('user:3dd80d107941012f5e2c60c5470a09c8')))
    cursor_data.execute('SELECT id, state, receiver_user_id, parent_click_id, amount, share_users FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    click_id = result[0]
    self.assertTupleEqual((click_id, 5, None, None,0), result[:-1])
    self.assertIsNotNone(result[5])
    
    # should also be two "child" click objects, representing the split
    cursor_data.execute('SELECT COUNT(*) FROM clicks WHERE parent_click_id=%s', (click_id,))
    result = cursor_data.fetchone()
    self.assertEqual(2, result[0])
    
    # one for the share recipient
    cursor_data.execute('SELECT state, amount FROM clicks WHERE parent_click_id=%s AND receiver_user_id=%s', (click_id, 659867728))
    result = cursor_data.fetchone()
    self.assertTupleEqual((5, 0), result)
    
    # one for the button owner
    cursor_data.execute('SELECT state, amount FROM clicks WHERE parent_click_id=%s AND receiver_user_id=%s', (click_id, 1005146552))
    result = cursor_data.fetchone()
    self.assertTupleEqual((5, 0), result)
    
if __name__ == '__main__':
  unittest.main()
