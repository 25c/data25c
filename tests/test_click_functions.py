from config import SETTINGS, pg_connect
from datetime import datetime
from decimal import Decimal
import isodate
import json
import redis
import unittest
import uuid

import api
import click

class TestClickFunctions(unittest.TestCase):
  
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
    
    # make sure the all test users start with 0 balance
    cursor = self.pg_web.cursor()
    cursor.execute("UPDATE users SET balance_paid=0, balance_free=100, total_given=0")
    
    # set up a revenue share on another button
    cursor.execute("SELECT id FROM users WHERE uuid=%s", ("439bdb807941012f5e2d60c5470a09c8",))
    result = cursor.fetchone()
    share_users = json.dumps([{'user':result[0],'share_amount':10}])
    cursor.execute("UPDATE buttons SET share_users=%s WHERE uuid=%s", (share_users, "92d1cdb0f60c012f5f3960c5470a09c8",))
    
    # clear redis
    self.redis_web.flushdb()
    self.redis_data.flushdb()
    
  def tearDown(self):
    self.pg_data.close()
    self.pg_web.close()
    self.redis_data.connection_pool.disconnect()
    self.redis_web.connection_pool.disconnect()
    
  def test_send_first_click_email(self):
    cursor_data = self.pg_data.cursor()
    cursor_web = self.pg_web.cursor()
    
    # insert a valid click
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    
    # assert ending balance and click presence
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0, 75, 25), result)
    cursor_data.execute('SELECT state, receiver_user_id, parent_click_id, amount FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertTupleEqual((1, 659867728, None, Decimal(25)), result)
    # assert that first click email was enqueued
    self.assertEqual(1, self.redis_web.llen('resque:queue:mailer'))
    data = json.loads(self.redis_web.lindex('resque:queue:mailer', 0))
    self.assertEqual(['new_user_FirstClick',568334,None], data['args'])
    
  def test_validate_click(self):    
    # invalid user_id
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"invaliduuid", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data['referrer_user_uuid'])
    self.assertIsNone(result)
    
    # invalid button_id
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"invaliduuid", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data['referrer_user_uuid'])
    self.assertIsNone(result)
    
    # user is owner of button- not allowed
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"9a7ba1b0dff9012f5efc60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data['referrer_user_uuid'])
    self.assertIsNone(result)
        
    # valid click, no referrer- should return user_id and button_id for uuids
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data['referrer_user_uuid'])
    self.assertTupleEqual((568334, None, 702273458, None, None, None, None, 659867728, 'mrjingles', None), result)
    
    # valid click, invalid referrer- should still return user_id and button_id
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":"invaliduuid", "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data['referrer_user_uuid'])
    self.assertTupleEqual((568334, None, 702273458, None, None, None, None, 659867728, 'mrjingles', None), result)
    
    # valid click, valid referrer- should return all ids
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "referrer_user_uuid":"4b7172007941012f5e2f60c5470a09c8", "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data['referrer_user_uuid'])
    self.assertTupleEqual((568334, None, 702273458, None, None, None, 755095536, 659867728, 'mrjingles', None), result)
    
    # valid click on button with share, no referrer, should also return share hash
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"92d1cdb0f60c012f5f3960c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data['referrer_user_uuid'])
    self.assertTupleEqual((568334, None, 749341768, None, None, None, None, 1005146552, 'thisisfrancis'), result[:-1])
    self.assertEqual(659867728, result[9][0]['user']) 
    self.assertEqual(10, result[9][0]['share_amount']) 
    
    # valid click, no referrer, with url- should return user_id and button_id for uuids, a new url_id for url
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "url":"http://localhost:3000/thisisfrancis", "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    result = click.validate_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data['referrer_user_uuid'])
    self.assertTupleEqual((568334, None, 702273458), result[0:3])
    self.assertIsNotNone(result[3])
    self.assertTupleEqual((None, None, None, 659867728, 'mrjingles', None), result[4:])
    
    
  def test_insert_click(self):
    cursor_data = self.pg_data.cursor()
    cursor_web = self.pg_web.cursor()
    
    # insert a valid click, with a url different than referrer url
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "url":"http://localhost:3000/about", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    
    # assert ending balance and click presence
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0,75,25), result)
    cursor_data.execute('SELECT state, receiver_user_id, parent_click_id, amount, amount_paid, amount_free FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertTupleEqual((1, 659867728, None, Decimal(25), Decimal(0), Decimal(25)), result)
    # should be a url_id assigned to the url now
    cursor_data.execute('SELECT url_id FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    url_id = result[0]
    self.assertIsNotNone(url_id)
    # should also be two requests to scrape url title (for url, for referrer)
    self.assertEqual(2, self.redis_data.llen('QUEUE_SCRAPER'))

    # insert dummy titles and clear the queue, as if the scraper had completed
    cursor_data.execute("UPDATE urls SET title=%s WHERE url=%s", ('Test Title', data['url']))
    cursor_data.execute("INSERT INTO urls (uuid, url, title, updated_at, created_at) VALUES (%s, %s, %s, %s, %s)", (uuid.uuid4().hex, data['referrer'], 'Title', datetime.now(), datetime.now()))
    self.redis_data.delete('QUEUE_SCRAPER')
    
    # try inserting again, should overwrite and end up with same result
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0,75,25), result)
    cursor_data.execute('SELECT state, receiver_user_id, parent_click_id, amount FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertTupleEqual((1, 659867728, None, Decimal(25)), result)
    
    # should no longer be inserting scrape requests...
    self.assertEqual(0, self.redis_data.llen('QUEUE_SCRAPER'))

    # try inserting with an earlier timestamp, should be ignored
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "url":"http://localhost:3000/about", "amount":0, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"2012-09-12T00:20:18.882Z"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0,75,25), result)
    cursor_data.execute('SELECT state, receiver_user_id, parent_click_id, amount FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertTupleEqual((1, 659867728, None, Decimal(25)), result)
    
    # try again with a later timestamp, should be applied- 0 amount effectively is an "undo", verify state change
    cursor_data.execute("SELECT state FROM clicks WHERE uuid=%s", ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertEqual(1, result[0])
    
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "url":"http://localhost:3000/about", "amount":0, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0,100,0), result)

    cursor_data.execute('SELECT state, receiver_user_id, parent_click_id, amount, amount_paid, amount_free FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertTupleEqual((5, 659867728, None, 0, 0, 0), result)
    
    # insert AGAIN, with positive amount, verify state change
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "url":"http://localhost:3000/about", "amount":50, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0, 50, 50), result)

    cursor_data.execute('SELECT state, receiver_user_id, parent_click_id, amount, amount_paid, amount_free FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    self.assertTupleEqual((1, 659867728, None, Decimal(50), Decimal(0), Decimal(50)), result)
    
    # insert a new click with a new uuid, but should still have the same url_id
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc0", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "url":"http://localhost:3000/about", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    # assert ending balance and click presence
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0, 25, 75), result)
    # should be the same url_id 
    cursor_data.execute('SELECT url_id FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc0",))
    result = cursor_data.fetchone()
    self.assertEqual(url_id, result[0])
    
    # insert a new click with a comment
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc1", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "url":"http://localhost:3000/about", "comment_uuid": "04516c50-0aa2-0130-6095-60c5470a09c8", "comment_text": "This is a test comment.", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    # assert ending balance and click presence
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0, 0, 100), result)
    # should be the same url_id 
    cursor_data.execute('SELECT url_id FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc1",))
    result = cursor_data.fetchone()
    self.assertEqual(url_id, result[0])
    # should now be a comment in the database
    cursor_data.execute('SELECT id, comment_id FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc1",))
    result = cursor_data.fetchone()
    click_id = result[0]    
    self.assertIsNotNone(result[1])    
    comment_id = result[1]
    cursor_data.execute('SELECT uuid, user_id, button_id, url_id, click_id, content FROM comments WHERE id=%s', (comment_id,))
    result = cursor_data.fetchone()    
    self.assertTupleEqual(("04516c50-0aa2-0130-6095-60c5470a09c8", 568334, 702273458, url_id, click_id, "This is a test comment."), result)
    
    # now update that same click with a new comment
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc1", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "url":"http://localhost:3000/about", "comment_uuid": "04516c50-0aa2-0130-6095-60c5470a09c8", "comment_text": "This is a modified test comment.", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    # assert ending balance and click presence
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0, 0, 100), result)
    cursor_data.execute('SELECT uuid, user_id, button_id, url_id, content FROM comments WHERE id=%s', (comment_id,))
    result = cursor_data.fetchone()    
    self.assertTupleEqual(("04516c50-0aa2-0130-6095-60c5470a09c8", 568334, 702273458, url_id, "This is a modified test comment."), result)
    
    # now insert a new tip on the same comment from a different user
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc2", "user_uuid":"4b7172007941012f5e2f60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "url":"http://localhost:3000/about", "comment_uuid": "04516c50-0aa2-0130-6095-60c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    # should be the same comment id
    cursor_data.execute('SELECT comment_id FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc2",))
    result = cursor_data.fetchone()    
    self.assertEqual(comment_id, result[0])
    
    # now try undoing the original comment click, which should cascade and undo the comment promotion click
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc1", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "url":"http://localhost:3000/about", "comment_uuid": "04516c50-0aa2-0130-6095-60c5470a09c8", "comment_text": "This is a modified test comment.", "amount":0, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    # assert ending balance and click presence for original click user
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0, 25, 75), result)
    # original click now 0
    cursor_data.execute('SELECT amount FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc1",))
    result = cursor_data.fetchone()    
    self.assertEqual(0, result[0])
    # and also promotion click
    cursor_data.execute('SELECT amount FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc2",))
    result = cursor_data.fetchone()    
    self.assertEqual(0, result[0])
    
    # try updating click with amount beyond balance, should be dropped
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc1", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "url":"http://localhost:3000/about", "comment_uuid": "04516c50-0aa2-0130-6095-60c5470a09c8", "comment_text": "This is a modified test comment.", "amount":50, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    # assert same ending balance and click presence for original click user
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0, 25, 75), result)
    # original click still 0
    cursor_data.execute('SELECT amount FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc1",))
    result = cursor_data.fetchone()    
    self.assertEqual(0, result[0])
    
    # try inserting a new click with amount beyond balance, should also be dropped
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc3", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "url":"http://localhost:3000/about", "comment_uuid": "04516c50-0aa2-0130-6095-60c5470a09c8", "comment_text": "This is a modified test comment.", "amount":50, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    # assert same ending balance and click presence for original click user
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0, 25, 75), result)
    # click should not have been inserted
    cursor_data.execute('SELECT amount FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc3",))
    result = cursor_data.fetchone()    
    self.assertIsNone(result)
    
    # give the user some paid balance
    cursor_web.execute("UPDATE users SET balance_paid=100 WHERE LOWER(uuid)=LOWER(%s)", ("3dd80d107941012f5e2c60c5470a09c8",))
    
    # now try inserting new click again
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc3", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "url":"http://localhost:3000/about", "comment_uuid": "04516c50-0aa2-0130-6095-60c5470a09c8", "comment_text": "This is a modified test comment.", "amount":50, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    # assert new ending balance and click presence for user
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((75, 0, 125), result)
    # click should now have been inserted
    cursor_data.execute('SELECT amount, amount_paid, amount_free FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc3",))
    result = cursor_data.fetchone()    
    self.assertEqual((Decimal(50), Decimal(25), Decimal(25)), result)
    
    # now try updating the click, verify paid/free amounts change accordingly
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc3", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"a4b16a40dff9012f5efd60c5470a09c8", "url":"http://localhost:3000/about", "comment_uuid": "04516c50-0aa2-0130-6095-60c5470a09c8", "comment_text": "This is a modified test comment.", "amount":20, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    # assert new ending balance and click presence for user
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((100, 5, 95), result)
    # click should now have been inserted
    cursor_data.execute('SELECT amount, amount_paid, amount_free FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc3",))
    result = cursor_data.fetchone()    
    self.assertEqual((Decimal(20), Decimal(0), Decimal(20)), result)
    
  def test_insert_and_update_click_with_share(self):
    cursor_data = self.pg_data.cursor()
    cursor_web = self.pg_web.cursor()
    
    # insert a valid click
    message = '{"uuid":"a2afb8a0-fc6f-11e1-b984-eff95004abc9", "user_uuid":"3dd80d107941012f5e2c60c5470a09c8", "button_uuid":"92d1cdb0f60c012f5f3960c5470a09c8", "amount":25, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    
    # assert ending balance and click presence
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0,75,25), result)
    cursor_data.execute('SELECT id, state, receiver_user_id, parent_click_id, amount, amount_paid, amount_free, share_users FROM clicks WHERE uuid=%s', ("a2afb8a0-fc6f-11e1-b984-eff95004abc9",))
    result = cursor_data.fetchone()
    click_id = result[0]
    self.assertTupleEqual((click_id, 1, None, None, Decimal(25), Decimal(0), Decimal(25)), result[:-1])
    self.assertIsNotNone(result[5])
    
    # should also be two "child" click objects, representing the split
    cursor_data.execute('SELECT COUNT(*) FROM clicks WHERE parent_click_id=%s', (click_id,))
    result = cursor_data.fetchone()
    self.assertEqual(2, result[0])
    
    # one for the share recipient
    cursor_data.execute('SELECT state, amount, amount_paid, amount_free FROM clicks WHERE parent_click_id=%s AND receiver_user_id=%s', (click_id, 659867728))
    result = cursor_data.fetchone()
    self.assertTupleEqual((1, Decimal(2.5), Decimal(0), Decimal(2.5)), result)
    
    # one for the button owner
    cursor_data.execute('SELECT state, amount, amount_paid, amount_free FROM clicks WHERE parent_click_id=%s AND receiver_user_id=%s', (click_id, 1005146552))
    result = cursor_data.fetchone()
    self.assertTupleEqual((1,Decimal(22.5), Decimal(0), Decimal(22.5)), result)
    
    # now try "undoing" the click 
    click.undo_click(data['uuid'])
    
    # assert ending balance and click presence
    cursor_web.execute('SELECT balance_paid, balance_free, total_given FROM users WHERE uuid=%s', ("3dd80d107941012f5e2c60c5470a09c8",))
    result = cursor_web.fetchone()
    self.assertEqual((0,100,0), result)
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
    cursor_data.execute('SELECT state, amount, amount_paid, amount_free FROM clicks WHERE parent_click_id=%s AND receiver_user_id=%s', (click_id, 659867728))
    result = cursor_data.fetchone()
    self.assertTupleEqual((5, 0, 0, 0), result)
    
    # one for the button owner
    cursor_data.execute('SELECT state, amount, amount_paid, amount_free FROM clicks WHERE parent_click_id=%s AND receiver_user_id=%s', (click_id, 1005146552))
    result = cursor_data.fetchone()
    self.assertTupleEqual((5, 0, 0, 0), result)
    
if __name__ == '__main__':
  unittest.main()
