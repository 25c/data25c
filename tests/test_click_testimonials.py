from config import SETTINGS, pg_connect
from datetime import datetime
import isodate
import json
import redis
import unittest
import uuid

import api
import click

class TestClickTestimonials(unittest.TestCase):
  
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
      
    # give the users some generous balance
    cursor = self.pg_web.cursor()
    cursor.execute("UPDATE users SET balance_paid=3000, balance_free=500, total_given=0")
    
    # clear redis
    self.redis_web.flushdb()
    self.redis_data.flushdb()
    
  def tearDown(self):
    self.pg_data.close()
    self.pg_web.close()
    self.redis_data.connection_pool.disconnect()
    self.redis_web.connection_pool.disconnect()

  def test_testimonials(self):
    # insert a $10 tip with comment from user 001
    message = '{"uuid":"43af5340-1b0c-0130-60aa-60c5470a09c8", "user_uuid":"69bbbf501b0a013060a560c5470a09c8", "button_uuid":"2bbded101b00013060a060c5470a09c8", "url":"http://localhost:3000/thisisfrancis", "comment_uuid":"8b5a3850-1b0c-0130-60ab-60c5470a09c8", "comment_text":"Comment from user_001", "referrer":"http://localhost:3000/thisisfrancis", "amount":1000, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data.get('comment_pseudonym', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    # emails- should be a "first click" email, and a new position email
    self.assertEqual(2, self.redis_web.llen('resque:queue:mailer'))
    data = json.loads(self.redis_web.lindex('resque:queue:mailer', -2))
    self.assertEqual('new_user_FirstClick', data['args'][0])
    data = json.loads(self.redis_web.lindex('resque:queue:mailer', -1))
    self.assertEqual(['new_position_in_testimonial','8b5a3850-1b0c-0130-60ab-60c5470a09c8', -1, 1], data['args'])
    
    # insert a $15 tip with comment from user 002
    message = '{"uuid":"5c56a960-1b11-0130-60ae-60c5470a09c8", "user_uuid":"c8f46e501b0b013060a660c5470a09c8", "button_uuid":"2bbded101b00013060a060c5470a09c8", "url":"http://localhost:3000/thisisfrancis", "comment_uuid":"3946dd60-1b11-0130-60ad-60c5470a09c8", "comment_text":"Comment from user_002", "referrer":"http://localhost:3000/thisisfrancis", "amount":1500, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data.get('comment_pseudonym', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    # emails- should be new position emails for both comments
    self.assertEqual(5, self.redis_web.llen('resque:queue:mailer'))
    data = json.loads(self.redis_web.lindex('resque:queue:mailer', -2))
    self.assertEqual(['new_position_in_testimonial','3946dd60-1b11-0130-60ad-60c5470a09c8', -1, 1], data['args'])
    data = json.loads(self.redis_web.lindex('resque:queue:mailer', -1))
    self.assertEqual(['new_position_in_testimonial','8b5a3850-1b0c-0130-60ab-60c5470a09c8', 1, 2], data['args'])
    
    # insert a $15 tip with comment from user 003
    message = '{"uuid":"7c7d5310-1b13-0130-60af-60c5470a09c8", "user_uuid":"cd3797201b0b013060a760c5470a09c8", "button_uuid":"2bbded101b00013060a060c5470a09c8", "url":"http://localhost:3000/thisisfrancis", "comment_uuid":"815e21d0-1b13-0130-60b0-60c5470a09c8", "comment_text":"Comment from user_003", "referrer":"http://localhost:3000/thisisfrancis", "amount":1500, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data.get('comment_pseudonym', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))    
    cache = self.redis_data.get('2bbded101b00013060a060c5470a09c8:http://localhost:3000/thisisfrancis')
    data = json.loads(cache)
    self.assertEqual(3, len(data))
    self.assertEqual("3946dd60-1b11-0130-60ad-60c5470a09c8", data[0]['uuid'])
    self.assertEqual("815e21d0-1b13-0130-60b0-60c5470a09c8", data[1]['uuid'])
    self.assertEqual("8b5a3850-1b0c-0130-60ab-60c5470a09c8", data[2]['uuid'])
    # emails- should be new position emails for displaced comment and new comment
    self.assertEqual(8, self.redis_web.llen('resque:queue:mailer'))
    data = json.loads(self.redis_web.lindex('resque:queue:mailer', -2))
    self.assertEqual(['new_position_in_testimonial','815e21d0-1b13-0130-60b0-60c5470a09c8', -1, 2], data['args'])
    data = json.loads(self.redis_web.lindex('resque:queue:mailer', -1))
    self.assertEqual(['new_position_in_testimonial','8b5a3850-1b0c-0130-60ab-60c5470a09c8', 2, 3], data['args'])
    
    # insert a $5 tip promotion on user 003's comment by user 004
    message = '{"uuid":"ddeaa750-1b1b-0130-60b1-60c5470a09c8", "user_uuid":"d09a5df01b0b013060a860c5470a09c8", "button_uuid":"2bbded101b00013060a060c5470a09c8", "url":"http://localhost:3000/thisisfrancis", "comment_uuid":"815e21d0-1b13-0130-60b0-60c5470a09c8", "referrer":"http://localhost:3000/thisisfrancis", "amount":500, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data.get('comment_pseudonym', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    cache = self.redis_data.get('2bbded101b00013060a060c5470a09c8:http://localhost:3000/thisisfrancis')
    data = json.loads(cache)
    self.assertEqual(3, len(data))
    self.assertEqual("815e21d0-1b13-0130-60b0-60c5470a09c8", data[0]['uuid'])
    self.assertEqual(2000, data[0]['amount'])
    self.assertEqual(1, len(data[0]['promoters']))
    self.assertEqual("d09a5df01b0b013060a860c5470a09c8", data[0]['promoters'][0]['uuid'])    
    self.assertEqual("3946dd60-1b11-0130-60ad-60c5470a09c8", data[1]['uuid'])
    self.assertEqual("8b5a3850-1b0c-0130-60ab-60c5470a09c8", data[2]['uuid'])
    # emails- should be new position emails for displaced comments
    self.assertEqual(12, self.redis_web.llen('resque:queue:mailer'))
    data = json.loads(self.redis_web.lindex('resque:queue:mailer', -3))
    self.assertEqual(['new_position_in_testimonial','815e21d0-1b13-0130-60b0-60c5470a09c8', 2, 1], data['args'])
    data = json.loads(self.redis_web.lindex('resque:queue:mailer', -2))
    self.assertEqual(['new_position_in_testimonial','3946dd60-1b11-0130-60ad-60c5470a09c8', 1, 2], data['args'])
    data = json.loads(self.redis_web.lindex('resque:queue:mailer', -1))
    self.assertEqual('testimonial_promoted', data['args'][0])
    
    # insert a $5 tip with comment by user 004
    message = '{"uuid":"98b49430-1b21-0130-60b2-60c5470a09c8", "user_uuid":"d09a5df01b0b013060a860c5470a09c8", "button_uuid":"2bbded101b00013060a060c5470a09c8", "url":"http://localhost:3000/thisisfrancis", "comment_uuid":"9f0d5780-1b21-0130-60b3-60c5470a09c8", "comment_text":"Comment from user_004", "referrer":"http://localhost:3000/thisisfrancis", "amount":500, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data.get('comment_pseudonym', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    cache = self.redis_data.get('2bbded101b00013060a060c5470a09c8:http://localhost:3000/thisisfrancis')
    data = json.loads(cache)
    self.assertEqual(4, len(data))
    self.assertEqual("815e21d0-1b13-0130-60b0-60c5470a09c8", data[0]['uuid'])
    self.assertEqual("3946dd60-1b11-0130-60ad-60c5470a09c8", data[1]['uuid'])
    self.assertEqual("8b5a3850-1b0c-0130-60ab-60c5470a09c8", data[2]['uuid'])
    self.assertEqual("9f0d5780-1b21-0130-60b3-60c5470a09c8", data[3]['uuid'])
    # emails- should be new position emails for new comment
    self.assertEqual(14, self.redis_web.llen('resque:queue:mailer'))
    data = json.loads(self.redis_web.lindex('resque:queue:mailer', -1))
    self.assertEqual(['new_position_in_testimonial','9f0d5780-1b21-0130-60b3-60c5470a09c8', -1, 4], data['args'])    
    
    # insert a $25 pseudonymous tip with comment by user 004
    message = '{"uuid":"277c78a0-2bab-0130-60f6-60c5470a09c8", "user_uuid":"d09a5df01b0b013060a860c5470a09c8", "button_uuid":"2bbded101b00013060a060c5470a09c8", "url":"http://localhost:3000/thisisfrancis", "comment_uuid":"2df62260-2bab-0130-60f7-60c5470a09c8", "comment_text":"Pseudonymous comment from user_004", "comment_pseudonym":"a big fan", "referrer":"http://localhost:3000/thisisfrancis", "amount":2500, "referrer_user_uuid":null, "referrer":"http://localhost:3000/thisisfrancis", "user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1", "ip_address":"127.0.0.1", "created_at":"'+datetime.utcnow().isoformat()+'"}'
    data = json.loads(message)
    click.insert_click(data['uuid'], data['user_uuid'], data['button_uuid'], data.get('url', None), data.get('comment_uuid', None), data.get('comment_text', None), data.get('comment_pseudonym', None), data['referrer_user_uuid'], data['amount'], data['ip_address'], data['user_agent'], data['referrer'], isodate.parse_datetime(data['created_at']))
    cache = self.redis_data.get('2bbded101b00013060a060c5470a09c8:http://localhost:3000/thisisfrancis')
    data = json.loads(cache)
    self.assertEqual(5, len(data))
    self.assertEqual("2df62260-2bab-0130-60f7-60c5470a09c8", data[0]['uuid'])
    self.assertEqual("a big fan", data[0]['owner']['name'])
    self.assertIsNone(data[0]['owner']['uuid'])
    self.assertEqual("815e21d0-1b13-0130-60b0-60c5470a09c8", data[1]['uuid'])
    self.assertEqual("3946dd60-1b11-0130-60ad-60c5470a09c8", data[2]['uuid'])
    self.assertEqual("8b5a3850-1b0c-0130-60ab-60c5470a09c8", data[3]['uuid'])
    self.assertEqual("9f0d5780-1b21-0130-60b3-60c5470a09c8", data[4]['uuid'])
