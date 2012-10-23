from airbrakepy.logging.handlers import AirbrakeHandler
from config import SETTINGS, pg_connect
from datetime import datetime

import click
import isodate
import json
import logging
import lxml.html
import psycopg2
import redis
import sys
import urllib2

# initialize logger
logging.basicConfig()
logger = logging.getLogger("scraper")
if SETTINGS['PYTHON_ENV'] == 'development' or SETTINGS['PYTHON_ENV'] == 'test':
  logger.setLevel(logging.DEBUG)
else:
  logger.setLevel(logging.INFO)
  handler = AirbrakeHandler(SETTINGS['AIRBRAKE_API_KEY'], environment=SETTINGS['PYTHON_ENV'], component_name='scraper', node_name='data25c')
  handler.setLevel(logging.ERROR)
  logger.addHandler(handler)
  
# initialize redis connection
redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])
    
class Scraper:
  def __init__(self, dom):
    self.dom = dom
    
  def title(self):
    for element in self.dom.xpath('//html/head/title'):
      return element.text
    
def scrape(url):
  try:
    # open url
    html = urllib2.urlopen(url).read()
  except:
    logger.exception("%s: unable to download", url)
    # TODO re-enqueue for retry
    return
    
  try:
    dom = lxml.html.fromstring(html)
    return Scraper(dom)
  except:
    logger.exception('%s: unable to scrape html', url)

def process_message(message):
  try:
    # parse JSON data
    data = json.loads(message)
  except ValueError:
    logger.warn('%s: unparseable message=%s', message)
    return
  
  try:
    logger.info("%s: scraping...", data['url'])
    scraper = scrape(data['url'])
    if scraper is not None:
      click.insert_title(data['url'], scraper.title())
      logger.info("%s: %s", data['url'], scraper.title())
  except:
    logger.exception('%s: unexpected exception', data['url'])
        
def process_queue():
  # block and wait for click data, pushing into processing queue
  message = redis_data.brpop('QUEUE_SCRAPER', 0)
  # process message
  process_message(message[1])
  
def rescrape_all():
  try:
    pg_data = pg_connect(SETTINGS['DATABASE_URL'])
    cursor = pg_data.cursor()
    cursor.execute("SELECT DISTINCT(referrer) FROM clicks")
    for row in cursor:
      redis_data.lpush('QUEUE_SCRAPER', json.dumps({ 'url': row[0] }))
      logger.info("Enqueueing: %s", row[0])
  except:
    logger.exception('Unexpected exception re-enqueing referrer urls for scraping')
  finally:
    cursor.close()
    pg_data.commit()
    
if __name__ == '__main__':
  logger.info("Starting scraper...")
  while True:
    process_queue()
    