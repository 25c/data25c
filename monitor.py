from airbrakepy.logging.handlers import AirbrakeHandler
from config import SETTINGS

import json
import logging
import redis
import sys
import time

# initialize logger
logging.basicConfig()
logger = logging.getLogger("monitor")
if SETTINGS['PYTHON_ENV'] == 'development' or SETTINGS['PYTHON_ENV'] == 'test':
  logger.setLevel(logging.DEBUG)
else:
  logger.setLevel(logging.INFO)
  handler = AirbrakeHandler(SETTINGS['AIRBRAKE_API_KEY'], environment=SETTINGS['PYTHON_ENV'], component_name='validator', node_name='data25c')
  handler.setLevel(logging.ERROR)
  logger.addHandler(handler)
  
# initialize redis connection
redis_data = redis.StrictRedis.from_url(SETTINGS['REDIS_URL'])

def process_queue(previous_message, queue, queue_processing):
  # sleep for a bit
  time.sleep(5)
  # peek again at the head of the processing queue and compare
  message = redis_data.lindex(queue_processing, -1)
  if message is None:
    logger.info(queue_processing + ': empty')
  else:
    data = json.loads(message)
    logger.info(queue_processing + ': ' + data['uuid'] + ' is at the head of the processing queue')
    # if same, re-enqueue
    if previous_message is not None and previous_message == message:
      # remove from processing queue and re-enqueue on main queue
      pipe = redis_data.pipeline()
      pipe.lrem(queue_processing, 0, message)
      pipe.lpush(queue, message)
      pipe.execute()
      logger.info(queue + ': ' + data['uuid'] + ' re-enqueued')
      # assuming we've removed the head, peek again
      message = redis_data.lindex(queue_processing, -1)
      if message is None:
        logger.info(queue_processing + ': empty')
      else:
        data = json.loads(message)
        logger.info(queue_processing + ': ' + data['uuid'] + ' is at the head of the processing queue')
  return message

if __name__ == '__main__':
  if len(sys.argv) < 2:
    logger.error("Please specify the redis key of the queue you wish to monitor on the command line: python monitor.py <queue name>")
  else:
    queue = sys.argv[1]
    queue_processing = queue + '_PROCESSING'
    logger.info("Starting monitor on %s/%s..." % (queue, queue_processing))
    previous_message = None
    while True:
      previous_message = process_queue(previous_message, queue, queue_processing)
