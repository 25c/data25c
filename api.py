from airbrakepy.logging.handlers import AirbrakeHandler
from config import SETTINGS
from datetime import datetime
from flask import Flask, request

import click
import logging
import os

# initialize logger
logging.basicConfig()
logger = logging.getLogger("api")
if SETTINGS['PYTHON_ENV'] == 'development' or SETTINGS['PYTHON_ENV'] == 'test':
  logger.setLevel(logging.DEBUG)
else:
  logger.setLevel(logging.INFO)
  handler = AirbrakeHandler(SETTINGS['AIRBRAKE_API_KEY'], environment=SETTINGS['PYTHON_ENV'], component_name='api', node_name='data25c')
  handler.setLevel(logging.ERROR)
  logger.addHandler(handler)

app = Flask(__name__)

@app.route('/api/clicks/undo', methods=['POST'])
def clicks_undo():
  for uuid in request.form.getlist('uuids[]'):
    click.undo_click(uuid)
  return ''
  
@app.route('/api/comments/block', methods=['POST'])
def comments_block():
  for uuid in request.form.getlist('uuids[]'):
    logger.debug('blocking comment: ' + uuid);
    click.block_comment(uuid)
  return ''
  
if __name__ == '__main__':
  port = int(os.environ.get('PORT', 5400))
  app.run(host='0.0.0.0', port=port)
  