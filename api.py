from airbrakepy.logging.handlers import AirbrakeHandler
from config import SETTINGS
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
  handler = AirbrakeHandler(SETTINGS['AIRBRAKE_API_KEY'], environment=SETTINGS['PYTHON_ENV'], component_name='validator', node_name='data25c')
  handler.setLevel(logging.ERROR)
  logger.addHandler(handler)

app = Flask(__name__)

@app.route('/api/clicks/undo', methods=['POST'])
def clicks_undo():
  for uuid in request.form.getlist('uuids[]'):
    click.undo_click(uuid)
  return ''

@app.route('/api/clicks/fund', methods=['POST'])
def clicks_fund():
  for uuid in request.form.getlist('uuids[]'):
    click.fund_click(uuid)
  return ''
  
if __name__ == '__main__':
  port = int(os.environ.get('PORT', 5400))
  app.run(host='0.0.0.0', port=port)
  