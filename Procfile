validator: python validator.py
processor: python processor.py
monitor1: python monitor.py QUEUE
monitor2: python monitor.py QUEUE_DEDUCT
web: gunicorn api:app -b 0.0.0.0:$PORT -w 3
