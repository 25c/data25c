scraper: python scraper.py
processor: python processor.py
monitor1: python monitor.py QUEUE
web: gunicorn api:app -b 0.0.0.0:$PORT -w 3
