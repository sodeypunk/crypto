from celery import Celery

app = Celery('crypto', broker='amqp://guest@localhost//', backend='rpc://')

@app.task(ignore_result=True)
def add(x, y):
    result = x + y
    return result