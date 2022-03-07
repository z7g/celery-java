from celery import Celery

app = Celery('tasks', broker='pyamqp://admin:admin@localhost/test',backend='rpc://localhost/test')
# app=Celery('tasks',broker='redis://localhost:6379/0',backend='redis://localhost:6379/1')

@app.task
def add(x, y):
    return x + y

@app.task
def div(x, y):
    return x / y