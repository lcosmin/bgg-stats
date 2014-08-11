from celery import Celery


from bgg import get_user_collection

app = Celery()
app.config_from_object('celeryconfig')


@app.task
def fetch_bgg_user_collection(username):
    return get_user_collection(username, cache=True, cache_location="/Users/drk/Local/Projects/personal/bgg-stats")
