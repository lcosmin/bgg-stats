from celery import Celery
import os

from bgg import get_user_collection, get_user_plays

app = Celery()
app.config_from_object('celeryconfig')


@app.task
def fetch_bgg_user_data(username):
    return [get_user_collection(username, cache="sqlite://{}".format(os.path.expanduser("~/.bgg_cache"))),
            get_user_plays(username, cache="sqlite://{}".format(os.path.expanduser("~/.bgg_cache")))]
