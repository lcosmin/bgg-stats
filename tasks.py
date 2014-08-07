from celery import Celery
from celery.utils.log import get_task_logger


from bgg import get_user_collection

app = Celery('tasks', broker='redis://localhost:6379/0', backend="redis://localhost:6379/0")

app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='json',
)

log = get_task_logger(__name__)


@app.task
def fetch_bgg_user_collection(username):
    return get_user_collection(username)
