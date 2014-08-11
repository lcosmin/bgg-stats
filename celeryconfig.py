BROKER_URL = "redis://localhost:6379/0"

CELERY_RESULT_BACKEND = "mongodb://localhost"
CELERY_MONGODB_BACKEND_SETTINGS = {
    "database": "celery",
    "taskmeta_collection": "tasks"
}


CELERYD_HIJACK_ROOT_LOGGER = False

CELERY_SEND_EVENTS = True

CELERYD_CONCURRENCY = 4

CELERY_RESULT_SERIALIZER = "json"

CELERY_TASK_SERIALIZER = "json"

CELERY_ACCEPT_CONTENT = ['json']
