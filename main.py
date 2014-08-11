from tasks import fetch_bgg_user_collection
import pymongo
import json
import time
import logging

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

tasks = []

with open("users.txt") as f:
    usernames = set([user.strip() for user in f.readlines()])

users = [x for x in usernames]

log.debug("sending tasks")

for u in users:
    tasks.append(fetch_bgg_user_collection.delay(u))

users_done = set()

conn = pymongo.MongoClient()

db = conn.bgg

data = db.data

keep_running = True


while keep_running:
    keep_running = False
    for user, result in zip(users, tasks):
        if result.ready():
            if user not in users_done:
                log.info("writing results for user {}".format(user))
                try:
                    data.insert(result.result)
                except Exception as e:
                    log.info("exception: {}, result: {}".format(e, result.result))
                users_done.add(user)
        else:
            keep_running = True

    time.sleep(5)