import pymongo
import argparse
import time
import logging

from bgg import get_user_collection

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


parallel = False


statistics = """

# each game is owned by [...]
db.collections.aggregate([ {$match: {"stats.user.owned": true}},
                           {$group: {_id: "$game.name", "users": {$addToSet: "$user.name"} }} ])

#
db.collections.aggregate([ {$match: {"stats.user.owned": true}},
                           {$group: {_id: "$game.name", "rating": {$avg: "$stats.average_rating"},
                           "users": {$push: "$user.name"} }}, {$sort: {"rating": -1}} ])

"""


def get_users(fname):
    # get the list of users, remove any duplicates
    with open(fname) as f:
        usernames = set(filter(lambda x: len(x), [user.strip() for user in f.readlines()]))

    return [x for x in usernames]


def fetch_user_collections(users):

    if parallel:

        from tasks import fetch_bgg_user_collection
        tasks = []
        finished_tasks = set()

        for user in users:
            # create a celery task for fetching this user's collection
            tasks.append(fetch_bgg_user_collection.delay(user))

        done = False
        # wait for all tasks to finish

        log.info("submitted tasks, waiting for replies")

        while not done:
            time.sleep(2)
            done = True
            for task in tasks:
                # look for a task that's ready
                if task.ready():
                    if task not in finished_tasks:
                        # a finished task we haven't seen before
                        finished_tasks.add(task)
                        yield task.result
                else:
                    done = False

    else:
        for user in users:
            log.info("fetching {}'s collection...".format(user))
            try:
                yield get_user_collection(user)
            except Exception as e:
                log.exception("error getting {}'s collection".format(user))


def write_results(mongo_collection, data):
    if data:
        for game in data:
            try:
                log.info(u"saving {}'s collection, game: {}".format(game["user"]["name"],
                                                                    game["game"]["name"]))
                mongo_collection.save(game)
            except Exception as e:
                log.exception("error writing {}'s collection to the database: {}".format(game["user"]["name"], e))


def main():
    global parallel

    p = argparse.ArgumentParser()

    p.add_argument("--mongodb", help="mongodb URI (e.g. mongodb://host[:port]", required=True)
    p.add_argument("--db", help="mongodb Database", required=True)
    p.add_argument("--collection", help="mongodb collection", required=True)
    p.add_argument("--users", help="file containing usernames", required=True)
    p.add_argument("-P", "--parallel", help="run script in parallel using celery", default=False, action="store_true")

    args = p.parse_args()

    log.info("connecting to MongoDB {}, db:{}/col:{}".format(args.mongodb, args.db, args.collection))
    conn = pymongo.MongoClient(args.mongodb)

    db = conn[args.db]
    collection = db[args.collection]

    users = get_users(args.users)
    parallel = args.parallel

    for data in fetch_user_collections(users):
        write_results(collection, data)


if __name__ == "__main__":
    main()