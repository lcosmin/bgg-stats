from __future__ import unicode_literals

import pymongo
import argparse
import time
import logging
import codecs
import cStringIO
import csv
import datetime

from bgg import get_user_collection

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


parallel = False


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


def stat_games_owned_by(col, f):
    #
    # * Most owned games
    # * Best rated owned games
    # * Best rated owned expansions
    # * Games owned by only one user
    #
    log.info("creating list of games along with their owners...")

    r = col.aggregate([
        {"$match": {"stats.user.owned": True}},
        {"$group": {"_id": {"name": "$game.name", "expansion": "$game.expansion"},
                    "rating": {"$avg": "$stats.average_rating"},
                    "users": {"$addToSet": "$user.name"}}}
    ])

    u = UnicodeWriter(f)
    u.writerow(["game", "rating", "expansion", "owners number", "owned by"])
    for res in r["result"]:
        u.writerow([res["_id"]["name"],
                    str(res["rating"]),
                    str(res["_id"]["expansion"]),
                    str(len(res["users"])),
                    " ".join(res["users"])])
        log.info(" '{}' owned by {}".format(res["_id"], res["users"]))


def stat_users_with_most_games(col, f):
    #
    # * Users with most games
    #
    log.info("creating list of users sorted by the number of owned games...")

    r = col.aggregate([
        {"$match": {"stats.user.owned": True}},
        {"$group": {"_id": "$user.name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ])


    u = UnicodeWriter(f)
    u.writerow(["user", "games owned"])
    for res in r["result"]:
        u.writerow([res["_id"], str(res["count"])])
        log.info("{} owns {} games".format(res["_id"], res["count"]))


def stat_wished_games(col, f):
    #
    # * Most wished for games
    # * Most wished for expansions
    #
    log.info("creating list of wished for games...")

    r = col.aggregate([
        {"$match": {"stats.user.wishlist": True}},
        {"$group": {"_id": {"name": "$game.name", "expansion": "$game.expansion"},
                    "users": {"$addToSet": "$user.name"},
                    "rating": {"$avg": "$stats.average_rating"}}}
    ])

    u = UnicodeWriter(f)
    u.writerow(["game", "rating", "expansion", "wished by number", "wished by"])

    for res in r["result"]:
        u.writerow([res["_id"]["name"],
                    str(res["rating"]),
                    str(res["_id"]["expansion"]),
                    str(len(res["users"])),
                    " ".join(res["users"])])
        log.info("{} wished for by {}".format(res["_id"], res["users"]))


def stat_categories_by_popularity(col, f):
    #
    # * Most popular game categories
    #
    log.info("creating list of most popular categories...")

    r = col.aggregate([
        {"$match": {"stats.user.owned": True, "game.expansion": False}},
        {"$unwind": "$game.categories"},
        {"$group": {"_id": "$game.categories", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ])

    u = UnicodeWriter(f)
    u.writerow(["category", "count"])

    for res in r["result"]:
        u.writerow([res["_id"], str(res["count"])])
        log.info("category {}, count: {}".format(res["_id"], res["count"]))


def stat_families_by_popularity(col, f):
    #
    # * Most popular game families
    #
    log.info("creating list of most popular game families...")

    r = col.aggregate([
        {"$match": {"stats.user.owned": True, "game.expansion": False}},
        {"$unwind": "$game.families"},
        {"$group": {"_id": "$game.families", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ])

    u = UnicodeWriter(f)
    u.writerow(["family", "count"])

    for res in r["result"]:
        u.writerow([res["_id"], str(res["count"])])
        log.info("family {}, count: {}".format(res["_id"], res["count"]))


def stat_most_popular_users_top10(col, f):
    #
    # * Most popular games from people's top10
    #
    log.info("creating list of most popular games from user's top10...")

    r = col.aggregate([
        {"$project": {"top_game": "$user.top10", "user": "$user.name"}},
        {"$group": {"_id": "$user", "top_game": {"$first": "$top_game"}}},  # in each db record there is a full top10 list, thus we keep only 1 copy
        {"$unwind": "$top_game"},
        {"$group": {"_id": "$top_game", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}
    ])

    u = UnicodeWriter(f)
    u.writerow(["game", "in top10 of"])

    for res in r["result"]:
        u.writerow([res["_id"], str(res["count"])])
        log.info("game '{}', count: {}".format(res["_id"], res["count"]))


def stat_most_popular_users_hot10(col, f):
    #
    # * Most popular games from people's hot10
    #
    log.info("creating list of most popular games from user's hot10...")

    r = col.aggregate([
        {"$project": {"hot_game": "$user.hot10", "user": "$user.name"}},
        {"$group": {"_id": "$user", "hot_game": {"$first": "$hot_game"}}},  # in each db record there is a full hot10 list, thus we keep only 1 copy
        {"$unwind": "$hot_game"},
        {"$group": {"_id": "$hot_game", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}
    ])

    u = UnicodeWriter(f)
    u.writerow(["game", "in hot10 of"])

    for res in r["result"]:
        u.writerow([res["_id"], str(res["count"])])
        log.info("game '{}', count: {}".format(res["_id"], res["count"]))


def stats_play_count_by_user(col, f):

    log.info("counting number of recorded plays per user...")

    r = col.aggregate([{"$group": {"_id": "$user", "count": {"$sum": "$quantity"}}}])

    u = UnicodeWriter(f)
    u.writerow(["user", "number of plays"])

    for res in r["result"]:
        u.writerow([res["_id"], str(res["count"])])
        log.info("user '{}', number of plays: {}".format(res["_id"], res["count"]))


def stats_recent_play_count_by_user(col, f):

    log.info("counting number of recent plays per user...")

    now = datetime.datetime.now()

    start_date = now - datetime.timedelta(days=30)

    r = col.aggregate([
        {"$match": {"date": {"$gte": start_date}}},
        {"$group": {"_id": "$user", "count": {"$sum": "$quantity"}}}
    ])

    u = UnicodeWriter(f)
    u.writerow(["user", "recent number of plays"])

    for res in r["result"]:
        u.writerow([res["_id"], str(res["count"])])
        log.info("user '{}', number of plays: {}".format(res["_id"], res["count"]))


def stats_play_count_by_game(col, f):

    log.info("counting number of plays per game...")

    r = col.aggregate([{"$group": {"_id": "$game_name", "count": {"$sum": "$quantity"} }}])

    u = UnicodeWriter(f)
    u.writerow(["game", "number of plays"])

    for res in r["result"]:
        u.writerow([res["_id"], str(res["count"])])
        log.info("game '{}', number of plays: {}".format(res["_id"], res["count"]))


def stats_recent_play_count_by_game(col, f):

    log.info("counting number of recent plays per game...")

    now = datetime.datetime.now()

    start_date = now - datetime.timedelta(days=30)

    r = col.aggregate([
        {"$match": {"date": {"$gte": start_date}}},
        {"$group": {"_id": "$game_name", "count": {"$sum": "$quantity"} }}
    ])

    u = UnicodeWriter(f)
    u.writerow(["game", "recent number of plays"])

    for res in r["result"]:
        u.writerow([res["_id"], str(res["count"])])
        log.info("game '{}', number of plays: {}".format(res["_id"], res["count"]))


def stats_play_count_by_month(col, f):

    log.info("counting number of plays per month...")

    r = col.aggregate([{"$match": {"date": {"$ne": None}}},
                       {"$project": {"month": {"$month": "$date"}, "quantity": "$quantity"}},
                       {"$group": {"_id": "$month", "count": {"$sum": "$quantity"}}}])

    u = UnicodeWriter(f)
    u.writerow(["month_id", "month", "number of plays"])

    for res in r["result"]:
        d = datetime.datetime.strptime(str(res["_id"]), "%m")
        u.writerow([str(res["_id"]), d.strftime("%b"), str(res["count"])])
        log.info("month {}, number of plays: {}".format(res["_id"], res["count"]))


def stats_play_count_by_day_of_week(col, f):

    log.info("counting number of plays per day of week...")

    r = col.aggregate([{"$match": {"date": {"$ne": None}}},
                       {"$project": {"dow": {"$dayOfWeek": "$date"}, "quantity": "$quantity"}},
                       {"$group": {"_id": "$dow", "count": {"$sum": "$quantity"}}}])

    u = UnicodeWriter(f)
    u.writerow(["dow_id", "day of week", "number of plays"])

    m = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}

    for res in r["result"]:
        u.writerow([str(res["_id"]), m[res["_id"]], str(res["count"])])
        log.info("day of week {}, number of plays: {}".format(res["_id"], res["count"]))


def stats_plays_per_user_per_day_of_week(col, f):

    # TODO: merge with user total plays
    # TODO: merge with per day of week

    log.info("counting number of plays per user per day of week...")

    r = col.aggregate([
        {"$match": {"date": {"$ne": None}}},
        {"$project": {"quantity": "$quantity",
                      "dow": {"$dayOfWeek": "$date"},
                      "user": "$user"}},
        {"$group": {"_id": {"user": "$user", "dow": "$dow"},
                    "count": {"$sum": "$quantity"}}},
        {"$group": {"_id": "$_id.user",
                    "days": {"$push": {"day": "$_id.dow", "count": "$count"}} }}

    ])

    u = UnicodeWriter(f)
    u.writerow(["user", "mon", "tue", "wed", "thu", "fri", "sat", "sun"])

    for res in r["result"]:
        row = [res["_id"], "0", "0", "0", "0", "0", "0", "0"]

        for d in res["days"]:
            row[d["day"]] = str(d["count"])

        log.info("user with plays: {}".format(row))
        u.writerow(row)



def statistics(games_collection, plays_collection, path):

    import os
    from functools import partial

    p = partial(os.path.join, path)

    # with open(p("games_owned_by.tsv"), "w") as f:
    #     stat_games_owned_by(games_collection, f)
    #
    # with open(p("users_with_most_games.tsv"), "w") as f:
    #     stat_users_with_most_games(games_collection, f)
    #
    # with open(p("wished_for_games.tsv"), "w") as f:
    #     stat_wished_games(games_collection, f)
    #
    # with open(p("popular_owned_categories.tsv"), "w") as f:
    #     stat_categories_by_popularity(games_collection, f)
    #
    # with open(p("popular_owned_families.tsv"), "w") as f:
    #     stat_families_by_popularity(games_collection, f)
    #
    # with open(p("top10.tsv"), "w") as f:
    #     stat_most_popular_users_top10(games_collection, f)
    #
    # with open(p("hot10.tsv"), "w") as f:
    #     stat_most_popular_users_hot10(games_collection, f)

    with open(p("user_plays.tsv"), "w") as f:
        stats_play_count_by_user(plays_collection, f)

    with open(p("user_recent_plays.tsv"), "w") as f:
        stats_recent_play_count_by_user(plays_collection, f)

    with open(p("game_plays.tsv"), "w") as f:
        stats_play_count_by_game(plays_collection, f)

    with open(p("game_recent_plays.tsv"), "w") as f:
        stats_recent_play_count_by_game(plays_collection, f)

    with open(p("monthly_plays.tsv"), "w") as f:
        stats_play_count_by_month(plays_collection, f)

    with open(p("dow_plays.tsv"), "w") as f:
        stats_play_count_by_day_of_week(plays_collection, f)

    with open(p("user_plays_per_day_of_week.tsv"), "w") as f:
        stats_plays_per_user_per_day_of_week(plays_collection, f)


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
                yield get_user_collection(user, cache="sqlite:///tmp/cache.db?ttl=36000")
            except Exception as e:
                log.exception("error getting {}'s collection".format(user))


def write_results(mongo_collection, data):
    if data:
        for game in data:
            try:
                log.info("saving {}'s collection, game: {}".format(game["user"]["name"],
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

    p.add_argument("-f", "--fetch", help="fetch collections", action="store_true")
    p.add_argument("-", "--users", help="file containing usernames")
    p.add_argument("-P", "--parallel", help="run script in parallel using celery", default=False, action="store_true")

    p.add_argument("-s", "--statistics", help="path where to store the statistics")

    args = p.parse_args()

    log.info("connecting to MongoDB {}, db:{}/col:{}".format(args.mongodb, args.db, args.collection))
    conn = pymongo.MongoClient(args.mongodb)

    db = conn[args.db]
    collection = db[args.collection]

    if args.fetch:
        users = get_users(args.users)
        parallel = args.parallel

        for data in fetch_user_collections(users):
            write_results(collection, data)

    if args.statistics:
        statistics(None, collection, args.statistics)

if __name__ == "__main__":
    main()