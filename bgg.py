import logging

from libBGG.BGGAPI import BGGAPI


log = logging.getLogger("bgg")


def get_user_collection(username):
    bgg = BGGAPI()
    return bgg.fetch_collection(username).data()

if __name__ == "__main__":
    get_user_collection("fagentu007")
