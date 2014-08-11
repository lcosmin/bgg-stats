import logging

from boardgamegeek.api import BGGAPI


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger("bgg")


def get_user_collection(username):
    bgg = BGGAPI()

    col = bgg.collection(username)

    d = []

    for game in col.iteritems():
        # fetch game info
        complete_game_info = bgg.game(None, bgid=game.id)

        d.append({"user": col.owner,
                  "game_id": game.id,
                  "game_name": game.name,
                  "game_year": complete_game_info.year,
                  "game_minimum_players": complete_game_info.min_players,
                  "game_maximum_players": complete_game_info.max_players,
                  "game_play_time": complete_game_info.playing_time,
                  "game_minimum_age": complete_game_info.min_age,
                  "game_categories": complete_game_info.categories,
                  "game_mechanics": complete_game_info.mechanics,
                  "user_rating": game.rating,
                  "user_owned": game.own,
                  "user_prevowned": game.prev_owned,
                  "user_preordered": game.preordered,
                  "user_want": game.want,
                  "user_want_to_buy": game.want_to_buy,
                  "user_want_to_play": game.want_to_play,
                  "user_wishlist": game.wishlist,
                  "user_wishlist prio": game.wishlist_priority})
    return d