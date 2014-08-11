import logging

from boardgamegeek.api import BoardGameGeek


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger("bgg")


def get_user_collection(username, **kwargs):
    try:
        bgg = BoardGameGeek(**kwargs)

        col = bgg.collection(username)

        d = []

        for game in col.iteritems():
            # fetch complete user info (for fun statistics)
            complete_user_info = bgg.user(col.owner)

            # fetch game info
            complete_game_info = bgg.game(name=game.name, game_id=game.id)

            # the _id of the document will be "username+game id"
            d.append({"_id": "{} {}".format(col.owner, game.id),
                      # user information
                      "user": {
                          "name": col.owner,
                          "country": complete_user_info.country,
                          "state": complete_user_info.state
                      },

                      # game information
                      "game": {
                          "id": game.id,
                          "name": game.name,
                          "year": complete_game_info.year,
                          "min_age": complete_game_info.min_age,
                          "min_players": complete_game_info.min_players,
                          "max_players": complete_game_info.max_players,
                          "play_time": complete_game_info.playing_time,
                          "categories": complete_game_info.categories,
                          "families": complete_game_info.families,
                          "mechanics": complete_game_info.mechanics,
                      },

                      # statistics
                      "stats": {
                          "average_rating": complete_game_info.rating_average,
                          "ranks": complete_game_info.ranks,
                          "users_owned": complete_game_info.users_owned,
                          "users_want": complete_game_info.users_wanting,
                          "users_wish": complete_game_info.users_wishing,
                          "users_trading": complete_game_info.users_trading,
                          "users_rated": complete_game_info.users_rated,


                          # user-specific statistics (from his collection)
                          "user": {"rating": game.rating,
                                   "owned": game.owned,
                                   "prevowned": game.prev_owned,
                                   "preordered": game.preordered,
                                   "want": game.want,
                                   "want_to_buy": game.want_to_buy,
                                   "want_to_play": game.want_to_play,
                                   "wishlist": game.wishlist,
                                   "wishlist prio": game.wishlist_priority
                          }
                      }
            })

        return d
    except Exception as e:
        log.exception("{}".format(e))
        return []