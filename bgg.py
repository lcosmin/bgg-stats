import logging

from boardgamegeek.api import BoardGameGeek


logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger("bgg")


def get_user_collection(username, **kwargs):
    try:
        bgg = BoardGameGeek(**kwargs)

        collection = bgg.collection(username)

        d = []

        for game in collection:
            # fetch complete user info (for fun statistics)
            complete_user_info = bgg.user(collection.owner)

            # fetch game info
            complete_game_info = bgg.game(name=game.name, game_id=game.id)

            # the _id of the document will be "username+game id"
            d.append({"_id": "{} {}".format(collection.owner, game.id),
                      # user information
                      "user": {
                          "name": collection.owner,
                          "country": complete_user_info.country,
                          "state": complete_user_info.state,
                          "total_buddies": complete_user_info.total_buddies,
                          "buddies": [b.name for b in complete_user_info.buddies],
                          "total_guilds": complete_user_info.total_guilds,
                          "guilds": [g.name for g in complete_user_info.guilds],
                          "top10": [g.name for g in complete_user_info.top10],
                          "hot10": [g.name for g in complete_user_info.hot10],
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
                          "expansion": complete_game_info.expansion,
                          "expansions": [i.name for i in complete_game_info.expansions],
                          "expands": [i.name for i in complete_game_info.expands]
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
                                   "wishlist_prio": game.wishlist_priority
                          }
                      }
            })

        return d
    except Exception as e:
        log.exception("{}".format(e))
        return []