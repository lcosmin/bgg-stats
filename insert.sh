#!/bin/bash

function run() {
    loop=1
    while [[ $loop -eq 1 ]]
    do
        echo "trying to upload: $*"
        python upload_to_google.py --update $1 --file $2
        loop=$?
        sleep 5
    done


}

run 1oGCDXkgaIMFUgCBQn0CYsCrvjsiuNClklrGCIAPM ./STATS/dow_plays.csv

run 1SkWzhCEfDcmYwg75cQzxE-k_JPRXVtgmfESbPt3R ./STATS/game_recent_plays.csv

run 1rql18HAoHHRj6LWGFRLDGSUf9ZzdW_AjotbTXKbD ./STATS/user_plays_per_day_of_week.csv

run 1n9SHfXjz9dkvC94a464c5J6ZNmJhNVJZyVOcZgD1 ./STATS/monthly_plays.csv

run 1PDFPtBwHlQ4HdWveoUNRYfrZbsbxu0USEHm6MWWV ./STATS/game_plays.csv

run 1KLyuw0jue6PQ_rLsk-cUKiLilUwPsYk2jopzPo8Q ./STATS/user_recent_plays.csv

run 1DCHTCTXRBACZTlLkIiIpykpY5H7f1iAHIYlrR8hG ./STATS/user_plays.csv

run 1majmpGGF6AYG3WbZ2pcJ-3X4DBk8EKtv6ha99XzM ./STATS/hot10.csv

run 1OPEnBNUu0fd3M7Dqx8JrAkWooN2tC8jFvWt_Y_mG ./STATS/top10.csv

run 1_xJbzHFsUj_KuxmgNmMinDx8IkgsXobZenJGWEI6 ./STATS/popular_owned_families.csv

run 107LfOc8rqZQ7QAxowecb4A5XsYfc_o8SXBuXtpXl ./STATS/popular_owned_categories.csv

run 1CBRfUPTrOMyBz4yiEg8a7xZ6r6x_OmWc4sekm4Nm ./STATS/wished_for_games.csv

run 1JpEsTNQWT0aY1pJ31Ikn3hTzXk0lLu9mdLb5--8f ./STATS/games_owned_by.csv

run 11mYizgiHiqcyDETVW83xgtfyZ1SZT2OVc_wz7okR ./STATS/users_with_most_games.csv