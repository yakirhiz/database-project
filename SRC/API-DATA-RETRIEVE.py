from requests import get
import mysql.connector
import json
import time

baseURL = "https://www.balldontlie.io/api/v1/"


def getGames():
    query_games = "games/?per_page=100&seasons[]="
    query = baseURL + query_games
    with open('games.txt', 'w') as f:
        for year in range(2015, 2019):
            currQuery = query + str(year) + "&"
            cnt = 0
            data = get(currQuery).json()
            total_pages = data['meta']['total_pages']
            for page in range(1, total_pages + 1):
                final_query = currQuery + "page=" + str(page)
                data_in_page = get(final_query).json()
                for record in data_in_page['data']:
                    data_to_insert = {
                        "id": record["id"],
                        "date": record["date"],
                        "home_team_id": record["home_team"]['id'],
                        "visitor_team_id": record["visitor_team"]['id'],
                        "home_team_score": record["home_team_score"],
                        "visitor_team_score": record["visitor_team_score"],
                        "season": record["season"]
                    }

                    print(f"Adding game number {cnt} \n")
                    json.dump(data_to_insert, f)
                    f.write("\n")
                    cnt += 1
                    if cnt % 60 == 0:
                        time.sleep(30)
        
            


def getSeasonStats():
    query_teams = "season_averages?"
    query = baseURL + query_teams
    with open('players_seasons_stats.txt', 'w') as f:
        for year in range(2015, 2019):
            season = "season=" + str(year) + "&"
            seasonQuery = query + season
            for i in range(1, 495):
                currQuery = seasonQuery + "player_ids[]=" +str(i)
                try:
                    data = get(currQuery).json()
                except:
                    continue

                #checks if data exits
                if len(data['data']) == 0:
                    continue
                
                player_season_data = data['data'][0]
                data_to_insert = {
                    "player_id": player_season_data["player_id"],
                    "points_average": player_season_data["pts"],
                    "field_goal_precentage": player_season_data["fg_pct"],
                    "free_throw_precentage": player_season_data["ft_pct"],
                    "field_goal3_precentage": player_season_data["fg3_pct"],
                    "avergae_minutes": player_season_data["min"],
                    "rebounds": player_season_data["reb"],
                    "assists": player_season_data["ast"],
                    "steals": player_season_data["stl"],
                    "blocks": player_season_data["blk"],
                    "games_played": player_season_data['games_played'],
                    "season": player_season_data['season']
                }
            
                print(f"stat number {i + (year - 1015)}")
                json.dump(data_to_insert, f)
                f.write("\n")
                
                if i % 59 == 0:
                    time.sleep(40)


def getGameStats():
    # https://www.balldontlie.io/api/v1/stats/?postseason=true&seasons[]=2015&per_page=100&page=2
    ## 
    # stats of a player on spacific game
    # #
    query_games = "stats/?postseason=true&per_page=100&"
    query = baseURL + query_games
    with open('players_games_stats.txt', 'w') as f:

        for year in range(2015, 2019):
            season = "seasons[]=" + str(year) + "&"
            currQuery = query + season
            data = get(currQuery).json()
            
            cnt = 0
            total_pages = data['meta']['total_pages']
            print(total_pages)
            for page in range(1, total_pages + 1):
                print(f"handling page {page}: \n")
                final_query = currQuery + "page=" + str(page)
                data_in_page = get(final_query).json()
                for record in data_in_page['data']:
                    data_to_insert = {
                        "id": record["id"],
                        "game_id": record["game"]["id"],
                        "player_id": record["player"]["id"],
                        "team_id": record["team"]["id"],
                        "rebounds": record["reb"],
                        "points": record["pts"],
                        "asissts": record["ast"],
                        "blocks": record["blk"]
                    }

                    json.dump(data_to_insert, f)
                    f.write("\n")
                    cnt += 1
                    print (f"{cnt}")
                    if cnt % 59 == 0:
                        time.sleep(30)



def getTeams():
    query_teams = "teams/"
    query = baseURL + query_teams
    with open('teams.txt', 'w') as f:

        for i in range(1, 31):
            currQuery = query + str(i)
            data = get(currQuery).json()
            data_to_insert = {
                "id": data["id"],
                "abbreviation": data["abbreviation"],
                "city": data["city"],
                "conference": data["conference"],
                "full_name": data["full_name"],
                "nickname": data["name"]
            }
        
            print(data_to_insert)
            json.dump(data_to_insert, f)
            f.write("\n")
            
            if i % 59 == 0:
                time.sleep(30)



def getPlayers():
    query_players = "players/?per_page=100&"
    query = baseURL + query_players
    ids = set()
    with open('players.txt', 'w') as f:
        currQuery = query
        cnt = 0
        data = get(currQuery).json()
        total_pages = data['meta']['total_pages']
        for page in range(1, total_pages + 1):
            final_query = currQuery + "page=" + str(page)
            data_in_page = get(final_query).json()
            for record in data_in_page['data']:
                
                if record["id"] in ids:
                    continue

                ids.add(record["id"])
                data_to_insert = {
                    "id": record["id"],
                    "first_name": record["first_name"],
                    "last_name": record["last_name"],
                    "position": record["position"],
                    "team_id": record["team"]["id"]
                }

                print(f"Adding game number {cnt} \n")
                json.dump(data_to_insert, f)
                f.write("\n")
                cnt += 1
                if cnt % 60 == 0:
                    time.sleep(30)



if __name__ == "__main__":
    getGameStats()
    getSeasonStats()
    getGames()
    getTeams()
    getPlayers()