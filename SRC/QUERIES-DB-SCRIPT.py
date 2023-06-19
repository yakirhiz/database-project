import mysql.connector
from mysql.connector import errorcode

print("Selection queries...")
cnx = mysql.connector.connect(  user='eldadl',
                                password='elda1353',
                                host='mysqlsrv1.cs.tau.ac.il',
                                database='eldadl',
                                port='3306')

cursor = cnx.cursor(prepared=True)

def execute_query1():
  # Query #1 -
  print("Executing query #1...")
  
  query1 =  "SELECT PGS.game_id, P.first_name, P.last_name, PGS.points " \
            "FROM eldadl.players_games_stats as PGS, eldadl.players as P " \
            "WHERE P.id = PGS.player_id AND PGS.points >= ALL(  SELECT PGS2.points " \
            "FROM eldadl.players_games_stats as PGS2 " \
		    "WHERE PGS.game_id = PGS2.game_id AND PGS2.points IS NOT NULL) "

  cursor.execute(query1)
  print("query1: \n" + str(cursor.fetchall()))


def execute_query2():
  # Query #2 - The query return the number of teams that score more than 130 pts per season and conference
  print("Executing query #2...")
  
  query2 =  "SELECT high_score_teams.season, high_score_teams.conference, COUNT(*) as number_teams_over_130pts " \
            "FROM ( SELECT DISTINCT G.season, T.conference, T.id " \
                   "FROM eldadl.games as G, eldadl.teams as T " \
  	               "WHERE ((G.home_team_score > 130 AND G.home_team_id = T.id) " \
  	               "OR (G.visitor_team_score > 130 AND G.visitor_team_id = T.id))) AS high_score_teams " \
            "GROUP BY high_score_teams.season, high_score_teams.conference " \
            "ORDER BY high_score_teams.season"
  
  cursor.execute(query2)
  print("query2: \n" + str(cursor.fetchall()))


def execute_query3():
  # Query #3 - The query return for each player the amount games with over 30 pts for each season
  print("Executing query #3...")
  
  query3 =  "SELECT P.id, P.first_name, P.last_name , G.season, COUNT(*) as over_30pts_games " \
            "FROM eldadl.players as P, eldadl.games as G, eldadl.players_games_stats as PGS " \
            "WHERE PGS.game_id = G.id AND PGS.player_id = P.id AND PGS.points > 30 " \
            "GROUP BY P.id, G.season " \
            "HAVING COUNT(*) > 0 " \
            "ORDER BY P.id"
  
  cursor.execute(query3)
  print("query3: \n" + str(cursor.fetchall()))


def execute_query4():
  # Query #4 - The query return for each team the number if different players than played in the team (2015-2018)
  print("Executing query #4...")
  
  query4 =  "SELECT players_teams.full_name, COUNT(*) as num_of_players " \
            "FROM (SELECT DISTINCT T.id as team_id, T.full_name, PGS.player_id " \
                  "FROM eldadl.teams as T, eldadl.players_games_stats as PGS " \
                  "WHERE T.id = PGS.team_id) as players_teams " \
            "GROUP BY players_teams.team_id"
  
  cursor.execute(query4)
  print("query4: \n" + str(cursor.fetchall()))


def execute_query5():
  # Query #5 - The query return the average score of each conference per season
  print("Executing query #5...")
  
  query5 =    "SELECT team_score.season, team_score.conference, AVG(team_score.score) as avg_score " \
              "FROM (SELECT G1.season, T1.conference, G1.home_team_score as score " \
                  "FROM eldadl.games as G1, eldadl.teams as T1 " \
                  "WHERE T1.id = G1.home_team_id " \
                  "UNION " \
                  "SELECT G2.season, T2.conference, G2.visitor_team_score as score " \
                  "FROM eldadl.games as G2, eldadl.teams as T2 " \
                  "WHERE T2.id = G2.visitor_team_id) as team_score " \
              "GROUP BY team_score.season, team_score.conference " \
              "ORDER BY team_score.season"
  
  cursor.execute(query5)
  print("query5: \n" + str(cursor.fetchall()))


def execute_query6(last_name):
  # Query #6 - Given last name of a player the query return the basic details about the player
  print("Executing query #6...")
  
  query6=  "SELECT P.first_name, P.last_name, P.position, T.full_name " \
           "FROM eldadl.players as P, eldadl.teams as T " \
           "WHERE P.team_id = T.id AND MATCH(last_name) AGAINST('%s')"
  
  cursor.execute(query6 % last_name)
  print("query6: \n" + str(cursor.fetchall()))


if __name__ == "__main__":
  execute_query1()
  execute_query2()
  execute_query3()
  execute_query4()
  execute_query5()
  execute_query6('james')
