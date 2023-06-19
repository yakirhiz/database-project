from requests import get
import json
import mysql.connector
from mysql.connector import errorcode


def create_tables():
    TABLES = [0,0,0,0,0]

    TABLES[0] = (
        "CREATE TABLE teams ("
        "   id int NOT NULL,"
        "   abbreviation CHAR(3),"
        "   city VARCHAR(20),"
        "   conference CHAR(4),"
        "   full_name VARCHAR(25),"
        "   nickname VARCHAR(20),"
        "   PRIMARY KEY (id)"
        ")"
    )

    TABLES[1] = (
        "CREATE TABLE players ("
        "   id int NOT NULL,"
        "   first_name VARCHAR(20),"
        "   last_name VARCHAR(20) NOT NULL,"
        "   position VARCHAR(20),"
        "   team_id int,"
        "   FULLTEXT (last_name),"
        "   PRIMARY KEY (id),"
        "   FOREIGN KEY (team_id) REFERENCES teams(id)"
        ")"
    )

    TABLES[2] = (
        "CREATE TABLE games ("
        "   id int NOT NULL,"
        "   date VARCHAR(40),"
        "   home_team_id int,"
        "   home_team_score int,"
        "   visitor_team_id int,"
        "   visitor_team_score int,"
        "   season int,"
        "   INDEX(home_team_id),"
        "   PRIMARY KEY (id),"
        "   FOREIGN KEY (home_team_id) REFERENCES teams(id),"
        "   FOREIGN KEY (visitor_team_id) REFERENCES teams(id)"
        ")"
    )

    TABLES[3] = (
        "CREATE TABLE players_seasons_stats ("
        "   player_id int NOT NULL,"
        "   points_average float,"
        "   field_goal_precentage float,"
        "   free_throw_precentage float,"
        "   field_goal3_precentage float,"
        "   average_minutes VARCHAR(10),"
        "   rebounds float,"
        "   assists float,"
        "   steals float,"
        "   blocks float,"
        "   games_played int,"
        "   season int,"
        "   INDEX(player_id),"
        "   PRIMARY KEY (player_id, season),"
        "   FOREIGN KEY (player_id) REFERENCES players(id)"
        ")"
    )

    TABLES[4] = (
        "CREATE TABLE players_games_stats ("
        "   id int NOT NULL,"
        "   game_id int,"
        "   player_id int,"
        "   team_id int,"
        "   rebounds int,"
        "   points int,"
        "   asissts int,"
        "   blocks int,"
        "   INDEX(player_id),"
        "   PRIMARY KEY (id),"
        "   FOREIGN KEY (player_id) REFERENCES players(id),"
        "   FOREIGN KEY (game_id) REFERENCES games(id),"
        "   FOREIGN KEY (team_id) REFERENCES teams(id)"
        ")"
    )

    for i in range(len(TABLES)):
        table_create_query = TABLES[i]
        try:
            print("Creating table {}: ".format(table_create_query))
            cursor.execute(table_create_query)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("Already exist.")
            else:
                print(err.msg)
        else:
            print('OK')


def drop_tables():
    TABLES = [0,0,0,0,0]

    TABLES[0] = ("DROP TABLE teams")

    TABLES[1] = ("DROP TABLE players")

    TABLES[2] = ("DROP TABLE games")

    TABLES[3] = ("DROP TABLE players_seasons_stats")

    TABLES[4] = ("DROP TABLE players_games_stats")

    for i in range(len(TABLES) - 1, -1, -1):
        drop_table_query = TABLES[i]
        try:
            print("Droping table {}: ".format(drop_table_query))
            cursor.execute(drop_table_query)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("Already exist.")
            else:
                print(err.msg)
        else:
            print('OK')


PLAYRES_INSERT = ("INSERT INTO players "
                  "(id, first_name, last_name, position, team_id) "
                  "VALUES (%s, %s, %s, %s, %s)")

TEAMS_INSERT = ("INSERT INTO teams "
                  "(id,abbreviation,city,conference,full_name,nickname) "
                  "VALUES (%s, %s, %s, %s, %s, %s)")

GAMES_INSERT = ("INSERT INTO games "
                  "(id,date,home_team_id,visitor_team_id,home_team_score,visitor_team_score,season) "
                  "VALUES (%s, %s, %s, %s, %s, %s, %s)")

PLAYERS_SEASON_STATS_INSERT = ("INSERT INTO players_seasons_stats "
                                "(player_id,points_average,field_goal_precentage,free_throw_precentage,field_goal3_precentage,average_minutes,rebounds, assists, steals, blocks, games_played, season) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

PLAYERS_GAMES_STATS_INSERT = ("INSERT INTO players_games_stats "
                                "(id,game_id,player_id,team_id,rebounds,points,asissts,blocks) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")


def insertToTables():
    filenames_lst = ["teams.txt", "players.txt", "games.txt", "players_seasons_stats.txt", "players_games_stats.txt"]
    
    indices_lst = [
        {1:"id",2:"abbreviation", 3:"city", 4:"conference", 5:"full_name", 6:"nickname"},
        {1: "id", 2:"first_name", 3:"last_name", 4:"position", 5:"team_id"},
        {1: "id", 2:"date", 3:"home_team_id", 4:"visitor_team_id", 5:"home_team_score", 6:"visitor_team_score", 7:"season"},
        {1: "player_id", 2:"points_average", 3:"field_goal_precentage", 4:"free_throw_precentage", 5:"field_goal3_precentage", 6:"avergae_minutes", 7:"rebounds", 8:"assists", 9:"steals", 10:"blocks", 11:"games_played", 12:"season"},
        {1: "id", 2:"game_id", 3:"player_id", 4:"team_id", 5:"rebounds", 6:"points", 7:"assists", 8:"blocks"}
    ]
    
    queries_lst = [TEAMS_INSERT, PLAYRES_INSERT, GAMES_INSERT, PLAYERS_SEASON_STATS_INSERT, PLAYERS_GAMES_STATS_INSERT]
    for i in range(len(filenames_lst)):
        insertToTable(filenames_lst[i], indices_lst[i], queries_lst[i])


def insertToTable(filename, indices, query):
    with open(filename, 'r') as f:
        lines = f.readlines()
        for line in lines:
            obj = json.loads(line)
            lst = [obj[indices[i]] for i in range(1, len(indices) + 1)]
            tuple_row = tuple(lst)
            cursor.execute(query,tuple_row)
            cnx.commit()


if __name__ == "__main__":
    cnx = mysql.connector.connect(user='eldadl', password='elda1353',
                                    host='mysqlsrv1.cs.tau.ac.il', database='eldadl',
                                    port='3306')
    
    cursor = cnx.cursor(prepared=True)
    drop_tables()
    create_tables()
    insertToTables()
    cnx.commit()
    cursor.close()
    cnx.close()
