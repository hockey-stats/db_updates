import duckdb


############## Constants ################

DB_NAME = 'md:'

########### End Constants ###############

"""
Script meant to only be run once, to initialize the tables used to store game-by-game
skater data that will be updated after every game.
"""

if __name__ == '__main__':
    print('Connecting to database...')
    conn = duckdb.connect(database=DB_NAME, read_only=False)

    conn.execute("""
                CREATE OR REPLACE TABLE skater_games (
                    name VARCHAR,
                    gameID INT,
                    gameDate DATE,
                    team VARCHAR,
                    position VARCHAR,
                    state VARCHAR,
                    iceTime FLOAT,
                    goals INT,
                    primaryAssists INT,
                    secondaryAssists INT,
                    shots INT,
                    individualxGoals FLOAT,
                    goalsFor INT,
                    goalsAgainst INT,
                    goalsShare FLOAT,
                    xGoalsFor FLOAT,
                    xGoalsAgainst FLOAT,
                    xGoalsShare FLOAT,
                    corsiFor INT,
                    corsiAgaint INT,
                    corsiShare FLOAT
                 );
                 """)

    print('skater_games table initialized!')

    conn.execute("""
                CREATE OR REPLACE TABLE goalie_games (
                    name VARCHAR,
                    gameID INT,
                    gameDate DATE,
                    team VARCHAR,
                    state VARCHAR,
                    iceTime FLOAT,
                    shotsAgainst INT,
                    goalsAgainst INT,
                    xGoalsAgainst FLOAT
                 );
                """)

    print('goalie_games table initialized!')

    print('Table initialization complete!')
