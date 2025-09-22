import argparse
import duckdb


############## Constants ################

DB_NAME = 'md:'

########### End Constants ###############

"""
Script meant to only be run once, to initialize the tables used to store game-by-game
skater data that will be updated after every game.
"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--preseason', action='store_true', default=False,
                        help='Enable flag to create tables for preseason data.')
    args = parser.parse_args()

    print('Connecting to database...')
    conn = duckdb.connect(database=DB_NAME, read_only=False)

    skater_table = 'skater_games'
    goalie_table = 'goalie_games'
    if args.preseason:
        skater_table = f'preseason_{skater_table}'
        goalie_table = f'preseason_{goalie_table}'

    conn.execute(f"""
                CREATE OR REPLACE TABLE {skater_table} (
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

    print(f'{skater_table} table initialized!')

    conn.execute(f"""
                CREATE OR REPLACE TABLE {goalie_table} (
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

    print(f'{goalie_table} table initialized!')

    print('Table initialization complete!')
