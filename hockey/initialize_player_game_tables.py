import os

import duckdb


############## Constants ################

DB_NAME = 'hockey-stats.db'

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
                    game_id INT,
                    game_date DATE,
                    team VARCHAR,
                    position VARCHAR,
                    state VARCHAR,
                    icetime FLOAT,
                    goals INT,
                    primary_assists INT,
                    secondary_assists INT,
                    shots INT,
                    ixG FLOAT,
                    GF INT,
                    GA INT,
                    GF_share FLOAT,
                    xGF FLOAT,
                    xGA FLOAT,
                    xGF_share FLOAT,
                    CF INT,
                    CA INT,
                    CF_share FLOAT
                 );
                 """)

    print('skater_games table initialized!')

    conn.execute("""
                CREATE OR REPLACE TABLE goalie_games (
                    name VARCHAR,
                    game_id INT,
                    game_date DATE,
                    team VARCHAR,
                    state VARCHAR,
                    icetime FLOAT,
                    SA INT,
                    GA INT,
                    xGA FLOAT
                 );
                """)

    print('goalie_games table initialized!')

    print('Table initialization complete!')
