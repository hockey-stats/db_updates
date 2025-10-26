"""
One-off script which can be used to add new columns to skaters table
"""

from datetime import datetime
import polars as pl
import duckdb


############## Constants ################

# URL used to download CSV data from MoneyPuck
DATA_URL = 'https://moneypuck.com/moneypuck/playerData/seasonSummary/{}/regular/skaters.csv'

# Columns that will be used from base CSV
USED_COLUMNS = ['playerId', 'season', 'name', 'team', 'position', 'situation', 'games_played',
                'icetime', 'I_F_points', 'I_F_goals', 'I_F_xGoals', 
                'OnIce_F_flurryScoreVenueAdjustedxGoals', 'OnIce_A_flurryScoreVenueAdjustedxGoals',
                'OnIce_F_goals', 'OnIce_A_goals',
                'I_F_oZoneShiftStarts', 'I_F_dZoneShiftStarts', 'I_F_neutralZoneShiftStarts', 
                'I_F_flyShiftStarts',
                'faceoffsWon', 'faceoffsLost', 'shotsBlockedByPlayer',
                'penalties', 'penaltiesDrawn']

########### End Constants ###############

def main():
    dfs = []
    for y in range(2008, 2026):
        df = pl.read_csv(DATA_URL.format(y), columns=USED_COLUMNS)
        dfs.append(df)

    df = pl.concat(dfs)

    # Rename some columns to be nicer to work with
    df = df.rename({
        'OnIce_F_goals': 'goalsFor',
        'OnIce_A_goals': 'goalsAgainst',
        'OnIce_F_flurryScoreVenueAdjustedxGoals': 'xGoalsFor',
        'OnIce_A_flurryScoreVenueAdjustedxGoals': 'xGoalsAgainst',
        'I_F_points': 'points',
        'I_F_goals': 'goals',
        'I_F_xGoals': 'individualxGoals',
        'I_F_oZoneShiftStarts': 'oZoneShifts',
        'I_F_dZoneShiftStarts': 'dZoneShifts',
        'I_F_neutralZoneShiftStarts': 'neutralZoneShifts',
        'I_F_flyShiftStarts': 'flyShifts',
        'penalties': 'penaltiesTaken',
        'shotsBlockedByPlayer': 'shotsBlocked',
        'playerId': 'playerID',
        'games_played': 'gamesPlayed',
        'icetime': 'iceTime'
    })

    # Icetime is in seconds by default, convert to minutes
    df = df.with_columns(pl.col('iceTime') / 60.0)

    # Compute rate metrics from each column containing a total metric value,
    # i.e. goalsFor -> goalsForPerHour (GFph)
    for total_col, rate_col in zip(['goalsFor', 'goalsAgainst', 'xGoalsFor',
                                    'xGoalsAgainst', 'points', 'goals'],
                                    ['goalsForPerHour', 'goalsAgainstPerHour', 'xGoalsForPerHour',
                                     'xGoalsAgainstPerHour', 'pointsPerHour', 'goalsPerHour']):

        df = df.with_columns((pl.col(total_col) * (60.0 / pl.col('iceTime'))).alias(rate_col))

    # Also compute a players average icetime per game
    df = df.with_columns((pl.col('iceTime') / (pl.col('gamesPlayed'))).alias('averageIceTime'))

    conn = duckdb.connect(database='md:', read_only=False)

    # Have columns in correct order
    df = df[['playerID', 'season', 'name', 'team', 'position', 'situation', 'gamesPlayed',
             'iceTime', 'points', 'goals', 'individualxGoals', 'xGoalsFor', 'xGoalsAgainst',
             'goalsFor', 'goalsAgainst', 'xGoalsForPerHour', 'xGoalsAgainstPerHour',
             'goalsForPerHour', 'goalsAgainstPerHour', 'pointsPerHour', 'goalsPerHour',
             'averageIceTime', 'penaltiesTaken', 'penaltiesDrawn', 'faceoffsWon', 'faceoffsLost',
             'shotsBlocked', 'oZoneShifts', 'dZoneShifts', 'neutralZoneShifts', 'flyShifts']]

    conn.execute("""
                CREATE OR REPLACE TABLE skaters (
                    playerID INT,
                    season INT,
                    name VARCHAR,
                    team VARCHAR,
                    position VARCHAR,
                    situation VARCHAR,
                    gamesPlayed INT,
                    iceTime FLOAT,
                    points INT,
                    goals INT,
                    individualxGoals FLOAT,
                    xGoalsFor FLOAT,
                    xGoalsAgainst FLOAT,
                    goalsFor INT,
                    goalsAgainst INT,
                    xGoalsForPerHour FLOAT,
                    xGoalsAgainstPerHour FLOAT,
                    goalsForPerHour FLOAT,
                    goalsAgainstPerHour FLOAT,
                    pointsPerHour FLOAT,
                    goalsPerHour FLOAT,
                    averageIceTime FLOAT,
                    penaltiesTaken INT,
                    penaltiesDrawn INT,
                    faceoffsWon INT,
                    faceoffsLost INT,
                    shotsBlocked INT,
                    oZoneShifts INT,
                    dZoneShifts INT,
                    neutralZoneShifts INT,
                    flyShifts INT
                 );
                """)

    conn.execute("INSERT INTO skaters SELECT * FROM df")


if __name__ == '__main__':
    main()
