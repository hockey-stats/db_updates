import polars as pl


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


def gather_df(season: int) -> pl.DataFrame:
    """
    Script used to update tables containing skater-level data. 
    
    Pulls full data from MoneyPuck for a given season, saving only columns relevant to our plots,
    and stores in a polars DF. Then computes a few additional columns before returning the
    DataFrame. 

    Designed to be called by a larger DB update script (e.g. update_tables.py in this directory).

    As of August 25, 2025, the plots which use this table are:
        - Skater ratio scatter plots (i.e. xGF vs xGA)
        - Skater points-per-hour plot

    :param int season: The season we'll be working with.
    :return pl.DataFrame: Cleaned and proccessed DataFrame that will be used to update the DB.
    """

    df = pl.read_csv(DATA_URL.format(season), columns=USED_COLUMNS)

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

    # Have columns in correct order
    df = df[['playerID', 'season', 'name', 'team', 'position', 'situation', 'gamesPlayed',
             'iceTime', 'points', 'goals', 'individualxGoals', 'xGoalsFor', 'xGoalsAgainst',
             'goalsFor', 'goalsAgainst', 'xGoalsForPerHour', 'xGoalsAgainstPerHour',
             'goalsForPerHour', 'goalsAgainstPerHour', 'pointsPerHour', 'goalsPerHour',
             'averageIceTime', 'penaltiesTaken', 'penaltiesDrawn', 'faceoffsWon', 'faceoffsLost',
             'shotsBlocked', 'oZoneShifts', 'dZoneShifts', 'neutralZoneShifts', 'flyShifts']]

    return df


if __name__ == '__main__':
    test_df = gather_df(2024)
    print(test_df)
