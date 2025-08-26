import polars as pl


############## Constants ################

# URL used to download CSV data from MoneyPuck
DATA_URL = 'https://moneypuck.com/moneypuck/playerData/seasonSummary/{}/regular/skaters.csv'

# Columns that will be used from base CSV
USED_COLUMNS = ['playerId', 'season', 'name', 'team', 'position', 'situation', 'games_played',
                'icetime', 'I_F_points', 'I_F_goals', 'OnIce_F_flurryScoreVenueAdjustedxGoals',
                'OnIce_A_flurryScoreVenueAdjustedxGoals', 'OnIce_F_goals', 'OnIce_A_goals']

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
    """

    df = pl.read_csv(DATA_URL.format(season), columns=USED_COLUMNS)

    # Icetime is in seconds by default, convert to minutes
    df = df.with_columns(pl.col('icetime') / 60.0)

    # Rename some columns to be nicer to work with
    df = df.rename({
        'OnIce_F_goals': 'goalsFor',
        'OnIce_A_goals': 'goalsAgainst',
        'OnIce_F_flurryScoreVenueAdjustedxGoals': 'xGoalsFor',
        'OnIce_A_flurryScoreVenueAdjustedxGoals': 'xGoalsAgainst',
        'I_F_points': 'points',
        'I_F_goals': 'goals'
    })

    # Compute rate metrics from each column containing a total metric value,
    # i.e. goalsFor -> goalsForPerHour (GFph)
    for total_col, rate_col in zip(['goalsFor', 'goalsAgainst', 'xGoalsFor',
                                    'xGoalsAgainst', 'points', 'goals'],
                                    ['GFph', 'GAph', 'xGFph', 'xGAph', 'ppg', 'gph']):

        df = df.with_columns((pl.col(total_col) * (60.0 / pl.col('icetime'))).alias(rate_col))

    # Also compute a players average icetime per game
    df = df.with_columns(pl.col('icetime') / (pl.col('games_played') * 60.0))

    return df


if __name__ == '__main__':
    test_df = gather_df(2024)
    print(test_df)
    test_df.write_csv('test.csv')
