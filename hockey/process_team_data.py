import polars as pl


############## Constants ################

# URL used to download CSV data from MoneyPuck
DATA_URL = 'https://moneypuck.com/moneypuck/playerData/seasonSummary/{}/regular/teams.csv'

# Columns that will be used from base CSV
USED_COLUMNS = ['season', 'team', 'situation', 'games_played', 'iceTime', 'goalsFor',
                'goalsAgainst', 'flurryScoreVenueAdjustedxGoalsFor',
                'flurryScoreVenueAdjustedxGoalsAgainst']

########### End Constants ###############


def gather_df(season: int) -> pl.DataFrame:
    """
    Script used to update tables containing team-level data.
    
    Pulls full data from MoneyPuck for a given season, saving only columns relevant to our plots,
    and stores in a polars DF. Then computes a few additional columns before updating the relevant
    table in the given database.

    As of August 25, 2025, the plots which use this table are:
        - Team ratio scatter plots (e.g. xGF vs xGA)
        - Special teams lollipop plots

    :param int season: The season we'll be working with.
    """

    df = pl.read_csv(DATA_URL.format(season), columns=USED_COLUMNS)

    # Icetime is in seconds by default, convert to minutes
    df = df.with_columns(pl.col('iceTime') / 60.0)

    # Rename some columns to be nicer to work with
    df = df.rename({
        'games_played': 'gamesPlayed',
        'flurryScoreVenueAdjustedxGoalsFor': 'xGoalsFor',
        'flurryScoreVenueAdjustedxGoalsAgainst': 'xGoalsAgainst'
    })

    # Compute rate metrics from each column containing a total metric value,
    # i.e. goalsFor -> goalsForPerHour 
    for total_col, rate_col in zip(['goalsFor', 'goalsAgainst', 'xGoalsFor', 'xGoalsAgainst'],
                                   ['goalsForPerHour', 'goalsAgainstPerHour',
                                    'xGoalsForPerHour', 'xGoalsAgainstPerHour']):

        df = df.with_columns((pl.col(total_col) * (60.0 / pl.col('iceTime'))).alias(rate_col))

    return df


if __name__ == '__main__':
    test_df = gather_df(2024)
    print(test_df.filter(pl.col('situation') == '5on5'))
    test_df.write_csv('test.csv')
