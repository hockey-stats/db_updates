import polars as pl



############## Constants ################

# URL used to download CSV data from MoneyPuck
DATA_URL = 'https://moneypuck.com/moneypuck/playerData/careers/gameByGame/all_teams.csv'

# Columns that will be used from base CSV
USED_COLUMNS = ['gameId', 'season', 'team', 'situation', 'iceTime', 'xGoalsPercentage',
                'corsiPercentage', 'goalsFor', 'goalsAgainst', 'playoffGame']

########### End Constants ###############


def gather_df(season: int) -> pl.DataFrame:
    """
    Script used to update tables containing game-by-game data for each team.

    The dataset provided by MoneyPuck includes every season going back to 2008. This is a bit
    excessive for our purposes, so in our table we will only store data from the current season.

    Designed to be called by a larger DB update script (e.g. update_tables.py in this directory).

    As of August 27, 2025, the plots which use this table are:
        - xG% rolling average plot

    :param int season: The season we'll be working with.
    :return pl.DataFrame: Cleaned and proccessed DataFrame that will be used to update the DB.
    """

    df = pl.read_csv(DATA_URL, columns=USED_COLUMNS)

    # Filter to just this season and exclude playoff games
    df = df.filter((pl.col('season') == season) & (pl.col('playoffGame') == 0))

    # Don't need to keep this column after the filter call
    df = df.drop(['playoffGame'])

    df = df.rename({
        'gameId': 'gameID',
        'xGoalsPercentage': 'xGoalsShare'
    })

    return df


if __name__ == '__main__':
    test_df = gather_df(2024)
    print(test_df)
