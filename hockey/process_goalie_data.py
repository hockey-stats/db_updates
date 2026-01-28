import polars as pl
from urllib.error import HTTPError
import requests

from process_team_data import get_data_with_retries


############## Constants ################

# URL used to download CSV data from MoneyPuck
DATA_URL = 'https://moneypuck.com/moneypuck/playerData/seasonSummary/{}/regular/goalies.csv'

# Columns that will be used from base CSV
USED_COLUMNS = ['playerId', 'season', 'name', 'team', 'situation', 'games_played', 'icetime',
                'goals', 'xGoals',
                'lowDangerGoals', 'lowDangerxGoals', 'lowDangerShots',
                'mediumDangerGoals', 'mediumDangerxGoals', 'mediumDangerShots',
                'highDangerGoals', 'highDangerxGoals', 'highDangerShots']

########### End Constants ###############


def gather_df(season: int) -> pl.DataFrame:
    """
    Script used to update tables containing goalie season-level data.

    Pulls full data from MoneyPuck for a given season, saving only columns relevant to our plots,
    and stores in a polars DF. Adds a few additional columns before returning the DataFrame.

    Designed to be called by a larger DB update script (e.g. update_tables.py in this directory).

    :param int season: The season for which to gather data.
    :return pl.DataFrame: Cleaned and processed DataFrame that will be used to update the DB.
    """

    #try:
    #    df = pl.read_csv(DATA_URL.format(season), columns=USED_COLUMNS)
    #except HTTPError as e:
    #    print(e)
    #    df = get_data_with_retries(data_url=DATA_URL, season=season, columns=USED_COLUMNS)

    r = requests.get(DATA_URL.format(season), verify=False)
    df = pl.read_csv(r.content, columns=USED_COLUMNS)

    # Icetime is in seconds by default, convert to minutes
    df = df.with_columns(pl.col('icetime') / 60.0)

    # Rename a few columns to match DB schema
    df = df.rename({
        'playerId': 'playerID',
        'games_played': 'gamesPlayed',
        'icetime': 'iceTime'
    })

    return df


if __name__ == '__main__':
    test_df = gather_df(2024)
    with pl.Config(tbl_cols=20):
        print(test_df)
