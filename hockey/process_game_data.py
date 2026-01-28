from datetime import datetime
import polars as pl
import requests



############## Constants ################

# URL used to download CSV data from MoneyPuck
DATA_URL = 'https://moneypuck.com/moneypuck/playerData/careers/gameByGame/all_teams.csv'

# Columns that will be used from base CSV
USED_COLUMNS = ['gameId', 'season', 'team', 'gameDate', 'home_or_away', 'situation', 'iceTime',
                'xGoalsFor', 'xGoalsAgainst', 'xGoalsPercentage',
                'penalityMinutesFor', 'penalityMinutesAgainst',
                'corsiPercentage', 'goalsFor', 'goalsAgainst', 
                'playoffGame']

########### End Constants ###############


def gather_df(season: int) -> pl.DataFrame:
    """
    Script used to update tables containing game-by-game data for each team.

    Designed to be called by a larger DB update script (e.g. update_tables.py in this directory).

    As of August 27, 2025, the plots which use this table are:
        - xG% rolling average plot

    :param int season: The season we'll be working with.
    :return pl.DataFrame: Cleaned and proccessed DataFrame that will be used to update the DB.
    """

    #df = pl.read_csv(DATA_URL, columns=USED_COLUMNS)
    r = requests.get(DATA_URL, verify=False)
    df = pl.read_csv(r.content, columns=USED_COLUMNS)

    # Filter to just this season and exclude playoff games
    df = df.filter((pl.col('season') == season) & (pl.col('playoffGame') == 0))

    # Don't need to keep this column after the filter call
    df = df.drop(['playoffGame'])

    df = df.with_columns(
        # Convert gameDate from a YYYYMMDD format to a YYYY-MM-DD format using datetime
        pl.col('gameDate').map_elements(
            lambda date: datetime.strftime(datetime.strptime(str(date), '%Y%m%d'), '%Y-%m-%d'),
            return_dtype=pl.String
        ),
        # Also convert 'home_or_away' into a boolean column
        pl.col('home_or_away').map_elements(
            lambda a: bool(a == 'HOME'),
            return_dtype=pl.Boolean
        ).alias('isHomeTeam'),
        # And convert iceTime from seconds into minutes
        pl.col('iceTime') / 60.0
    )

    # Rename a few columns to better align with the rest of the tables in the DB
    df = df.rename({
        'gameId': 'gameID',
        'xGoalsPercentage': 'xGoalsShare',
        'corsiPercentage': 'corsiShare',
        'penalityMinutesFor': 'penaltyMinutesFor',
        'penalityMinutesAgainst': 'penaltyMinutesAgainst'
    })

    # Have columns in correct order
    df = df[['team', 'season', 'gameID', 'gameDate', 'isHomeTeam', 'iceTime', 'situation',
             'xGoalsFor', 'xGoalsAgainst', 'xGoalsShare', 'corsiShare', 'goalsFor',
             'goalsAgainst', 'penaltyMinutesFor', 'penaltyMinutesAgainst']]

    return df


if __name__ == '__main__':
    test_df = gather_df(2024)
    print(test_df)
