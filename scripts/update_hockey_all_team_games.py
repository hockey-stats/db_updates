"""
One-off script which can be used to add new columns to team_games table
"""

from datetime import datetime
import polars as pl
import duckdb


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

def main():
    df = pl.read_csv(DATA_URL, columns=USED_COLUMNS)

    df = df.filter(pl.col('playoffGame') == 0).drop(['playoffGame'])

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

    df = df.drop('home_or_away')

    # Rename a few columns to better align with the rest of the tables in the DB
    df = df.rename({
        'gameId': 'gameID',
        'xGoalsPercentage': 'xGoalsShare',
        'corsiPercentage': 'corsiShare',
        'penalityMinutesFor': 'penaltyMinutesFor',
        'penalityMinutesAgainst': 'penaltyMinutesAgainst'
    })

    conn = duckdb.connect(database='md:', read_only=False)

    # Have columns in correct order
    df = df[['team', 'season', 'gameID', 'gameDate', 'isHomeTeam', 'iceTime', 'situation',
             'xGoalsFor', 'xGoalsAgainst', 'xGoalsShare', 'corsiShare', 'goalsFor',
             'goalsAgainst', 'penaltyMinutesFor', 'penaltyMinutesAgainst']]

    conn.execute("""
                CREATE OR REPLACE TABLE team_games (
                    team VARCHAR,
                    season INT,
                    gameID INT,
                    gameDate DATE,
                    isHomeTeam BOOL,
                    iceTime FLOAT,
                    situation VARCHAR,
                    xGoalsFor FLOAT,
                    xGoalsAgainst FLOAT,
                    xGoalsShare FLOAT,
                    corsiShare FLOAT,
                    goalsFor INT,
                    goalsAgainst INT,
                    penaltyMinutesFor INT,
                    penaltyMinutesAgainst INT
                 );
                """)

    conn.execute("INSERT INTO team_games SELECT * FROM df")


if __name__ == '__main__':
    main()