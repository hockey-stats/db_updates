from argparse import ArgumentParser
from datetime import datetime
import duckdb

import process_skater_data
import process_team_data


TEAM_GAMES = 'https://moneypuck.com/moneypuck/playerData/careers/gameByGame/all_teams.csv'
TEAM_SEASON = 'https://moneypuck.com/moneypuck/playerData/seasonSummary/{}/regular/teams.csv'
SKATER_SEASON = 'https://moneypuck.com/moneypuck/playerData/seasonSummary/{}/regular/skaters.csv'

DB_NAME = 'hockey-stats.db'


def main(season: int) -> None:
    """
    This script is designed to be run every morning within a GitHub Actions workflow. 
    
    It works by pulling CSV data from MoneyPuck into dataframes and then using those to
    update tables in the DuckDB database, which is stored as a GitHub Artifact.

    :param int season: NHL season for which to pull data
    """

    conn = duckdb.connect(database=DB_NAME, read_only=False)

    skater_df = process_skater_data.gather_df(season)
    team_df = process_team_data.gather_df(season)

    for df, table_name in zip([skater_df, team_df],
                              ['skaters', 'teams']):

        conn.execute(f'CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df;')


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-s', '--season', type=int,
                        default=datetime.now().year - 1 if datetime.now().month < 10 \
                                else datetime.now().year,
                        help='Season for which we pull data')
    args = parser.parse_args()

    main(season=args.season)
