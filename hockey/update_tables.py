import datetime
import zipfile
import os
import json
from argparse import ArgumentParser

import duckdb
import requests

import process_skater_data
import process_team_data
import process_game_data


############## Constants ################

#DB_NAME = 'hockey-stats.db'
DB_NAME = 'md:'

########### End Constants ###############


def download_database() -> None:
    """
    Function to download the artifact containing the .db file from GitHub.

    Looks for the version of the artifact that was most recently updated, assuming that
    to be the most recent version of the database.

    Expects a GitHub PAT with proper permissions to be available as an environment variable (in
    this case provided by the GitHub Actions workflow).
    """
    url = "https://api.github.com/repos/hockey-stats/db_updates/actions/artifacts"
    payload = {}
    headers = {
        'Authorization': f'Bearer {os.environ["GITHUB_PAT"]}'
    }
    output_filename = 'db.zip'

    # Returns a list of every available artifact for the repo
    response = requests.request("GET", url, headers=headers, data=payload, timeout=10)
    response_body = json.loads(response.text)

    # Variable to store the most recent updated time for an artifact, for comparison
    most_recent_update = None
    download_url = ''

    print(response_body)
    for artifact in response_body['artifacts']:
        update_time = datetime.datetime.strptime(artifact['updated_at'], '%Y-%m-%dT%H:%M:%SZ')\
            .replace(tzinfo=datetime.timezone.utc)

        if most_recent_update is None or most_recent_update or update_time > most_recent_update:
            most_recent_update = update_time
            download_url = artifact['archive_download_url']

    if most_recent_update is None:
        # If no artifact was found, raise an error
        raise ValueError("No artifact found, exiting...")

    # Downloads the artifact as a zip file
    dl_response = requests.request("GET", download_url, headers=headers, data=payload, timeout=60)
    with open(output_filename, 'wb') as fo:
        fo.write(dl_response.content)

    # And unzip
    with zipfile.ZipFile(output_filename, 'r') as zip_ref:
        zip_ref.extractall(os.getcwd())

    print(os.listdir(os.getcwd()))

    print('Database download complete')


def main(season: int) -> None:
    """
    This script is designed to be run every morning within a GitHub Actions workflow. 
    
    It works by pulling CSV data from MoneyPuck into dataframes and then using those to
    update tables in the DuckDB database, which is stored as a GitHub Artifact.

    :param int season: NHL season for which to pull data
    """

    print('Downloading database artifact...')
    #download_database()

    print('Gathering skater data...')
    skater_df = process_skater_data.gather_df(season)

    print('Gathering team data...')
    team_df = process_team_data.gather_df(season)

    print('Gathering game-by-game team data...')
    team_games_df = process_game_data.gather_df(season)

    print('Connecting to database...')
    conn = duckdb.connect(database=DB_NAME, read_only=False)

    for df, table_name in zip([skater_df, team_df, team_games_df],
                              ['skaters', 'teams', 'team_games']):

        print(f"Updating {table_name} table...")
        conn.execute(f'CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df;')

    print('Database update complete!')


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-s', '--season', type=int,
                        default=datetime.datetime.now().year - 1 if datetime.datetime.now().month < 10 \
                                else datetime.datetime.now().year,
                        help='Season for which we pull data')
    args = parser.parse_args()

    main(season=args.season)
