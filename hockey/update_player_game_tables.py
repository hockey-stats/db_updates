import os
import glob
from argparse import ArgumentParser

import duckdb
import polars as pl


############## Constants ################

DB_NAME = 'hockey-stats.db'

########### End Constants ###############


def process_skater_data(path: str, game_id: int) -> pl.DataFrame:
    """
    Processes raw data for skaters into a single DataFrame containing all the columns
    needed to create the post-game report. For each team there will be 8 CSVs, one for
    each of all strengths, 5v5, PP, and PK statistics in both individual and on-ice formats.

    Output DataFrame will have information from all 8, with each player having four rows
    for each game state that includes both the invididual and on-ice metrics.

    :param str path: Filepath to folder containing raw CSVs.
    :param str game_id: Game ID
    """

    final_df = pl.DataFrame()

    indiv_df = pl.DataFrame()
    for filename in glob.glob(os.path.join(path, f'*{game_id}*st.csv')):
        # Filename will be in the format
        #   date_gameID_team_state_(oi/st).csv
        # We only want the team name and state from this for the dataframe,
        # and then also get the date to use for the output filename.
        date, _, team, state, _ = os.path.basename(filename).split('_')

        df = pl.read_csv(filename)[['Player', 'Position', 'TOI', 'Goals', 'First Assists',
                                    'Second Assists', 'Shots', 'ixG']]

        df = df.with_columns(
            pl.lit(state).alias('state'),
            pl.lit(team).alias('team'),
            pl.lit(game_id).alias('game_id'),
            pl.lit(date).alias('game_date')
        ).cast(
            {'TOI': pl.Float64,
             'ixG': pl.Float64}
        )

        if len(indiv_df) == 0:
            indiv_df = df
        else:
            indiv_df = pl.concat([indiv_df, df])

    onice_df = pl.DataFrame()
    for filename in glob.glob(os.path.join(path, f'*{game_id}*oi.csv')):
        _, _, team, state, _ = os.path.basename(filename).split('_')

        df = pl.read_csv(filename)[['Player', 'Position', 'CF', 'CA', 'GF', 'GA',
                                    'xGF', 'xGA']]

        df = df.with_columns(
            pl.lit(state).alias('state'),
            pl.lit(team).alias('team'),
            ((pl.col('GF') / (pl.col('GF') + pl.col('GA'))) * 100).round(2).alias('GF_share'),
            ((pl.col('xGF') / (pl.col('xGF') + pl.col('xGA'))) * 100).round(2).alias('xGF_share'),
            ((pl.col('CF') / (pl.col('CF') + pl.col('CA'))) * 100).round(2).alias('CF_share'),
        ).cast(
            {'xGF': pl.Float64,
             'xGA': pl.Float64}
        )

        if len(onice_df) == 0:
            onice_df = df
        else:
            onice_df = pl.concat([onice_df, df])

    final_df = indiv_df.join(onice_df, on=['Player', 'team', 'state', 'Position'], how='right')
    final_df = final_df.rename({
        'Player': 'name',
        'Position': 'position',
        'TOI': 'icetime',
        'Goals': 'goals',
        'First Assists': 'primary_assists',
        'Second Assists': 'secondary_assists',
        'Shots': 'shots'
    })

    final_df = final_df.sort(by='name', descending=False).fill_nan(0)

    # Check for and handle an error with the data source where xG values are all given as 0
    col_sum = final_df['ixG'].sum()
    if col_sum == 0:
        raise ValueError("Expected Goal values sum to 0, issue with data source, exiting...")

    return final_df[['name', 'game_id', 'game_date', 'team', 'position', 'state', 'icetime',
                     'goals', 'primary_assists', 'secondary_assists', 'shots', 'ixG',
                     'GF', 'GA', 'GF_share', 'xGF', 'xGA', 'xGF_share', 'CF', 'CA', 'CF_share']]

def process_goalie_data(path, game_id):
    """
    Raw goalie data is provided as one CSV for each game state, per team. Combines all 8
    into one DataFrame and return it.

    :param str path: Filepath to folder containing raw CSVs.
    :param str game_id: Game ID
    """

    goalie_df = pl.DataFrame()
    for filename in glob.glob(os.path.join(path, f'*{game_id}*goalies.csv')):
        date, _, team, state, _ = os.path.basename(filename).split('_')

        df = pl.read_csv(filename)[['Player', 'TOI', 'Shots Against', 'Goals Against',
                                    'Expected Goals Against']]
        df = df.with_columns(
            pl.lit(team).alias('team'),
            pl.lit(state).alias('state'),
            pl.lit(game_id).alias('game_id'),
            pl.lit(date).alias('game_date')
        ).cast(
            {'TOI': pl.Float64,
             'Expected Goals Against': pl.Float64}
        )

        if len(goalie_df) == 0:
            goalie_df = df
        else:
            goalie_df = pl.concat([goalie_df, df])

    goalie_df = goalie_df.rename({
        'Player': 'name',
        'TOI': 'icetime',
        'Shots Against': 'SA',
        'Goals Against': 'GA',
        'Expected Goals Against': 'xGA'
    })

    return goalie_df[['name', 'game_id', 'game_date', 'team', 'state', 'icetime',
                      'SA', 'GA', 'xGA']]


def main(path, game_id):
    """
    Opens the CSV files containing raw game data from NaturalStatTrick, combines into two 
    dataframes (one for skaters, one for goalies), and saves them to CSVs to be used
    for plotting.
    :param str path: Path to directory containing raw CSV files.
    :param str game_id: ID for game that will be processed.
    """
    print("Processing raw skater and goalie data...")
    skater_df = process_skater_data(path, game_id)
    goalie_df = process_goalie_data(path, game_id)

    print('Connecting to database...')
    conn = duckdb.connect(database=DB_NAME, read_only=False)

    print("Updating skater table...")
    conn.execute("INSERT INTO skater_games SELECT * FROM skater_df")

    print("Updating goalie table...")
    conn.execute("INSERT INTO goalie_games SELECT * FROM goalie_df")

    print('Database update complete!')


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-p', '--path', default=os.path.join(os.getcwd(), 'data'),
                        help='Path to folder containing CSV data.')
    parser.add_argument('-g', '--game_id', required=True,
                        help='Game ID for which tables should be processed.')
    args = parser.parse_args()

    main(path=args.path, game_id=args.game_id)
