"""
One-off script for renaming all the columns in the NST tables (skater_games and goalie_games) to
better align with the naming conventions of the MoneyPuck tables.
"""

import duckdb
import polars as pl

def main():
    pl.Config(tbl_cols=40)

    connection = duckdb.connect('md:', read_only=False)

    # Start with skater_games

    s_df = connection.sql('SELECT * FROM skater_games').pl()

    for bad, good in zip(['FLAK', 'SJSS', 'LAKK', 'TBLL', 'NJDD'],
                         ['FLA', 'SJS', 'LAK', 'TBL', 'NJD']):
        s_df = s_df.with_columns(
            pl.col('team').str.replace_all(f'^{bad}$', good)
        )

    # And now goalie_games

    g_df = connection.sql('SELECT * FROM goalie_games').pl()

    for bad, good in zip(['FLAK', 'SJSS', 'LAKK', 'TBLL', 'NJDD'],
                         ['FLA', 'SJS', 'LAK', 'TBL', 'NJD']):
        s_df = s_df.with_columns(
            pl.col('team').str.replace_all(f'^{bad}$', good)
        )

    connection.sql('CREATE OR REPLACE TABLE skater_games AS SELECT * FROM s_df;')
    connection.sql('CREATE OR REPLACE TABLE goalie_games AS SELECT * FROM g_df;')

if __name__ == '__main__':
    main()
