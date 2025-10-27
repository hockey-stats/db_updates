"""
One-off script for renaming all the columns in the NST tables (skater_games and goalie_games) to
better align with the naming conventions of the MoneyPuck tables.
"""

import duckdb
import polars as pl

def main():
    pl.Config(tbl_cols=40)

    connection = duckdb.connect('md:')

    # Start with skater_games

    s_df = connection.sql('SELECT * FROM skater_games').pl()

    s_df = s_df.rename({
        "state": "situation",
    })

    # Fix issue with NST using '\xa0' instead of a space in names
    s_df = s_df.with_columns(
        pl.col('name').str.replace_all('\xa0', ' ', literal=True)
    )

    print(s_df)
    print(s_df['name'][0])

    # And now goalie_games

    g_df = connection.sql('SELECT * FROM goalie_games').pl()

    g_df = g_df.rename({
        'state': 'situation',
    })

    g_df = g_df.with_columns(
        pl.col('name').str.replace_all('\xa0', ' ', literal=True)
    )

    print(g_df)
    print(g_df['name'][0])

    connection.sql('CREATE OR REPLACE TABLE skater_games AS SELECT * FROM s_df;')
    connection.sql('CREATE OR REPLACE TABLE goalie_games AS SELECT * FROM g_df;')

if __name__ == '__main__':
    main()
