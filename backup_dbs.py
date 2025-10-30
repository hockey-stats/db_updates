"""
Script that creates backups of tables in the MotherDuck DB. Should be run before every DB update
to ensure no loss of data.
"""
import argparse

import duckdb


def main(source: str) -> None:
    """
    Script works by downloading the entire source table into a DataFrame, and then creating/
    replacing a backup table from that DataFrame.

    The name of every backup table will be f"backup_{source}".

    :param str source: Name of the source table that is being backed up.
    """

    conn = duckdb.connect('md:')

    print(f"Download data from {source}...")
    full_df = conn.sql(f'SELECT * FROM {source};').pl()

    print('Creating backup table...')
    conn.sql(f'CREATE OR REPLACE TABLE backup_{source} AS SELECT * FROM full_df')

    print('Backup complete!')
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source', required=True, type=str,
                        help='The source table for which a backup will be created/updated.')
    args = parser.parse_args()

    main(source=args.source)
