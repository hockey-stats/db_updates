import polars as pl
from urllib.error import HTTPError

############## Constants ################

# URL used to download CSV data from MoneyPuck
DATA_URL = 'https://moneypuck.com/moneypuck/playerData/seasonSummary/{}/regular/teams.csv'

# Columns that will be used from base CSV
USED_COLUMNS = ['season', 'team', 'situation', 'games_played', 'iceTime', 'goalsFor',
                'goalsAgainst', 'flurryScoreVenueAdjustedxGoalsFor',
                'flurryScoreVenueAdjustedxGoalsAgainst']

########### End Constants ###############


def fix_moneypuck_csv_header_issue(season: int) -> pl.DataFrame:
    """
    In rare cases, the CSV files provided by MoneyPuck will be missing a header row. This function
    contains logic to download the data, and subsitute a header row from another year, to create
    a new CSV with the proper header and read it into a DataFrame.

    :param int season: The season with the erroneous CSV.
    :return pl.DataFrame: The fixed CSV.
    """
    import requests

    with requests.Session() as s:
        # Download the header-less data from MoneyPuck
        download = s.get(DATA_URL.format(season))
        decoded = download.content.decode('utf-8')
        broken_data = decoded.splitlines()

        # We take all but the first entry per row, since the first entry corresponds to a
        # duplicated 'team' column
        broken_data = [row.split(',')[1:] for row in broken_data]

        # Download a known working version to get the proper header row from
        download = s.get(DATA_URL.format(2024))
        decoded = download.content.decode('utf-8')

        # This gives us all the headers in a comma-seperated single string
        working_header = decoded.splitlines()[0]

        # In the same way as above, remove the first entry from the headers and make it
        # a list of strings
        working_header = working_header.split(',')[1:]

    # And put it all together as a DataFrame
    df = pl.DataFrame(data=broken_data, schema=working_header, orient='row')[USED_COLUMNS]
    df = df.cast({
        'season': pl.Int64,
        'games_played': pl.Int64,
        'iceTime': pl.Float64,
        'goalsFor': pl.Float64,
        'goalsAgainst': pl.Float64,
        'flurryScoreVenueAdjustedxGoalsFor': pl.Float64,
        'flurryScoreVenueAdjustedxGoalsAgainst': pl.Float64
    })

    return df


def get_data_with_retries(data_url: str, season: int, columns: list[str], 
                          retries: int=3) -> pl.DataFrame:
    """
    If an HTTP error is raised when trying to pull the CSV, this function is called to
    try it again a few more times.

    Args:
        data_url (str): URL template to CSV file.
        season (int): Season for which to gather data.
        columns (list[str]): Columns to keep from the raw CSV.
        retries (int, optional): Max number of retries to try, Defaults to 3.

    Returns:
        pl.DataFrame: The data in the CSV.
    """
    i = 0
    while i <= retries:
        try:
            df = pl.read_csv(data_url.format(season), columns=columns)
            return df
        except HTTPError as e:
            print(e)
            print(f"Attempt #{i + 1} failed, retrying...")
            i += 1
    raise e


def gather_df(season: int) -> pl.DataFrame:
    """
    Script used to update tables containing team-level data.
    
    Pulls full data from MoneyPuck for a given season, saving only columns relevant to our plots,
    and stores in a polars DF. Then computes a few additional columns before updating the relevant
    table in the given database.

    :param int season: The season we'll be working with.
    """

    try:
        df = pl.read_csv(DATA_URL.format(season), columns=USED_COLUMNS)
    except pl.exceptions.ColumnNotFoundError:
        # As of Oct. 15 2025, the 2022 data has no header in the CSV, so apply a fix here
        df = fix_moneypuck_csv_header_issue(season)
    except HTTPError as e:
        print(e)
        df = get_data_with_retries(data_url=DATA_URL, season=season, columns=USED_COLUMNS)


    # Icetime is in seconds by default, convert to minutes
    df = df.with_columns(pl.col('iceTime') / 60.0)

    # Rename some columns to be nicer to work with
    df = df.rename({
        'games_played': 'gamesPlayed',
        'flurryScoreVenueAdjustedxGoalsFor': 'xGoalsFor',
        'flurryScoreVenueAdjustedxGoalsAgainst': 'xGoalsAgainst'
    })

    # Compute rate metrics from each column containing a total metric value,
    # i.e. goalsFor -> goalsForPerHour
    for total_col, rate_col in zip(['goalsFor', 'goalsAgainst', 'xGoalsFor', 'xGoalsAgainst'],
                                   ['goalsForPerHour', 'goalsAgainstPerHour',
                                    'xGoalsForPerHour', 'xGoalsAgainstPerHour']):

        df = df.with_columns((pl.col(total_col) * (60.0 / pl.col('iceTime'))).alias(rate_col))

    return df[['team', 'season', 'situation', 'gamesPlayed', 'iceTime', 'xGoalsFor', 'goalsFor',
               'xGoalsAgainst', 'goalsAgainst', 'goalsForPerHour', 'goalsAgainstPerHour',
               'xGoalsForPerHour', 'xGoalsAgainstPerHour']]


if __name__ == '__main__':
    test_df = gather_df(2022)
    with pl.Config(tbl_cols=40):
        print(test_df.filter(pl.col('situation') == '5on5'))
    test_df.write_csv('test.csv')
