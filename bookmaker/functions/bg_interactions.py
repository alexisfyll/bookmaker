import pandas as pd
from pandas_gbq import read_gbq, to_gbq
from dotenv import load_dotenv
from bookmaker.functions import fn_get_game_report
import os
import time



# Load environment variables from .env file
load_dotenv()
project_id = os.getenv('PROJECT_ID')


def fn_compare_id(df_old_data: pd.DataFrame, df_new_data: pd.DataFrame):
    """
    Compare the 'id' columns of two dataframes and return the new data.

    Parameters:
    df_old_data (pd.DataFrame): The dataframe containing the old data. 
                                It must have a column named 'id'.
    df_new_data (pd.DataFrame): The dataframe containing the new data. 
                                It must have a column named 'id'.

    Returns:
    pd.DataFrame: A dataframe containing rows from df_new_data where the 'id' 
                  is not present in df_old_data.

    """
    return df_new_data[~df_new_data['id'].isin(df_old_data['id'])]



def fn_get_database_game_ids(competitions_ids: list, seasons: list):
    """
    Get the 'id' column from the BigQuery table for the given competitions and seasons.

    Parameters:
    bg_table (str): The name of the BigQuery table.
    competitions_ids (list): A list of competition ids.
    seasons (list): A list of seasons.

    Returns:
    pd.DataFrame: A dataframe containing the 'id' column for the given competitions and seasons.

    """
    competitions_formatted = [f"'{competition_id}'" for competition_id in competitions_ids]
    seasons_formatted = [f"'{season}'" for season in seasons]

    query = f"""
    SELECT id
    FROM `fbref_raw_data.games`
    WHERE 
        competition_id IN ({', '.join(competitions_formatted)}) 
        AND season IN ({', '.join(seasons_formatted)})
    """

    return read_gbq(query, project_id=project_id)


def fn_generate_game_reports(df_new_games: pd.DataFrame):
    """
    Generate the game_reports rows for the new games scrapped.

    Parameters:
    df_new_games (pd.DataFrame): A dataframe containing at least game_id, home_id, away_id.

    Returns:
    pd.DataFrame: A dataframe containing the game reports for the new games in the database.

    """
    df_game_reports = pd.DataFrame()
    for i in range (df_new_games.shape[0]):
        df_temp = fn_get_game_report(df_new_games['id'].iloc[i], df_new_games['home_id'].iloc[i], df_new_games['away_id'].iloc[i])
        df_game_reports = pd.concat([df_game_reports, df_temp], ignore_index=True)  
        time.sleep(5)

    return df_game_reports

def df_insert_games_and_game_reports(df_new_games: pd.DataFrame, df_new_game_reports: pd.DataFrame):
    """
    Insert the new games and game reports into the BigQuery table.

    Parameters:
    df_new_games (pd.DataFrame): A dataframe containing the new games.
    df_new_game_reports (pd.DataFrame): A dataframe containing the new game reports.

    """
    games_table='fbref_raw_data.games'
    game_reports_table='fbref_raw_data.game_reports'

    # Selection of right columns for games dataframe
    df_games_insert = df_new_games[['id', 'competition_id', 'date', 'season', 'gameweek']]
    
    to_gbq(df_games_insert, games_table, project_id=project_id, if_exists='append')
    to_gbq(df_new_game_reports, game_reports_table, project_id=project_id, if_exists='append')