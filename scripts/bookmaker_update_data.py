#!/usr/bin/env python3
import re, sys
from datetime import datetime
from bookmaker.functions import (
    fn_get_seasons_calendars,
    fn_get_database_game_ids,
    fn_compare_id,
    fn_batch_scrapping_insert,
    fn_insert_new_teams
)

def validate_competition_ids(competition_ids):
    try:
        return [int(id) for id in competition_ids]
    except ValueError:
        raise ValueError("Competition IDs must be a list of integers.")

def validate_seasons(seasons):
    season_pattern = re.compile(r'^\d{4}-\d{4}$')
    for season in seasons:
        if not season_pattern.match(season):
            raise ValueError("Seasons must be in the format 'YYYY-YYYY'.")
    return seasons

def validate_max_gameweek(max_gameweek):
    if max_gameweek is not None:
        try:
            return int(max_gameweek)
        except ValueError:
            raise ValueError("Max gameweek must be an integer.")
    return None

def validate_use_proxy(use_proxy):
    if type(use_proxy) == bool:
        return use_proxy
    else:
        raise ValueError("Use_proxy should be a boolean.")


def main(competition_ids: list, seasons: list, max_gameweek: int = None, use_proxy: bool = True):
    # Parameters validation
    competition_ids = validate_competition_ids(competition_ids)
    seasons = validate_seasons(seasons)
    max_gameweek = validate_max_gameweek(max_gameweek)
    use_proxy = validate_use_proxy(use_proxy)

    print(f'Program executed at: {datetime.now().strftime("%H:%M:%S")}')
    # Calendars scrapping
    df_fbref_data = fn_get_seasons_calendars(competition_ids=competition_ids, seasons=seasons, max_gameweek=max_gameweek, use_proxy=use_proxy)
    
    # Get the corresponding games from the database
    df_games_db = fn_get_database_game_ids(competition_ids, seasons)

    # Check if there are new games
    df_new_games = fn_compare_id(df_games_db, df_fbref_data)
    if df_new_games.empty:
        message = 'No new game to insert.'
        print(message)
        return message
    else:
        print(f'{df_new_games.shape[0]} new game(s) to insert')
    
    # Create new teams if necessary
    fn_insert_new_teams(df_new_games)

    # Get and insert games & game_reports
    fn_batch_scrapping_insert(df_new_games, use_proxy=use_proxy)

    print(f'Program finished at: {datetime.now().strftime("%H:%M:%S")}')
    return 

if __name__ == "__main__":
    # Make competitions and seasons as list
    competition_ids = sys.argv[1].split(',')
    seasons = sys.argv[2].split(',')
    max_gameweek = sys.argv[3] if len(sys.argv) > 3 else None
    
    use_proxy = True # True by default

    main(competition_ids, seasons, max_gameweek, use_proxy)