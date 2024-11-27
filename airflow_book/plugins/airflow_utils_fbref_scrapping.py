from bookmaker.functions import fn_get_season_calendar, fn_get_database_game_ids, fn_compare_id, fn_insert_new_teams, \
                                fn_generate_game_reports, fn_insert_games_and_game_reports

def get_fixtures(competition_ids, seasons, max_gameweek):
    df_fbref_data = fn_get_season_calendar(competition_ids, seasons, max_gameweek)
    return df_fbref_data

def filter_new_games(competition_ids, seasons, df_fbref_data):
    df_games_db = fn_get_database_game_ids(competition_ids, seasons)
    df_new_games = fn_compare_id(df_games_db, df_fbref_data)
    return df_new_games

def insert_new_teams(df_new_games):
    fn_insert_new_teams(df_new_games)

def insert_games_and_reports(df_new_games):
    games_per_batch = 10
    total_games = df_new_games.shape[0]
    batch_nb = (total_games + games_per_batch - 1) // games_per_batch

    for batch_index in range(batch_nb):
        start_index = batch_index * games_per_batch
        end_index = min(start_index + games_per_batch, total_games)
        batch_df = df_new_games[start_index:end_index]
        
        print(f'Processing batch {batch_index + 1}/{batch_nb}.')
        df_new_game_reports = fn_generate_game_reports(batch_df)
        fn_insert_games_and_game_reports(batch_df, df_new_game_reports)
        print(f'Batch {batch_index + 1}/{batch_nb} inserted.')

    return f'All {total_games} games and game reports inserted.'

