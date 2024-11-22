from airflow.operators.python_operator import PythonOperator

def get_fixtures(**kwargs):
    competition_ids = kwargs['competition_ids']
    seasons = kwargs['seasons']
    max_gameweek = kwargs.get('max_gameweek', None)
    df_fbref_data = fn_get_season_calendar(competition_ids, seasons, max_gameweek)
    kwargs['ti'].xcom_push(key='df_fbref_data', value=df_fbref_data)

def filter_new_games(**kwargs):
    competition_ids = kwargs['competition_ids']
    seasons = kwargs['seasons']
    df_fbref_data = kwargs['ti'].xcom_pull(key='df_fbref_data', task_ids='get_season_calendar')
    df_games_db = fn_get_database_game_ids(competition_ids, seasons)
    df_new_games = fn_compare_id(df_games_db, df_fbref_data)
    kwargs['ti'].xcom_push(key='df_new_games', value=df_new_games)

def insert_new_teams(**kwargs):
    df_new_games = kwargs['ti'].xcom_pull(key='df_new_games', task_ids='get_database_game_ids')
    fn_insert_new_teams(df_new_games)

def insert_games_and_reports(**kwargs):
    df_new_games = kwargs['ti'].xcom_pull(key='df_new_games', task_ids='get_database_game_ids')
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

