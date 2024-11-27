from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow_book.plugins.airflow_utils_fbref_scrapping import get_fixtures, filter_new_games, insert_new_teams, insert_games_and_reports

@dag(start_date=datetime(2024, 11, 20), 
     schedule_interval="0 4 * * 3", # Every Wednesday at 4:00 AM)
     catchup=False)
def dag_update_premier_league_data():

    competition_ids = [9]
    seasons = ['2023-2024']
    max_gameweek = 21    

    @task
    def task_get_fixtures(competition_ids, seasons, max_gameweek):
        return get_fixtures(competition_ids=competition_ids, seasons=seasons, max_gameweek=max_gameweek)
    
    @task
    def task_filter_new_games(competition_ids, seasons, df_fixtures):
        return filter_new_games(competition_ids=competition_ids, seasons=seasons, df_fbref_data=df_fixtures)

    @task
    def task_insert_new_teams(df_new_games):
        return insert_new_teams(df_new_games)

    @task
    def task_insert_games_and_reports(df_new_games):
        return insert_games_and_reports(df_new_games)

    # Define task dependencies
    fixtures = task_get_fixtures(competition_ids, seasons, max_gameweek)
    new_games = task_filter_new_games(competition_ids, seasons, fixtures)
    task_insert_new_teams(new_games)
    task_insert_games_and_reports(new_games)

dag_instance = dag_update_premier_league_data()