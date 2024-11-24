from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow_book.plugins import get_fixtures, filter_new_games, insert_new_teams, insert_games_and_reports

with DAG("dag_update_premier_league_data", start_date=datetime(2024, 11, 20),
         schedule_interval="0 4 * * 3",  # Every Wednesday at 4:00 AM
         catchup=False) as dag:

    task_get_season_calendar = PythonOperator(
        task_id='get_fixtures',
        python_callable=get_fixtures,
        op_kwargs={'competition_ids': [9], 'seasons': ['2024-2025']}
    )

    task_filter_new_game_ids = PythonOperator(
        task_id='filter_new_games',
        python_callable=filter_new_games,
        provide_context=True,
    )

    task_insert_new_teams = PythonOperator(
        task_id='insert_new_teams',
        python_callable=insert_new_teams,
        provide_context=True
    )

    task_process_batches = PythonOperator(
        task_id='insert_games_and_reports',
        python_callable=insert_games_and_reports,
        provide_context=True
    )
    

    task_get_season_calendar >> task_filter_new_game_ids >> task_insert_new_teams >> task_process_batches