import pandas as pd
import requests
from bs4 import BeautifulSoup
import re


def fn_get_season_calendar(competition_ids:list, seasons:list, max_gameweek:int=100):
    """
    Function that returns the fixtures of a season of a competition
    Takes as input the competition id and the season in the format 'yyyy-yyyy' or 'yyyy'
    """
    df_games = pd.DataFrame()

    # Loop over the competitions and seasons
    for competition_id in competition_ids:
        for season in seasons:
            # url of season calendar :
            url = f'https://fbref.com/en/comps/{competition_id}/{season}/schedule/'

            # Initializing parser
            response = requests.get(url)
            soup = BeautifulSoup(response.content, "html.parser")

            # Read table with pandas
            df_temp = pd.read_html(url, 
                            attrs={"id":f"sched_{season}_{competition_id}_1"}
                            )
            df_fixtures = df_temp[0].dropna(how='all').reset_index(drop=True)

            # Table cleaning
            df_fixtures = df_fixtures[['Wk', 'Date', 'Home', 'Score', 'Away']]
            df_fixtures.rename(columns={'Wk': 'gameweek',
                                    'Date': 'date',
                                    'Home': 'home_team',
                                    'Score': 'score',
                                    'Away': 'away_team'}, inplace=True)
            df_fixtures['gameweek'] = df_fixtures['gameweek'].astype(int)
            df_fixtures['date'] = pd.to_datetime(df_fixtures['date'])
            

            # Get the fixtures table html code to extract the game & teams ids
            table = soup.find(id=f'sched_{season}_{competition_id}_1').find("tbody")

            game_ids = []
            home_ids = []
            away_ids = []
            squad_id_pattern = r"/squads/([a-f0-9]+)/"
            game_id_pattern = r"/matches/([a-f0-9]+)/"

            # Loop through the rows of the table
            for game in table.find_all("tr"):
                # Skip rows with spacer
                if ("spacer" in game.get('class', []) and "partial_table" in game.get('class', []) and "result_all" in game.get('class', [])): 
                    continue

                # Extract match report / home / away URLs
                match_report = game.find('td', class_='left', attrs={'data-stat': 'match_report'})
                home_team = game.find('td', class_='right', attrs={'data-stat': 'home_team'})
                away_team = game.find('td', class_='left', attrs={'data-stat': 'away_team'})

                # Check if the match_report element is found
                if match_report:
                    mr = match_report.find('a').get('href')
                    ht = home_team.find('a').get('href')
                    at = away_team.find('a').get('href')
                    # extract game & teams ids using regex
                    game_id = re.search(game_id_pattern, mr)
                    home_id = re.search(squad_id_pattern, ht)
                    away_id = re.search(squad_id_pattern, at)
                    # Store ids
                    game_ids.append(game_id.group(1) if game_id else '')
                    home_ids.append(home_id.group(1) if home_id else '')
                    away_ids.append(away_id.group(1) if away_id else '')
            
            # Create a DataFrame from the list of match id
            df_id = pd.DataFrame({
                            'id': game_ids, 
                            'home_id': home_ids,
                            'away_id': away_ids
                            })
            
            # Check on the number of games and ids
            if df_fixtures.shape[0] != df_id.shape[0]:
                raise ValueError(f'The number of games and ids do not match for competition_id {competition_id} and season {season}')

            # Concatenate the fixtures and ids
            df_season_games = pd.concat([df_fixtures, df_id], axis=1)
            df_season_games['season'] = season
            df_season_games['competition_id'] = competition_id

            df_season_games = df_season_games[df_season_games['gameweek'] <= max_gameweek]

            # Concatenate the different seasons
            df_games = pd.concat([df_games, df_season_games], ignore_index=True)


    return df_games




def fn_get_game_report(game_id: str, home_team_id: str, away_team_id: str):
    """
    Function that takes game_id, home_id and away_id to generate game reports for both teams
    """
    # Url parameter
    game_url = 'https://fbref.com/en/matches/' + game_id

    # Initializing parser
    response = requests.get(game_url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Way to get home possession
    str_home_poss = soup.find(id='team_stats').find('td').find('div').find('div').string
    home_poss = float(str_home_poss.strip('%')) / 100

    df_output = pd.DataFrame()

    for team_id in [home_team_id, away_team_id]:
        dict_report = {'id': game_id + '_' + team_id,
                       'game_id': game_id,
                       'team_id': team_id }

        # Get location and possession
        if team_id==home_team_id:
            dict_report['location'] = 'home'
            dict_report['possession'] = home_poss
        else:
            dict_report['location'] = 'away'
            dict_report['possession'] = (1-home_poss)

        # General stats
        tag_general_stats = f'stats_{team_id}_summary'
        soup.find(id=tag_general_stats).find('tfoot')

        dict_report['goals'] = int(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'goals'}).string)
        dict_report['assists'] = int(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'assists'}).string)
        dict_report['pens_scored'] = int(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'pens_made'}).string)
        dict_report['pens_att'] = int(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'pens_att'}).string)
        dict_report['shots'] = int(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'shots'}).string)
        dict_report['shots_on_target'] = int(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'shots_on_target'}).string)
        dict_report['touches'] = int(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'touches'}).string)
        dict_report['xg'] = float(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'xg'}).string)
        dict_report['npxg'] = float(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'npxg'}).string)
        dict_report['xg_assist'] = float(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'xg_assist'}).string)
        dict_report['sca'] = float(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'sca'}).string)
        dict_report['gca'] = float(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'gca'}).string)
        dict_report['passes'] = int(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'passes'}).string)
        dict_report['passes_completed'] = int(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'passes_completed'}).string)
        dict_report['progressive_passes'] = int(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'progressive_passes'}).string)
        dict_report['carries'] = int(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'carries'}).string)
        dict_report['progressive_carries'] = int(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'progressive_carries'}).string)
        dict_report['take_ons'] = int(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'take_ons'}).string)
        dict_report['take_ons_won'] = int(soup.find(id=tag_general_stats).find('tfoot').find("td", attrs={'data-stat': 'take_ons_won'}).string)

        # Passing stats
        tag_passing_stats = f'stats_{team_id}_passing'
        soup.find(id=tag_passing_stats).find('tfoot')

        dict_report['pass_xa'] = float(soup.find(id=tag_passing_stats).find('tfoot').find("td", attrs={'data-stat': 'pass_xa'}).string)
        dict_report['pass_final_third'] = int(soup.find(id=tag_passing_stats).find('tfoot').find("td", attrs={'data-stat': 'passes_into_final_third'}).string)

        # Passing type stats
        tag_passing_type_stats = f'stats_{team_id}_passing_types'
        soup.find(id=tag_passing_type_stats).find('tfoot')

        dict_report['corner_kicks'] = int(soup.find(id=tag_passing_type_stats).find('tfoot').find("td", attrs={'data-stat': 'corner_kicks'}).string)
        dict_report['crosses'] = int(soup.find(id=tag_passing_type_stats).find('tfoot').find("td", attrs={'data-stat': 'crosses'}).string)

        # Defensive stats
        tag_defensive_stats = f'stats_{team_id}_defense'
        soup.find(id=tag_defensive_stats).find('tfoot')

        dict_report['tackles'] = int(soup.find(id=tag_defensive_stats).find('tfoot').find("td", attrs={'data-stat': 'tackles'}).string)
        dict_report['tackles_won'] = int(soup.find(id=tag_defensive_stats).find('tfoot').find("td", attrs={'data-stat': 'tackles_won'}).string)
        dict_report['challenges'] = int(soup.find(id=tag_defensive_stats).find('tfoot').find("td", attrs={'data-stat': 'challenges'}).string)
        dict_report['challenges_success'] = int(soup.find(id=tag_defensive_stats).find('tfoot').find("td", attrs={'data-stat': 'challenge_tackles'}).string)
        dict_report['blocks'] = int(soup.find(id=tag_defensive_stats).find('tfoot').find("td", attrs={'data-stat': 'blocks'}).string)
        dict_report['interceptions'] = int(soup.find(id=tag_defensive_stats).find('tfoot').find("td", attrs={'data-stat': 'interceptions'}).string)
        dict_report['errors'] = int(soup.find(id=tag_defensive_stats).find('tfoot').find("td", attrs={'data-stat': 'errors'}).string)
        dict_report['clearances'] = int(soup.find(id=tag_defensive_stats).find('tfoot').find("td", attrs={'data-stat': 'clearances'}).string)

        # Possession stats
        tag_possession_stats = f'stats_{team_id}_possession'
        soup.find(id=tag_possession_stats).find('tfoot')

        dict_report['touches_def_pen_area'] = int(soup.find(id=tag_possession_stats).find('tfoot').find("td", attrs={'data-stat': 'touches_def_pen_area'}).string)
        dict_report['touches_def_3rd'] = int(soup.find(id=tag_possession_stats).find('tfoot').find("td", attrs={'data-stat': 'touches_def_3rd'}).string)
        dict_report['touches_mid_3rd'] = int(soup.find(id=tag_possession_stats).find('tfoot').find("td", attrs={'data-stat': 'touches_mid_3rd'}).string)
        dict_report['touches_att_3rd'] = int(soup.find(id=tag_possession_stats).find('tfoot').find("td", attrs={'data-stat': 'touches_att_3rd'}).string)
        dict_report['touches_att_pen_area'] = int(soup.find(id=tag_possession_stats).find('tfoot').find("td", attrs={'data-stat': 'touches_att_pen_area'}).string)

        # Miscellaneous stats
        tag_misc_stats = f'stats_{team_id}_misc'
        soup.find(id=tag_misc_stats).find('tfoot')

        dict_report['aerials_won'] = int(soup.find(id=tag_misc_stats).find('tfoot').find("td", attrs={'data-stat': 'aerials_won'}).string)
        dict_report['aerials_lost'] = int(soup.find(id=tag_misc_stats).find('tfoot').find("td", attrs={'data-stat': 'aerials_lost'}).string)
        dict_report['cards_yellow'] = int(soup.find(id=tag_misc_stats).find('tfoot').find("td", attrs={'data-stat': 'cards_yellow'}).string)
        dict_report['cards_red'] = int(soup.find(id=tag_misc_stats).find('tfoot').find("td", attrs={'data-stat': 'cards_red'}).string)
        dict_report['cards_yellow_red'] = int(soup.find(id=tag_misc_stats).find('tfoot').find("td", attrs={'data-stat': 'cards_yellow_red'}).string)
        dict_report['fouls'] = int(soup.find(id=tag_misc_stats).find('tfoot').find("td", attrs={'data-stat': 'fouls'}).string)
        dict_report['fouled'] = int(soup.find(id=tag_misc_stats).find('tfoot').find("td", attrs={'data-stat': 'fouled'}).string)
        dict_report['pens_conceded'] = int(soup.find(id=tag_misc_stats).find('tfoot').find("td", attrs={'data-stat': 'pens_conceded'}).string)

        # Keeper stats
        tag_keeper_stats = f'keeper_stats_{team_id}'
        dict_report['shots_on_target_against'] = int(soup.find(id=tag_keeper_stats).find("td", attrs={'data-stat': 'gk_shots_on_target_against'}).string)
        dict_report['goals_against'] = int(soup.find(id=tag_keeper_stats).find("td", attrs={'data-stat': 'gk_goals_against'}).string)
        dict_report['gk_saves'] = int(soup.find(id=tag_keeper_stats).find("td", attrs={'data-stat': 'gk_saves'}).string)
        dict_report['gk_psxg'] = float(soup.find(id=tag_keeper_stats).find("td", attrs={'data-stat': 'gk_psxg'}).string)
        
        # Append report to DataFrame
        df_output = pd.concat([df_output, pd.DataFrame([dict_report])], ignore_index=True)

    return df_output