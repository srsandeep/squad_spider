# from my_logger import logger

import logging
logger = logging.getLogger('crawl_squad_spider')
import pandas as pd
from glob import glob
import numpy as np

class ExtractPlayers:

    def __init__(self, commentry_path):
        self.data_path = commentry_path

    def extract_all_players(self):
        teams = pd.read_json('/home/sandeep/projects/wc2019/data/teams.json')
        teams['shortname'] = teams['name'].str.replace('-', '')
        teams.set_index('id', drop=True, inplace=True)

        team_player_df = pd.DataFrame()
        for each_match in glob('/home/sandeep/projects/wc2019/data/*[0-9].json'):
            players = pd.read_json(each_match)
            if 'batsman_striker_id' not in players or 'batting_side_team_id' not in players:
                continue
            else:
                players_df = players.loc[:, ['batsman_striker_id', 'batting_side_team_id']].drop_duplicates().set_index('batting_side_team_id', drop=True)
                players_df = players_df.merge(teams, how='left', left_index=True,right_index=True )
                team_player_df = team_player_df.append(players_df)
                team_player_df.drop_duplicates(inplace=True)
        team_player_df.dropna(inplace=True)
        team_player_df['batsman_striker_id'] = team_player_df['batsman_striker_id'].astype(int)
        team_player_df.index = team_player_df.index.astype(int)

        return team_player_df

    def create_player_links(self, team_player_df):
        team_player_df['link'] = 'http://www.espncricinfo.com/' + team_player_df['shortname'] + '/content/player/' + team_player_df['batsman_striker_id'].astype(str) + '.html'
        return team_player_df

if __name__ == "__main__":
    extract_obj = ExtractPlayers('/home/sandeep/projects/wc2019/data')
    players_df = extract_obj.extract_all_players()
    new_df = extract_obj.create_player_links(players_df)
    logger.info('Exiting')