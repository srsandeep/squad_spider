# from my_logger import logger

import logging
logger = logging.getLogger('crawl_squad_spider')
import pandas as pd
from glob import glob
import numpy as np

class ExtractPlayers:

    def __init__(self, commentry_path):
        self.data_path = commentry_path

    def get_player_links(self):
        batsman_df = self.extract_all_players(player_type='batsman')
        batsman_df = self.create_player_links(batsman_df)
        bowler_df = self.extract_all_players(player_type='bowler')
        bowler_df = self.create_player_links(bowler_df)
        nonstriker_df = self.extract_all_players(player_type='non-striker')
        nonstriker_df = self.create_player_links(nonstriker_df)
        all_player_links = list(set(batsman_df['link'].values.tolist() + \
                        bowler_df['link'].values.tolist() + \
                        nonstriker_df['link'].values.tolist()))
        return all_player_links

    def extract_all_players(self, player_type='batsman'):
        teams = pd.read_json('/home/sandeep/projects/wc2019/data/teams.json')
        teams['shortname'] = teams['name'].str.replace('-', '')
        teams.set_index('id', drop=True, inplace=True)

        team_player_df = pd.DataFrame()
        if player_type == 'batsman':
            playerid_col_name = 'batsman_striker_id'
            player_side_team_id = 'batting_side_team_id'
        elif player_type == 'bowler':
            playerid_col_name = 'bowler_id'
            player_side_team_id = 'bowling_side_team_id'
        elif player_type == 'non-striker':
            playerid_col_name = 'batsman_non_striker_id'
            player_side_team_id = 'batting_side_team_id'
        else:
            return team_player_df

        for each_match in glob('/home/sandeep/projects/wc2019/data/*[0-9].json'):
            players = pd.read_json(each_match)
            if playerid_col_name not in players or player_side_team_id not in players:
                continue
            else:
                players_df = players.loc[:, [playerid_col_name, player_side_team_id]].drop_duplicates().set_index(player_side_team_id, drop=True)
                players_df = players_df.merge(teams, how='left', left_index=True,right_index=True )
                team_player_df = team_player_df.append(players_df)
                team_player_df.drop_duplicates(inplace=True)
        team_player_df.dropna(inplace=True)
        team_player_df[playerid_col_name] = team_player_df[playerid_col_name].astype(int)
        team_player_df.index = team_player_df.index.astype(int)
        team_player_df.rename(columns={playerid_col_name: 'player_id'}, inplace=True)

        return team_player_df

    def create_player_links(self, team_player_df):
        team_player_df['link'] = 'http://www.espncricinfo.com/' + team_player_df['shortname'] + '/content/player/' + team_player_df['player_id'].astype(str) + '.html'
        return team_player_df

if __name__ == "__main__":
    extract_obj = ExtractPlayers('/home/sandeep/projects/wc2019/data')
    batsman_df = extract_obj.extract_all_players(player_type='batsman')
    batsman_df = extract_obj.create_player_links(batsman_df)
    bowler_df = extract_obj.extract_all_players(player_type='bowler')
    bowler_df = extract_obj.create_player_links(bowler_df)
    nonstriker_df = extract_obj.extract_all_players(player_type='non-striker')
    nonstriker_df = extract_obj.create_player_links(nonstriker_df)
    all_player_links = list(set(batsman_df['link'].values.tolist() + \
                    bowler_df['link'].values.tolist() + \
                    nonstriker_df['link'].values.tolist()))
    logger.info('Exiting')