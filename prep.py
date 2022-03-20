import pandas as pd
from scripts import data_management as dm

from scripts import config as cfg

#-*- coding: UTF-8 -*-
#from sys import argv
#script, user_region, event_region = argv
#user_region = int(user_region)

regions = pd.read_csv('region_for_model.csv', encoding = 'UTF-8-SIG', sep = ';')

for i in range(len(regions)):
    user_region = int(regions.iloc[i][0])
    event_region = regions.iloc[i][1]

    print(f'{user_region} region dataframe is being processed ...')

    #####################

    user_event_df = dm.get_user_event_dataframe(
        user_event_file_path=cfg.CLICKS_ADD_PATH
    )
    user_event_df

    #####################

    users_df = dm.get_user_dataframe(
        users_file_path=cfg.USERS_FILE_PATH,
        regions_file_path=cfg.REGION_FILE_PATH,
        regions_nums_file_path=cfg.REGION_NUMS_FILE_PATH
    )
    users_df

    ####################

    events_df = dm.get_events_dataframe(
        events_file_path=cfg.ALL_EVENTS_FILE_PATH,
        organizations_file_path=cfg.ORGANIZATIONS_FILE_PATH
    )
    events_df

    ###################

    df = (
        user_event_df.
        pipe(dm.filter_user_event_by_user_region, users_df, user_region).
        pipe(dm.filter_user_event_by_event_region, events_df, event_region)
    )

    df.to_csv(cfg.REGION_PATH + f'{user_region}_region_user_event_df.csv')