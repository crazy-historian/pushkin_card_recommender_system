import random
import pandas as pd
import re

from datetime import datetime
from typing import Optional

from sandbox import config as cfg


def get_region_code(region_name: str, region_numbers: dict) -> Optional[int]:
    try:
        return region_numbers[region_name]
    except KeyError:
        return None

def get_org_id(org_name: str, org_dict: dict) -> Optional[int]:
    try:
        return org_dict[org_name]
    except KeyError:
        return None

def get_user_dataframe(users_data_path: str,
                       uniq_file_path: str,
                       region_file_path: str,
                       region_nums_file_path: str) -> pd.DataFrame:
    users_df = pd.read_csv(users_data_path, sep=';')
    uniq_df = pd.read_csv(uniq_file_path, sep=';')
    region_df = pd.read_csv(region_file_path, sep=';')
    region_nums = pd.read_csv(region_nums_file_path)

    users_df['create_date'] = pd.to_datetime(
        [datetime.fromtimestamp(x).strftime('%Y-%m-%d') for x in users_df['create_date']])
    uniq_df['create_date'] = pd.to_datetime(uniq_df['create_date'])
    uniq_df['user_phone_details_id'] = uniq_df['user_phone_details'].apply(lambda x:
                                                                           re.sub('(\..*)', '', x.replace('iOS ', ''))
                                                                           if 'iOS' in x
                                                                           else re.sub('\ \(.*', '', x))
    uniq_df['user_phone_details_id'] = uniq_df['user_phone_details_id'].apply(lambda x:
                                                                              re.sub('(\..*)', '', x)
                                                                              if 'Android' in x
                                                                              else x)
    uniq_df['type_phone'] = uniq_df['user_phone_details_id'].apply(lambda x:
                                                                   x.replace('iOS,', '2').split()[0]
                                                                   if 'iOS' in x
                                                                   else x.replace('Android,', '1').split()[0])

    users_df = users_df.merge(uniq_df, on=['user_id'], how='left')
    users_df = users_df.merge(region_df, on=['user_id'], how='left')

    users_df['user_birth'] = pd.to_datetime(users_df['user_birth'], yearfirst=True,
                                            infer_datetime_format=True)
    users_df['age'] = users_df['user_birth'].apply(lambda x: int(round((datetime.now() - x).days / 365.2425, 0)))

    region_nums = region_nums.rename(columns={
        'Наименование субъекта': 'region_name',
        'Код ГИБДД': 'region_code'
    })
    region_nums_dict = dict(zip(region_nums.region_name, region_nums.region_code))
    users_df.loc[users_df.region.notna(), ['user_region']] = users_df['region'].dropna().\
        apply(func=get_region_code, region_numbers=region_nums_dict)

    users_df = users_df.rename(columns={'create_date_x': 'account_create_date', 'create_date_y': 'app_open_date'})
    users_df['diff'] = users_df['app_open_date'] - users_df['account_create_date']

    clicked_users_df = users_df[users_df['buyer_mobile_phone'].notna()]
    clicked_users = clicked_users_df.groupby(['user_id'])['app_open_date'].count().reset_index().rename(
        columns={'app_open_date': 'in_count'})
    clicked_users = clicked_users_df.merge(clicked_users, on=['user_id'], how='left')
    users_full = clicked_users.drop_duplicates(subset=['user_id'], ignore_index=True)

    return users_full

def get_organization_dataframe(organizations_data_path: str) -> pd.DataFrame:
    organization_df = pd.read_csv(organizations_data_path, sep=';')
    return organization_df

def get_click_datframe(clicks_data_path: str) -> pd.DataFrame:
    click_df = pd.read_csv(clicks_data_path, sep=';', encoding= 'unicode_escape', error_bad_lines=False)
    click_df = click_df.drop_duplicates(['create_time', 'user_id'])
    return click_df

def get_event_dataframe(events_data_path: str,
                        all_events_data_path: str,
                        organizations_dataframe: pd.DataFrame) -> pd.DataFrame:
    events_df = pd.read_csv(events_data_path, sep=';', error_bad_lines=False)
    events_df = events_df.rename(columns={'Ссылка на покупку билета': 'url',
                                    'ID': 'session_id',
                                    'Название события': 'session_name',
                                    'Дополнительная ссылка на покупку билета': 'additional_url',
                                    'Организатор мероприятия': 'organization_name'})

    events_df['session_identity'] = events_df['session_name'].apply(lambda x: x.split()[0].strip())
    events_df = events_df.loc[(events_df['url'].notna()) | (events_df['additional_url'].notna())]

    events_pushka_accepted_30122021_df = pd.read_csv(all_events_data_path, sep=',')
    events_pushka_accepted_30122021_df = events_pushka_accepted_30122021_df[
        (events_pushka_accepted_30122021_df['entity.saleLink'].notna()) |
        (events_pushka_accepted_30122021_df['entity.additionalSaleLinks.0'])]

    events_pushka_accepted_30122021_df['session_identity'] = events_pushka_accepted_30122021_df['entity.name'
    ].apply(lambda x:
            x.split()[0].strip())

    events_pushka_accepted_30122021_df = events_pushka_accepted_30122021_df.\
        rename(columns={'entity.saleLink': 'url',
                        'entity._id': 'session_id',
                        'entity.name': 'session_name',
                        'entity.additionalSaleLinks.0': 'additional_url',
                        'entity.organization._id': 'organization_id',
                        'entity.organization.name': 'organization_name'})

    events_all_df = pd.concat([events_pushka_accepted_30122021_df, events_df])
    events_all_df = events_all_df.drop_duplicates(['session_id', 'url', 'additional_url'])

    org_id_dict = dict(zip(organizations_dataframe['Учреждение'], organizations_dataframe['ID']))

    events_all_df['organization_id'].fillna(
        events_all_df[events_all_df['organization_id'].isnull()]['organization_name'].apply(func=get_org_id,
                                                                                      org_dict=org_id_dict),
        inplace=True)
    events_all_df = events_all_df.astype({"organization_id": "int"})

    events_1 = events_all_df[events_all_df['additional_url'].isnull()].drop('additional_url', axis=1)
    events_2 = events_all_df[events_all_df['url'] == events_all_df['additional_url']].drop('additional_url', axis=1)
    events_3 = events_all_df.loc[events_all_df['additional_url'].notna()].drop_duplicates(
        subset=['additional_url', 'url']).drop('additional_url', axis=1)
    events_4 = events_all_df.loc[events_all_df['additional_url'].notna()].drop_duplicates(
        subset=['additional_url', 'url']).drop('url', axis=1).rename(columns={'additional_url': 'url'})

    event = pd.concat([events_1, events_2, events_3, events_4], ignore_index=True)
    event.drop_duplicates(inplace=True)

    return event

def get_merged_click_event_dataframe(events_dataframe: pd.DataFrame,
                                     clicks_dataframe: pd.DataFrame,
                                     organizations_dataframe: pd.DataFrame,
                                     users_dataframe) -> pd.DataFrame:
    events_links = events_dataframe['url'].unique()
    clicks_links = clicks_dataframe['url'].unique()

    equal_links = []

    for i in clicks_links:
        for j in events_links:
            if i == j:
                equal_links.append(i)

    click_filter = clicks_dataframe['url'].isin(equal_links)
    clicks_dataframe = clicks_dataframe[click_filter]

    clicks_add = pd.merge(clicks_dataframe, events_dataframe, how='left', on='url')
    organizations_dataframe = organizations_dataframe.rename(
        columns= {'ID': 'organization_id','Категория': 'org_category', 'Код': 'org_region_number'})
    clicks_add = pd.merge(clicks_add, organizations_dataframe[['organization_id', 'org_region_number', 'org_category']],
                          how = 'left', on = 'organization_id')
    clicks_add = clicks_add[clicks_add['org_category'].notna()]
    clicks_add['type'] = clicks_add[['org_category', 'session_identity']].apply(lambda x: '_'.join(x), axis=1)

    clicks_add = pd.merge(clicks_add, users_dataframe[['user_id', 'age', 'user_region']],
                         how='left', on='user_id')
    clicks_add = clicks_add[clicks_add['user_region'].notna()]

    return clicks_add

def save_to_csv(dataframe: pd.DataFrame, path: str):
    dataframe.to_csv(path + 'cliks_add_3.csv', sep=';', index=False)

if __name__ == "__main__":
    random.seed(43)

    users_file = cfg.USERS_FILE_PATH
    uniq_file = cfg.UNIQ_FILE_PATH
    region_file = cfg.REGION_FILE_PATH
    region_nums_file = cfg.REGION_NUMS_FILE_PATH
    users_full = get_user_dataframe(users_file, uniq_file, region_file, region_nums_file)

    organizations_file = cfg.ORGANIZATIONS_FILE_PATH
    organizations = get_organization_dataframe(organizations_file)

    clicks_file = cfg.CLICK_FILE_PATH
    clicks = get_click_datframe(clicks_file)

    events_file = cfg.EVENTS_FILE_PATH
    all_events_file = cfg.ALL_EVENTS_FILE_PATH
    events = get_event_dataframe(events_file, all_events_file, organizations)

    full_dataframe = get_merged_click_event_dataframe(events, clicks, organizations, users_full)

    path = cfg.MAIN_PATH
    save_to_csv(full_dataframe, path)










