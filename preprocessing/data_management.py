import pandas as pd
from datetime import datetime
from typing import List, Union, Optional

REGION_FILE_PATH = '../dataset/users_and_purchases/region.txt'
REGION_NUMS_FILE_PATH = '../dataset/RegionRussia.csv'
USERS_FILE_PATH = '../dataset/users_and_purchases/users.txt'
CLICK_FILE_PATH = '../dataset/users_and_purchases/click.txt'
EXPANDED_CLICK_FILE_PATH = './dataset/cliks_add.csv'

ALL_EVENTS_FILE_PATH = '../dataset/users_and_purchases/events/events_pushka_accepted_30122021.csv'
FUTURE_EVENTS = '../dataset/users_and_purchases/events/events.csv'
ORGANIZATIONS_FILE_PATH = '../dataset/users_and_purchases/events/organizations.csv'

DOWNLOAD_DATE = '2021-11-15'
DOWNLOAD_DATE = datetime.strptime(str(DOWNLOAD_DATE), "%Y-%m-%d")


def get_user_age_in_years(birth_date) -> int:
    date_1 = datetime.strptime(str(birth_date), "%Y-%m-%d")
    return abs((DOWNLOAD_DATE - date_1).days) // 365


def get_region_code(region_name: str, region_numbers: dict) -> Optional[int]:
    try:
        return region_numbers[region_name]
    except KeyError:
        return None


def get_user_age(user_id: str, user_age: dict) -> Optional[int]:
    try:
        return user_age[str(user_id)]
    except KeyError:
        return None


def get_user_dataframe(
        users_file_path: str,
        regions_file_path: str,
        regions_nums_file_path: str
) -> pd.DataFrame:
    users_df = pd.read_csv(users_file_path, sep=';')
    users_age = users_df
    users_age['user_age'] = users_age['user_birth'].apply(func=get_user_age_in_years)
    users_age_dict = dict(zip(users_age.user_id, users_age.user_age))

    users_regions_df = pd.read_csv(regions_file_path, sep=';')
    region_nums = pd.read_csv(regions_nums_file_path)
    region_nums = region_nums.rename(columns={
        'Наименование субъекта': 'region_name',
        'Код ГИБДД': 'region_code'
    })
    region_nums_dict = dict(zip(region_nums.region_name, region_nums.region_code))
    users_regions = users_regions_df
    users_regions['region_code'] = users_regions['region'].apply(func=get_region_code, region_numbers=region_nums_dict)

    users_full = users_regions
    users_full['age'] = users_full['user_id'].apply(func=get_user_age, user_age=users_age_dict)
    users_full = users_full.dropna(subset=['age'])
    return users_full


#########################

def get_region_from_address(address: str) -> str:
    return address.split(',')[0]


def get_event_region(org_id: int, regions_dict: dict) -> Optional[int]:
    try:
        return regions_dict[org_id]
    except KeyError:
        return None


def get_event_category(org_id: int, categories_dict) -> Optional[str]:
    try:
        return categories_dict[org_id]
    except KeyError:
        return None


def get_events_dataframe(
        events_file_path: str,
        organizations_file_path: str
) -> pd.DataFrame:
    all_events_df = pd.read_csv(events_file_path)
    organizations_df = pd.read_csv(organizations_file_path, sep=';')

    organizations = organizations_df
    organizations = organizations.rename(columns={
        'ID': 'org_id',
        'Учреждение': 'org_name',
        'Адрес': 'address',
        'ИНН': 'INN',
        'Категория': 'category'
    })
    organizations['region'] = organizations['address'].apply(func=get_region_from_address)
    org_id_region = dict(zip(organizations.org_id, organizations.region))
    org_id_category = dict(zip(organizations.org_id, organizations.category))

    all_events = all_events_df
    all_events = all_events.rename(columns={
        'entity._id': 'event_id',
        'entity.name': 'event_name',
        'entity.saleLink': 'link',
        'entity.additionalSaleLinks.0': 'add_link',
        'entity.organization._id': 'org_id',
        'entity.organization.name': 'org_name'
    })
    all_events = all_events.dropna(subset=['org_id'])
    all_events = all_events.astype({'org_id': int})

    all_events['region_name'] = all_events['org_id'].apply(func=get_event_region, regions_dict=org_id_region)
    all_events['category'] = all_events['org_id'].apply(func=get_event_category, categories_dict=org_id_category)
    return all_events


#########################

def get_clicks_dataframe(
        clicks_file_path: str
) -> pd.DataFrame:
    clicks_df = pd.read_csv(clicks_file_path, sep=';')
    clicks = clicks_df
    clicks = clicks.drop(columns=['url',
                                  'create_date',
                                  'user_region',
                                  'user_phone_details',
                                  'buyer_mobile_phone',
                                  'user_phone_details',
                                  'user_phone_details_id',
                                  'user_phone_details_id_2',
                                  'session_identity',
                                  'organization_name',
                                  'Org_region_number', 'org_category', 'age'
                                  ])
    clicks = clicks.rename(columns={
        'session_id': 'event_id',
        'session_name': 'event_name'
    })
    clicks = clicks.drop_duplicates(['create_time', 'user_id'])
    clicks = clicks.dropna(subset=['event_id', 'organization_id'])
    return clicks


#########################

def get_user_event_dataframe(
        user_event_file_path: str
) -> pd.DataFrame:
    user_event_df = get_clicks_dataframe(user_event_file_path)
    user_event = user_event_df.groupby(['user_id', 'event_id', 'event_name'])['create_time'].count().reset_index()
    user_event = user_event.rename(columns={'create_time': 'clicks_count'})
    user_event['event_id'] = user_event['event_id'].astype(int)
    return user_event


#########################

def filter_user_event_df_by_user_age(
        user_event_df: pd.DataFrame,
        user_df: pd.DataFrame,
        user_age: Optional[Union[int, List[int]]]
) -> pd.DataFrame:
    if isinstance(user_age, list) and len(user_age) == 2:
        users_by_age = user_df.loc[(user_df['age'] >= user_age[0]) |
                                   (user_df['age'] <= user_age[1])]
    elif isinstance(user_age, int):
        users_by_age = user_df.loc[user_df['age'] == user_age]
    else:
        raise ValueError("Incorrect value of user_age")

    user_id_set = set(users_by_age.user_id)
    user_event = user_event_df.loc[user_event_df['user_id'].isin(user_id_set)]
    return user_event


def filter_user_event_by_user_region(
        user_event_df: pd.DataFrame,
        user_df: pd.DataFrame,
        user_region: Optional[Union[int, List[int]]]
) -> pd.DataFrame:
    if isinstance(user_region, list):
        user_region = set(user_region)
        user_by_region = user_df.loc[(user_df['region_code'].isin(user_region))]
    elif isinstance(user_region, int):
        user_by_region = user_df.loc[user_df['region_code'] == user_region]

    else:
        raise ValueError("Incorrect value of user_region")

    user_id_set = set(user_by_region.user_id)
    user_event = user_event_df.loc[user_event_df['user_id'].isin(user_id_set)]
    return user_event


def filter_user_event_by_event_region(
        user_event_df: pd.DataFrame,
        event_df: pd.DataFrame,
        event_region: Optional[Union[str, List[str]]]
) -> pd.DataFrame:
    if isinstance(event_region, list):
        event_region = set(event_region)
        event_by_region = event_df.loc[event_df['region_name'].isin(event_region)]
    elif isinstance(event_region, str):
        event_by_region = event_df.loc[event_df['region_name'] == event_region]
    else:
        raise ValueError("Incorrect value of event_region")

    event_id_set = set(event_by_region.event_id)
    user_event = user_event_df.loc[user_event_df['event_id'].isin(event_id_set)]
    return user_event


def numerate_user_event_df(user_event_df: pd.DataFrame) -> pd.DataFrame:
    user_event_df['event_id'] = user_event_df['event_id'].astype('category')
    user_event_df['user_id'] = user_event_df['user_id'].astype('category')
    user_event_df['user_num'] = user_event_df['user_id'].cat.codes
    user_event_df['event_num'] = user_event_df['event_id'].cat.codes
    return user_event_df


def get_extra_events_ids(user_event_df: pd.DataFrame) -> List[int]:
    event_ids = set(user_event_df['event_id'])
    future_events_df = pd.read_csv(FUTURE_EVENTS, sep=';')
    future_events_ids = set(future_events_df['ID'])
    return list(event_ids.difference(future_events_ids))


def filter_user_event_df(
        user_event_df: pd.DataFrame,
        users_df: pd.DataFrame,
        events_df: pd.DataFrame,
        user_region_code: int,
        event_region_name: str) -> pd.DataFrame:
    return (
        user_event_df.
        pipe(filter_user_event_by_user_region, users_df, user_region_code).
        pipe(filter_user_event_by_event_region, events_df, event_region_name)
    )
