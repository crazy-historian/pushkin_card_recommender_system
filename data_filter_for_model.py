import pandas as pd
from datetime import datetime
from typing import List, Union, Optional

path_input = input('Введите путь к исходным файлам: ')
#path_work = input('Введите путь к промежуточным файлам: ')
path_output = input('Введите путь к итоговым файлам: ')
user_rgn = int(input('Введите регион пользователя: '))
org_rgn = input('Введите регион организатора: ')
user_ag = int(input('Введите возвраст пользователя: '))

REGION_FILE_PATH = path_input + '/region.txt'
REGION_NUMS_FILE_PATH = path_input + '/RegionRussia.csv'
USERS_FILE_PATH = path_input + '/users.txt'
CLICK_FILE_PATH = path_input + '/click.txt'
EXPANDED_CLICK_FILE_PATH = path_input + '/cliks_add.csv'

ALL_EVENTS_FILE_PATH = path_input + '/events_pushka_accepted_30122021.csv'
ORGANIZATIONS_FILE_PATH = path_input + '/organizations.csv'

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


def get_user_dataframe() -> pd.DataFrame:
    users_df = pd.read_csv(USERS_FILE_PATH, sep=';')
    users_age = users_df
    users_age['user_age'] = users_age['user_birth'].apply(func=get_user_age_in_years)
    users_age_dict = dict(zip(users_age.user_id, users_age.user_age))

    users_regions_df = pd.read_csv(REGION_FILE_PATH, sep=';')
    region_nums = pd.read_csv(REGION_NUMS_FILE_PATH)
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


def get_events_dataframe() -> pd.DataFrame:
    all_events_df = pd.read_csv(ALL_EVENTS_FILE_PATH)
    organizations_df = pd.read_csv(ORGANIZATIONS_FILE_PATH, sep=';')

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

def get_clicks_dataframe() -> pd.DataFrame:
    clicks_df = pd.read_csv(EXPANDED_CLICK_FILE_PATH, sep=';')
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
                                  'session_name',
                                  'organization_name',
                                  'Org_region_number', 'org_category', 'age'
                                  ])
    clicks = clicks.rename(columns={'session_id': 'event_id'})
    clicks = clicks.drop_duplicates(['create_time', 'user_id'])
    clicks = clicks.dropna(subset=['event_id', 'organization_id'])
    return clicks


#########################

def get_user_event_dataframe() -> pd.DataFrame:
    user_event_df = get_clicks_dataframe()
    user_event = user_event_df.groupby(['user_id', 'event_id'])['create_time'].count().reset_index()
    user_event = user_event.rename(columns={'create_time': 'clicks_count'})
    return user_event


#########################

def filter_user_event_df_by_user_age(
        user_event_df: pd.DataFrame,
        user_df: pd.DataFrame,
        user_age: Optional[Union[int, list[int]]]
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
        user_region: Optional[Union[int, list[int]]]
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
        event_region: Optional[Union[str, list[str]]]
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


user_event = get_user_event_dataframe()
users = get_user_dataframe()

user_event = filter_user_event_df_by_user_age(
    user_event_df=user_event,
    user_df=users,
    user_age=user_ag
)
user_event = filter_user_event_by_user_region(
    user_event_df=user_event,
    user_df=users,
    user_region=user_rgn
)

events = get_events_dataframe()

user_event = filter_user_event_by_event_region(
    user_event_df=user_event,
    event_df=events,
    event_region=org_rgn#'г Москва'
)

print(user_event.info())
print(user_event.head())

user_event.to_csv(path_output + '/user_event.csv', sep=';', index=False)