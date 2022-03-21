import pandas as pd
from datetime import datetime
from typing import List, Union, Optional

DOWNLOAD_DATE = '2021-11-15'
DOWNLOAD_DATE = datetime.strptime(str(DOWNLOAD_DATE), "%Y-%m-%d")


def get_user_age_in_years(birth_date) -> int:
    date_1 = datetime.strptime(str(birth_date), "%Y-%m-%d")
    return abs((DOWNLOAD_DATE - date_1).days) // 365


def get_region_code_for_user(region_name: str, region_numbers: dict) -> Optional[int]:
    try:
        return region_numbers[region_name]
    except KeyError:
        return None


def get_region_code_for_event(region_name: str) -> Optional[int]:
    region_numbers = {
        'Белгородская обл': 31,
        'Калужская обл': 40,
        'г Санкт-Петербург': 78,
        'Курганская обл': 45,
        'Нижегородская обл': 52,
        'Самарская обл': 63,
        'Ярославская обл': 76,
        'Свердловская обл': 66,
        'Тульская обл': 71,
        'Пермский край': 59,
        'г Москва': 77,
        'Тверская обл': 69,
        'Респ Карелия': 10,
        'Ульяновская обл': 73,
        'АО Ханты-Мансийский Автономный округ - Югра': 86,
        'Омская обл': 55,
        'Смоленская обл': 67,
        'Тюменская обл': 72,
        'Тамбовская обл': 68,
        'Московская обл': 50,
        'Кемеровская обл': 42,
        'Чувашская республика Чувашия': 21,
        'Респ Татарстан': 16,
        'Рязанская обл': 62,
        'Ставропольский край': 26,
        'Пензенская обл': 58,
        'Респ Бурятия': 3,
        'Новосибирская обл': 54,
        'Краснодарский край': 23,
        'Респ Марий Эл': 12,
        'Астраханская обл': 30,
        'Удмуртская Респ': 18,
        'Ивановская обл': 37,
        'Забайкальский край': 75,
        'Саратовская обл': 64,
        'Волгоградская обл': 34,
        'Респ Крым': 82,
        'Кировская обл': 43,
        'Челябинская обл': 74,
        'Приморский край': 25,
        'Ростовская обл': 61,
        'Владимирская обл': 33,
        'Красноярский край': 24,
        'Курская обл': 46,
        'Камчатский край': 41,
        'Респ Мордовия': 13,
        'Хабаровский край': 27,
        'Респ Дагестан': 5,
        'Респ Башкортостан': 2,
        'Томская обл': 70,
        'Респ Адыгея': 1,
        'Алтайский край': 22,
        'Орловская обл': 57,
        'Костромская обл': 44,
        'Вологодская обл': 35,
        'Ямало-Ненецкий ао': 89,
        'Оренбургская обл': 56,
        'Калининградская обл': 39,
        'Респ Хакасия': 19,
        'Респ Коми': 11,
        'г Севастополь': 92,
        'Липецкая обл': 48,
        'Респ Саха /Якутия/': 14,
        'Магаданская обл': 49,
        'Новгородская обл': 53,
        'Ямало-Ненецкий АО': 89,
        'Чеченская Респ': 95,
        'Мурманская обл': 51,
        'Респ Северная Осетия - Алания': 15,
        'Кабардино-Балкарская респ': 7,
        'Воронежская обл': 36,
        'респ Дагестан': 5,
        'Амурская обл': 28,
        'Кабардино-Балкарская Респ': 7,
        'Сахалинская обл': 65,
        'Брянская обл': 32,
        'Ленинградская обл': 47,
        'Архангельская обл': 29,
        'Иркутская обл': 38,
        'Карачаево-Черкесская Респ': 9,
        'Ханты-Мансийский Автономный округ - Югра': 86,
        'Респ Алтай': 4,
        'Респ Ингушетия': 6,
        'Ненецкий АО': 83,
        'респ Бурятия': 3,
        'Чеченская респ': 95,
        'Псковская обл': 60,
        'Респ Калмыкия': 8,
        'п Рязановское': 77,
        'Респ Тыва': 17,
        'респ Татарстан': 16,
        'п Кленовское': 77,
        'п Десеновское': 77,
        'п Михайлово-Ярцевское': 77,
        'Еврейская Аобл': 79,
        'респ Саха /Якутия/': 14,
        'респ Мордовия': 13,
        'п Новофедоровское': 77,
        'ао Ханты-Мансийский Автономный округ - Югра': 86,
        'респ Ингушетия': 6,
        'респ Башкортостан': 2,
        'респ Карелия': 10,
        'респ Марий Эл': 12,
        'обл Кемеровская область - Кузбасс': 42,
        'Удмуртская респ': 18
    }
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
    users_regions['region_code'] = users_regions['region'].apply(func=get_region_code_for_user,
                                                                 region_numbers=region_nums_dict)

    users_full = users_regions
    users_full['age'] = users_full['user_id'].apply(func=get_user_age, user_age=users_age_dict)
    users_full = users_full.dropna(subset=['age'])
    return users_full


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

    all_events['region_code'] = all_events['region_name'].apply(func=get_region_code_for_event)
    return all_events


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


def get_future_event_dataframe(
        future_event_file_path: str
) -> pd.DataFrame:
    return pd.read_csv(future_event_file_path, sep=';')


def get_user_event_dataframe(
        user_event_file_path: str
) -> pd.DataFrame:
    user_event_df = get_clicks_dataframe(user_event_file_path)
    user_event = user_event_df.groupby(['user_id', 'event_id', 'event_name'])['create_time'].count().reset_index()
    user_event = user_event.rename(columns={'create_time': 'clicks_count'})
    user_event['event_id'] = user_event['event_id'].astype(int)
    return user_event
