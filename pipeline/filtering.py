import numpy as np
import pandas as pd
from typing import List, Union, Optional


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


def get_extra_events_ids(user_event_df: pd.DataFrame, future_event_df: pd.DataFrame) -> List[int]:
    event_ids = set(user_event_df['event_id'])
    future_events_ids = set(future_event_df['ID'])
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


def split_df_into_diapasons(df: pd.DataFrame) -> pd.DataFrame:
    activity = df.groupby('user_id')['clicks_count'].count().sort_values(ascending=False).reset_index()
    activity['diapason'] = pd.cut(activity['clicks_count'], bins=np.linspace(0, 70, 15),
                                  labels=[f'({i}:{j}]' for i, j in zip(range(0, 70, 5), range(5, 75, 5))])
    return activity
