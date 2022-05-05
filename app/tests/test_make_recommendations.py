import pandas as pd
import pytest
import json
from pathlib import Path
from app.pipeline import filtering as ftr
from app.recommenders.implicit_models import ALSRecommender, BPRRecommender


@pytest.mark.parametrize('user_event_df_file_path', ['../../data/user_event_dfs/user_77_event_77.csv'])
def test_als_recommender(user_event_df_file_path: str):
    user_event_df = pd.read_csv(user_event_df_file_path)
    user_event_df = ftr.numerate_user_event_df(user_event_df)
    user_event_df = user_event_df.rename(columns={
        'event_id': 'item_id',
        'event_num': 'item_num',
        'clicks_count': 'rating'
    }
    )
    recommender = ALSRecommender(
        confidence='alpha',
        alpha_value=15
    )
    recommender.fit(user_event_df)
    assert isinstance(recommender.get_all_recommendation(as_pd_dataframe=True), pd.DataFrame)


@pytest.mark.parametrize('user_event_df_file_path', ['../../data/user_event_dfs/user_77_event_77.csv'])
def test_bpr_recommender(user_event_df_file_path: str):
    user_event_df = pd.read_csv(user_event_df_file_path)
    user_event_df = ftr.numerate_user_event_df(user_event_df)
    user_event_df = user_event_df.rename(columns={
        'event_id': 'item_id',
        'event_num': 'item_num',
        'clicks_count': 'rating'
    }
    )
    recommender = BPRRecommender()
    recommender.fit(user_event_df)
    assert isinstance(recommender.get_all_recommendation(as_pd_dataframe=True), pd.DataFrame)


@pytest.mark.parametrize('user_df_filename, event_df_filename',
                         [('../../data/preprocessed/user_df.csv', '../../data/preprocessed/events_df.csv')])
def test_recommendation_ids(user_df_filename: str, event_df_filename: str):
    user_df = pd.read_csv(user_df_filename)
    event_df = pd.read_csv(event_df_filename)

    for file_name in Path('./data/recommendations').glob('*.json'):
        region_code = int(file_name.stem.split('_')[1])
        user_ids = set(user_df.loc[user_df.region_code == region_code]['user_id'].unique())
        event_ids = set(event_df.loc[event_df.region_code == region_code]['event_id'].unique())
        with open(file_name, 'r') as json_file:
            data = json.load(json_file)
            user_in_region_ids = set(data.keys())
            event_in_region_ids = set()
            for value in data.values():
                event_in_region_ids = event_in_region_ids.union(set(value.keys()))
            else:
                event_in_region_ids = set(map(int, event_in_region_ids))

            assert len(user_ids.intersection(user_in_region_ids)) == len(user_in_region_ids)
            assert len(event_ids.intersection(event_in_region_ids)) == len(event_in_region_ids)
