import pandas as pd
import pytest
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