import pytest
import pandas as pd
from itertools import combinations
from pipeline import filtering as flt

region_codes = pd.read_csv('test_data/RegionRussia.csv')
region_codes = region_codes.rename(columns={
    'Наименование субъекта': 'region_name',
    'Код ГИБДД': 'region_code'
})
region_codes = list(region_codes.region_code)
region_codes = list(combinations(region_codes, 2))

user_event_df = pd.read_csv('test_data/user_event_df.csv')
user_df = pd.read_csv('test_data/user_df.csv')
event_df = pd.read_csv('test_data/events_df.csv')


@pytest.mark.parametrize('user_region_code, event_region_code', region_codes)
def test_filtering_user_event_df(
        user_region_code: int,
        event_region_code: int
):
    assert isinstance(flt.filter_user_event_df(
        user_event_df,
        user_df,
        event_df,
        user_region_code=user_region_code,
        event_region_code=event_region_code
    ), pd.DataFrame)
