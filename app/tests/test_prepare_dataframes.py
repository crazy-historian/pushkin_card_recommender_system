import pytest
import pandas as pd

from app.pipeline import preprocessing as prs


@pytest.mark.parametrize('clicks_file_path', ['../../data/cliks_add.csv'])
def test_get_user_event_dataframe(
        clicks_file_path: str,
):
    assert isinstance(
        prs.get_user_event_dataframe(
            user_event_file_path=clicks_file_path
        ), pd.DataFrame)


@pytest.mark.parametrize('user_file_path, regions_file_path, regions_nums_file_path',
                         [('../../data/users.txt', '../../data/region.txt', '../../data/RegionRussia.csv')])
def test_get_user_dataframe(
        user_file_path: str,
        regions_file_path: str,
        regions_nums_file_path: str,
):
    assert isinstance(
        prs.get_user_dataframe(
            user_file_path,
            regions_file_path,
            regions_nums_file_path
        ), pd.DataFrame)


@pytest.mark.parametrize('event_file_path, organization_file_path',
                         [('../../data/events_pushka_accepted_30122021.csv', '../../data/organizations.csv')])
def test_get_event_dataframe(
        event_file_path: str,
        organization_file_path: str
):
    assert isinstance(
        prs.get_events_dataframe(
            event_file_path,
            organization_file_path
        ), pd.DataFrame
    )
