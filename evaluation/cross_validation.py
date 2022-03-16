import pandas as pd
from sklearn.model_selection import ParameterGrid
from tqdm.notebook import tqdm_notebook
from typing import Tuple

from recommenders.implicit_models import UserItemRecommender


def evaluate(
        user_id: str,
        user_event_df: pd.DataFrame,
        recommender: UserItemRecommender,
        number_of_recommended=10
) -> float:
    clicked_by_user = user_event_df.loc[user_event_df.user_id == user_id]

    count = 0
    for _, clicked_event in clicked_by_user.iterrows():
        example = user_event_df[(user_event_df.user_id == clicked_event.user_id) &
                                (user_event_df.item_id == clicked_event.item_id)]
        us_ev = user_event_df.drop(example.index)
        recommender.fit(user_item_df=us_ev, show_progress=False)
        recommended_to_user = recommender.get_user_recommendation(user_id, number_of_recommended, as_pd_dataframe=True)
        if example.iloc[0].item_id in list(recommended_to_user.item_id.values):
            count += 1

    return round(count / len(clicked_by_user), 2)


def cross_validation(
        user_activity_df: pd.DataFrame,
        user_event_df: pd.DataFrame,
        recommender: UserItemRecommender,
        parameters: dict,
        num_of_clicked: int = 5,
        num_of_recommended: int = 10
) -> Tuple[UserItemRecommender, dict]:
    users_in_interval = user_activity_df.loc[
        (user_activity_df.diapason == '(0:5]') &
        (user_activity_df.clicks_count == num_of_clicked)
        ]

    user_from_interval = users_in_interval.sample().iloc[0]
    best_params = dict()
    best_recommender = None
    max_probability = 0
    for params in tqdm_notebook(ParameterGrid(parameters)):
        rec = recommender(**params)
        probability = evaluate(
            user_id=user_from_interval.user_id,
            user_event_df=user_event_df,
            recommender=rec,
            number_of_recommended=num_of_recommended
        )
        if probability > max_probability:
            print(probability)
            max_probability = probability
            best_params = params
            best_recommender = rec

    print(max_probability, best_params)
    return best_recommender, best_params


def test_best_model(
        best_model: UserItemRecommender,
        user_item_df: pd.DataFrame,
        user_activity_df: pd.DataFrame,
        num_of_clicked: int = 5,
        num_of_recommended: int = 10
) -> float:
    users_in_interval = user_activity_df.loc[
        (user_activity_df.diapason == '(0:5]') &
        (user_activity_df.clicks_count == num_of_clicked)
        ].sample(frac=0.25)

    median_probability = 0.0
    counter = 0
    for _, user in users_in_interval.iterrows():
        median_probability += evaluate(
            user_id=user.user_id,
            user_event_df=user_item_df,
            recommender=best_model,
            number_of_recommended=num_of_recommended
        )
        counter += 1
    else:
        median_probability = median_probability / counter

    return median_probability
