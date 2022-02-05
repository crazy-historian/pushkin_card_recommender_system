import json
import pandas as pd
import scipy.sparse as sparse

from typing import Optional
from implicit.als import AlternatingLeastSquares
from implicit.bpr import BayesianPersonalizedRanking
from implicit.nearest_neighbours import bm25_weight


class EventRecommender:
    """
    An interface for ALS and BPR recommender models from implicit python module.
    """

    def __init__(self, user_event_df: pd.DataFrame, model_name: str = 'als'):
        """
        :param user_event_df: a pandas Dataframe witch the following columns:
         +---------+----------+-----------+--------------+
        | user_id | event_id | event_name | clicks_count |
        +---------+----------+-----------+--------------+

        :param model_name: 'als' for Alternative Least Squares or 'bpr' for Bayesian Personalized Ranking

        """
        self.recommendations = None
        self.user_event = user_event_df
        self.user_event['event_id'] = self.user_event['event_id'].astype('category')
        self.user_event['user_id'] = self.user_event['user_id'].astype('category')
        self.user_event['user_num'] = self.user_event['user_id'].cat.codes
        self.user_event['event_num'] = self.user_event['event_id'].cat.codes

        user_number_per_id = user_event_df.loc[:, ['user_num', 'user_id']].drop_duplicates()
        self.user_number_per_id = dict(zip(user_number_per_id.user_num, user_number_per_id.user_id))
        self.user_id_per_number = dict(zip(user_number_per_id.user_id, user_number_per_id.user_num))

        event_number_per_id = user_event_df.loc[:, ['event_num', 'event_id']].drop_duplicates()
        self.event_number_per_id = dict(zip(event_number_per_id.event_num, event_number_per_id.event_id))
        self.event_id_per_number = dict(zip(event_number_per_id.event_id, event_number_per_id.event_num))

        self.sparse_event_user = sparse.csr_matrix(
            (self.user_event['clicks_count'].astype(float), (self.user_event['event_num'], self.user_event['user_num']))
        )
        self.sparse_user_event = sparse.csr_matrix(
            (self.user_event['clicks_count'].astype(float), (self.user_event['user_num'], self.user_event['event_num']))
        )

        if model_name == 'als':
            params = {'factors': 20, 'regularization': 0.1, 'iterations': 20}
            alpha_val = 15
            self.model = AlternatingLeastSquares(**params)
            self.model.fit((self.sparse_event_user * alpha_val).astype('double'))
        elif model_name == 'bpr':
            params = {"factors": 60}
            self.model = BayesianPersonalizedRanking(**params)
            self.model.fit(self.sparse_event_user)
        else:
            raise ValueError(f'Incorrect value of the model_name argument: {model_name}')

    def prepare_recommendations(self) -> None:
        """
        Calculate recommendation for all users with the default parameters for implicit.<model>.recommend method
        :return:
        """
        recommendations = list()
        for user_num in range(len(self.user_number_per_id)):
            recommended = self.model.recommend(user_num, self.sparse_user_event)
            for item in recommended:
                event_num, event_score = item
                recommendations.append([
                        self.user_number_per_id[user_num],
                        self.event_number_per_id[event_num],
                        event_score
                    ])
        self.recommendations = pd.DataFrame(recommendations, columns=['user_id', 'event_id', 'score'])

    def get_quick_user_recommendation(self, user_id: str, number=10) -> Optional[pd.DataFrame]:
        user_num = self.user_id_per_number[user_id]
        recommendations = list()
        recommended = self.model.recommend(user_num, self.sparse_user_event, N=number)
        for item in recommended:
            event_num, event_score = item
            recommendations.append([
                self.user_number_per_id[user_num],
                self.event_number_per_id[event_num],
                event_score
            ])
        return pd.DataFrame(recommendations, columns=['user_id', 'event_id', 'score'])

    def get_user_recommendation(self, user_id: str) -> Optional[pd.DataFrame]:
        """
        Returns recommendation for particular user with the passed user_id.

        :return: pd.Dataframe or None if recommendation was not prepared.
        """
        if self.recommendations is not None:
            return self.recommendations.loc[self.recommendations['user_id'] == user_id]
        else:
            return None

    def get_users_interested_in_event(self, event_id: int, n_users: int = 10) -> Optional[pd.DataFrame]:
        """
        Returns n_users most interested in the event with the passed event_id

       :return: pd.Dataframe or None if recommendation was not prepared.
        """
        if self.recommendations is not None:
            return self.recommendations.loc[
                       self.recommendations['event_id'] == event_id
                       ].sort_values(by='score', ascending=False)[:n_users]
        else:
            return None

    def save_as_csv(self, filename: str) -> bool:
        """
        Saves recommendation dataframe as .csv without changes

        :param filename: target .csv full filename
        :return: boolean value
        """
        if self.recommendations is not None:
            self.recommendations.to_csv(filename)
            return True
        else:
            return False

    def save_as_json(self, filename):
        """
        Saves recommendation dataframe as .json with following format:
            { user_id: {
                    event_1: score_1,
                    event_2: score_2,
                    ...
                },
                ....
            }

        :param filename: target .json full filename
        :return: boolean value
        """
        recommendation_dict = dict()
        if self.recommendations is not None:
            for user_id in list(self.user_number_per_id.values()):
                user_rows = self.recommendations.loc[self.recommendations['user_id'] == user_id].values.tolist()
                rows_dict = dict()
                for row in user_rows:
                    rows_dict[row[1]] = row[2]
                recommendation_dict[user_id] = rows_dict

            with open(filename, 'w') as json_file:
                json.dump(recommendation_dict, json_file)
