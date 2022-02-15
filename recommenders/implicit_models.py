import csv
import json
import pandas as pd
import scipy.sparse as sparse

from tqdm import tqdm
from bidict import bidict
from typing import Optional, List, Union
from implicit.als import AlternatingLeastSquares
from implicit.bpr import BayesianPersonalizedRanking


class UserItemRecommender:
    """
    An interface for ALS and BPR recommender models from implicit python module.
    """

    def __init__(self,
                 user_item_df: pd.DataFrame,
                 extra_item_ids: List[int] = None,
                 num_of_threads: int = 0,
                 ):
        """
        :param user_item_df: a pandas Dataframe witch the following columns:
        +---------+---------+--------+----------+----------+
        | user_id | item_id | rating | user_num | item_num |
        +---------+---------+--------+----------+----------+
        :param extra_item_ids: a list of  extra item ids to filter out from the output

        """
        self.model = None
        self.recommendations = list()
        self.user_item = user_item_df
        self.num_of_threads = num_of_threads

        user_number_per_id = user_item_df.loc[:, ['user_num', 'user_id']].drop_duplicates()
        self.user_number_per_id = bidict(dict(zip(user_number_per_id.user_id, user_number_per_id.user_num)))

        item_number_per_id = user_item_df.loc[:, ['item_num', 'item_id']].drop_duplicates()
        self.item_number_per_id = bidict(dict(zip(item_number_per_id.item_id, item_number_per_id.item_num)))

        self.sparse_item_user = sparse.csr_matrix(
            (self.user_item['rating'].astype(float), (self.user_item['item_num'], self.user_item['user_num']))
        )
        self.sparse_user_item = sparse.csr_matrix(
            (self.user_item['rating'].astype(float), (self.user_item['user_num'], self.user_item['item_num']))
        )

        if extra_item_ids is not None:
            self.extra_item_ids = [self.item_number_per_id[item_id] for item_id in extra_item_ids]
        else:
            self.extra_item_ids = None

    def fit(self, model_name: str = 'als', **model_params) -> None:
        """
        Fit the model with passed parameters

        :param model_name: 'als' for Alternative Least Squares or 'bpr' for Bayesian Personalized Ranking
        :return:
        """
        if model_name == 'als':
            # params = {'factors': 20, 'regularization': 0.1, 'iterations': 20}
            alpha_val = 15
            self.model = AlternatingLeastSquares(**model_params, num_threads=self.num_of_threads)
            self.model.fit((self.sparse_item_user * alpha_val).astype('double'))
        elif model_name == 'bpr':
            # params = {"factors": 60}
            self.model = BayesianPersonalizedRanking(**model_params, num_threads=self.num_of_threads)
            self.model.fit(self.sparse_item_user)
        else:
            raise ValueError(f'Incorrect value of the model_name argument: {model_name}')

    def get_user_recommendation(self, user_id: str) -> list:
        """
        Calculates recommendation for user with the default parameters for implicit.<model>.recommend method and
        saves it as list
        :param user_id: string value of user id from source data
        :return:
        """
        user_num = self.user_number_per_id[user_id]
        recommended = self.model.recommend(user_num, self.sparse_user_item, filter_items=self.extra_item_ids)
        recommendations = list()
        for item in recommended:
            event_num, event_score = item
            recommendations.append([
                user_id,
                self.item_number_per_id.inverse[event_num],
                event_score
            ])
        return recommendations

    def get_all_recommendation(self, as_pd_dataframe: bool = True) -> Union[list, pd.DataFrame]:
        """
        Calculates recommendation for all users with the default parameters for implicit.<model>.recommend method and
        saves it as list or pd.Dataframe
        :param as_pd_dataframe: boolean flag, if True -- return as a pd.Dataframe, otherwise as a list
        :return:
        """
        for user_num in tqdm(self.user_number_per_id.keys()):
            self.recommendations.extend(self.get_user_recommendation(user_num))
        if as_pd_dataframe:
            return pd.DataFrame(self.recommendations, columns=['user_id', 'item_id', 'rating'])
        else:
            return self.recommendations

    def to_csv(self, filename: str) -> None:
        """
        Saves recommendation dataframe as .csv without changes

        :param filename: target .csv full filename
        :return: boolean value
        """
        if self.recommendations is None:
            self.get_all_recommendation(as_pd_dataframe=False)
        with open(f"{filename}.csv", 'w', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(['user_id', 'event_id', 'rating'])
            writer.writerows(self.recommendations)

    def to_json(self, filename) -> None:
        """
        Saves recommendation dataframe as .json with following format:
            {   user_id: {
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
        if self.recommendations is None:
            self.get_all_recommendation(as_pd_dataframe=False)

        current_user_num = self.recommendations[0][0]
        user_rec_dict = dict()
        for row in tqdm(self.recommendations):
            if row[0] != current_user_num:
                recommendation_dict[current_user_num] = user_rec_dict
                user_rec_dict = dict()
                current_user_num = row[0]
            else:
                user_rec_dict[row[1]] = round(float(row[2]), 3)

        with open(f'{filename}.json', 'w') as json_file:
            json.dump(recommendation_dict, json_file)
