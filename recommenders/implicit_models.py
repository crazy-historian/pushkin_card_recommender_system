import csv
import json
import pandas as pd
import scipy.sparse as sparse

from tqdm import tqdm
from bidict import bidict
from abc import abstractmethod, ABC
from typing import Optional, List, Union
from implicit.als import AlternatingLeastSquares
from implicit.bpr import BayesianPersonalizedRanking
from implicit.nearest_neighbours import bm25_weight


class UserItemRecommender(ABC):
    """
    An interface for ALS and BPR recommender models from implicit python module.
    """

    def __init__(self,
                 # user_item_df: pd.DataFrame,
                 # extra_item_ids: List[int] = None,
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
        self.user_item = None
        self.extra_item_ids = None

        self.user_number_per_id = None
        self.item_number_per_id = None
        self.sparse_item_user = None
        self.sparse_user_item = None

        self.num_of_threads = num_of_threads

    @abstractmethod
    def fit(self, user_item_df: pd.DataFrame, extra_items_ids: Optional[List[int]] = None, show_progress: bool = True, *args, **kwargs) -> None:
        """
        Fitting the model with passed parameters
        """
        print('- fitting the model')
        self.user_item = user_item_df
        self.extra_item_ids = extra_items_ids

        user_number_per_id = self.user_item.loc[:, ['user_num', 'user_id']].drop_duplicates()
        self.user_number_per_id = bidict(dict(zip(user_number_per_id.user_id, user_number_per_id.user_num)))

        item_number_per_id = self.user_item.loc[:, ['item_num', 'item_id']].drop_duplicates()
        self.item_number_per_id = bidict(dict(zip(item_number_per_id.item_id, item_number_per_id.item_num)))

        self.sparse_item_user = sparse.csr_matrix(
            (self.user_item['rating'].astype(float), (self.user_item['item_num'], self.user_item['user_num']))
        )
        self.sparse_user_item = sparse.csr_matrix(
            (self.user_item['rating'].astype(float), (self.user_item['user_num'], self.user_item['item_num']))
        )

        if self.extra_item_ids is not None:
            self.extra_item_ids = [self.item_number_per_id[item_id] for item_id in self.extra_item_ids]
        else:
            self.extra_item_ids = None

    def get_user_recommendation(self,
                                user_id: str,
                                N: int = 10,
                                as_pd_dataframe: bool = True) -> Union[list, pd.DataFrame]:
        """
        Calculates recommendation for user with the default parameters for implicit.<model>.recommend method and
        saves it as list
        :param as_pd_dataframe:
        :param N:  number of recommended items
        :param user_id: string value of user id from source data
        :return:
        """
        user_num = self.user_number_per_id[user_id]
        recommended = self.model.recommend(user_num, self.sparse_user_item, filter_items=self.extra_item_ids, N=N)
        recommendations = list()
        for item in recommended:
            try:
                event_num, event_score = item
                recommendations.append([
                    user_id,
                    self.item_number_per_id.inverse[event_num],
                    event_score
                ])
            except KeyError:
                # TODO: make error description more explicit
                #  when the number of cultural events is too small, recommender cannot return list of 10 events
                continue
        if as_pd_dataframe:
            return pd.DataFrame(recommendations, columns=['user_id', 'item_id', 'rating'])
        else:
            return recommendations

    def get_all_recommendation(self, as_pd_dataframe: bool = True) -> Union[list, pd.DataFrame]:
        """
        Calculates recommendation for all users with the default parameters for implicit.<model>.recommend method and
        saves it as list or pd.Dataframe
        :param as_pd_dataframe: boolean flag, if True -- return as a pd.Dataframe, otherwise as a list
        :return:
        """
        print('- preparing the list of recommendations')
        for user_num in tqdm(self.user_number_per_id.keys()):
            self.recommendations.extend(self.get_user_recommendation(user_num, as_pd_dataframe=False))
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
        print('- saving as csv')
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
        print('- saving as json')
        for row in tqdm(self.recommendations):
            if row[0] != current_user_num:
                recommendation_dict[current_user_num] = user_rec_dict
                user_rec_dict = dict()
                current_user_num = row[0]
            else:
                user_rec_dict[row[1]] = round(float(row[2]), 3)

        with open(f'{filename}.json', 'w') as json_file:
            json.dump(recommendation_dict, json_file)


class ALSRecommender(UserItemRecommender):
    def __init__(self,
                 factors: Optional[int] = 20,
                 regularization: Optional[float] = 0.1,
                 iterations: Optional[int] = 100,
                 confidence: Optional[str] = None,
                 alpha_value: Optional[int] = None,
                 K1: Optional[int] = None,
                 B: Optional[float] = None,
                 num_of_threads: int = 0,
                 ):
        super().__init__(
            num_of_threads
        )
        self.factors = factors
        self.regularization = regularization
        self.iterations = iterations
        self.confidence = confidence
        self.alpha_value = alpha_value
        self.K1 = K1
        self.B = B

    def fit(self, user_item_df: pd.DataFrame, extra_item_ids: Optional[List[int]] = None, show_progress: bool = True, *args, **kwargs) -> None:
        super().fit(user_item_df, extra_item_ids, show_progress, args, kwargs)
        self.model = AlternatingLeastSquares(
            factors=self.factors,
            regularization=self.regularization,
            iterations=self.iterations,
            num_threads=self.num_of_threads)
        if self.confidence == 'alpha' or self.confidence is None:
            self.model.fit((self.sparse_item_user * self.alpha_value).astype('double'), show_progress=show_progress)
        elif self.confidence == 'bm25':
            self.model.fit(bm25_weight(self.sparse_item_user, K1=self.K1, B=self.B), show_progress=show_progress)


class BPRRecommender(UserItemRecommender):
    def __init__(self,
                 factors: Optional[int] = 20,
                 learning_rate: Optional[float] = 0.01,
                 regularization: Optional[float] = 0.1,
                 iterations: Optional[int] = 100,
                 num_of_threads: int = 0,
                 ):
        super().__init__(
            num_of_threads
        )
        self.factors = factors
        self.learning_rate = learning_rate
        self.regularization = regularization
        self.iterations = iterations

    def fit(self, user_item_df: pd.DataFrame, extra_item_ids: Optional[List[int]] = None, show_progress: bool = True, *args, **kwargs) -> None:
        super().fit(user_item_df, extra_item_ids, show_progress, args, kwargs)
        self.model = BayesianPersonalizedRanking(
            factors=self.factors,
            learning_rate=self.learning_rate,
            regularization=self.regularization,
            iterations=self.iterations,
            num_threads=self.num_of_threads)

        self.model.fit(self.sparse_item_user, show_progress=show_progress)
