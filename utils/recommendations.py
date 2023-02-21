import itertools
from collections import Counter
from pandas import DataFrame, Series, concat, isnull

from typing import Tuple


class GridSearchInterest(object):
    def __init__(self, Fp_click: list, Fp_purchase: list, Fi: list, Fa: list, Dt=None, n=None):
        if Dt is None and n is None:
            raise Exception('Prodive at least one argument Dt or n')
        self.param_dict = {
            'Fp_click': Fp_click,
            'Fp_purchase': Fp_purchase,
            'Fi': Fi,
            'Fa': Fa
        }

    def _make_params_grid(self):
        return itertools.product(*self.param_dict.values())

    def make_profile(self, df: DataFrame, group_id: str, column: str) -> DataFrame:
        freq_df = df.groupby([group_id])[column].value_counts().unstack()
        pct_df_IP = freq_df.divide(freq_df.sum(axis=1), axis=0)
        return pct_df_IP.fillna(0)

    def fit(self, user_actions: DataFrame, user_primary_interests: Series) -> DataFrame:
        user_actions_copy = user_actions.copy()
        result_list = []
        for params in self._make_params_grid():
            Fp_click, Fp_purchase, Fi, Fa = params
            user_actions = user_actions_copy.copy()

            user_actions['value'] = Fp_click
            user_actions.loc[user_actions.is_buy ==
                             True, 'value'] = Fp_purchase
            mask = user_actions.is_primary_category != 0
            user_actions.loc[mask,
                             'value'] = user_actions.loc[mask, 'value'] * Fi

            users_weights = user_actions.groupby(['user_id', 'category'])[
                'value'].sum().unstack()
            users_weights = users_weights.divide(
                users_weights.sum(axis=1), axis=0)

            users_weights.sort_index(inplace=True)
            user_primary_interests.sort_index(inplace=True)

            weights = ((users_weights * Fa) + user_primary_interests.fillna(0))

            weights = weights.divide(weights.sum(axis=1), axis=0)
            weights[list(self.param_dict.keys())] = params

            result_list.append(weights.reset_index())
        df = concat(result_list)
        df = df.rename(columns={'index': 'user_id'})
        return df


def apply_etalon_diff(x):
    cols = x.loc[:, 'auto': 'travel'].add_suffix('_diff').columns
    x[cols] =\
        (x.loc[:, 'auto': 'travel'] - x.iloc[0].loc['auto': 'travel']).abs()
    return x


class DataProcessor(object):

    def _process_primary_interests(self, users: DataFrame) -> DataFrame:
        users_interests = users.set_index('id')['profile'].apply(
            lambda x: x.get('interests', []))
        users_interests = DataFrame(list(users_interests.apply(
            lambda x: Counter(x))), index=users_interests.index)

        # TODO : Which categories do we need?
        users_interests = users_interests.loc[:, 'auto': 'travel']
        users_interests = users_interests.divide(
            users_interests.sum(axis=1), axis=0)
        users_interests['edu'] = None
        users_interests[None] = None

        return users_interests

    def _process_clicks(self, clicks: DataFrame, transactions: DataFrame) -> DataFrame:
        clicks['is_buy'] = None
        clicks.loc[clicks.click_uuid.isin(
            transactions.click_uuid), 'is_buy'] = True

        return clicks


class DataProcessorCorrelation(DataProcessor):
    def process(self, clicks: DataFrame, transactions: DataFrame, users: DataFrame) -> Tuple[DataFrame, DataFrame]:

        users_interests = self._process_primary_interests(users)
        clicks = self._process_clicks(clicks, transactions)

        corr_users = list(set(clicks.user_id) & set(users_interests.index))

        corr_clicks = clicks[clicks.user_id.isin(corr_users)]
        corr_clicks = corr_clicks[corr_clicks.category.isin(
            list(users_interests.columns))]
        corr_clicks['is_primary_category'] = corr_clicks.apply(lambda x: 1 if not isnull(
            users_interests.loc[x.user_id, x.category]) else 0, axis=1)

        corr_interests = users_interests[users_interests.index.isin(
            corr_users)]

        return corr_clicks, corr_interests


class DataProcessorAll(DataProcessor):
    def process(self, clicks: DataFrame, transactions: DataFrame, users: DataFrame) -> Tuple[DataFrame, DataFrame]:

        users_interests = self._process_primary_interests(users)
        clicks = self._process_clicks(clicks, transactions)

        clicks['is_primary_category'] = clicks.apply(lambda x: 1 if not isnull(
            users_interests.loc[x.user_id, x.category]) else 0, axis=1)

        return clicks, users_interests


class RecommendationEngine(object):
    def __init__(self, Fp_click: list, Fp_purchase: list, Fi: list, Fa: list) -> DataFrame:
        self.param_dict = {
            'Fp_click': Fp_click,
            'Fp_purchase': Fp_purchase,
            'Fi': Fi,
            'Fa': Fa
        }

    def fit(self, user_actions: DataFrame, user_primary_interests: DataFrame):
        Fp_click, Fp_purchase, Fi, Fa = self.param_dict.values()

        user_actions['value'] = Fp_click
        user_actions.loc[user_actions.is_buy == True, 'value'] = Fp_purchase

        mask = user_actions.is_primary_category != 0
        user_actions.loc[mask, 'value'] = user_actions.loc[mask, 'value'] * Fi

        users_weights = user_actions.groupby(['user_id', 'category'])[
            'value'].sum().unstack()
        users_weights = users_weights.divide(users_weights.sum(axis=1), axis=0)

        users_weights.sort_index(inplace=True)
        user_primary_interests.sort_index(inplace=True)

        weights = user_primary_interests.fillna(
            0).add((users_weights * Fa), fill_value=0)
        weights = weights.divide(weights.sum(axis=1), axis=0)

        weights = weights[weights.loc[:, 'auto':'travel'].sum(axis=1) > 0]

        return weights
