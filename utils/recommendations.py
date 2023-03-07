import itertools
from collections import Counter
from pandas import DataFrame, Series, concat, isnull
import numpy as np

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
    @staticmethod
    def dict_to_df(data_dict : dict) -> DataFrame:
        series_data = Series(data_dict)
        return DataFrame(list(series_data.apply(
            lambda x: Counter(x))), index=series_data.index)
    
    def _process_primary_interests(self, users: DataFrame) -> Tuple[DataFrame, DataFrame]:
        users_interests = users.set_index('id')['profile'].apply(
            lambda x: x.get('interests', []))
        users_interests = DataFrame(list(users_interests.apply(
            lambda x: Counter(x))), index=users_interests.index)
        users_interests_dict = users.set_index('id')['profile'].apply(lambda x: x.get('interests', []) if x.get('interests', []) != None else [] ).to_dict()
        users_interests = users_interests.divide(
            users_interests.sum(axis=1), axis=0)
        users_interests[None] = None
        return users_interests, users_interests_dict

    def _process_primary_interests_non_empty(self, users: DataFrame) -> Tuple[DataFrame, DataFrame]:
        users_interests, users_interests_dict = self._process_primary_interests(users)
        users_interests_dict = {key:list(set(value)) for key, value in users_interests_dict.items() if value }

        users_interests = Series(users_interests_dict)
        users_interests = DataFrame(list(users_interests.apply(
            lambda x: Counter(x))), index=users_interests.index)
        
        return users_interests, users_interests_dict


class DataProcessorCorrelation(DataProcessor):
    def process(self, clicks: DataFrame, users: DataFrame) -> Tuple[DataFrame, DataFrame]:

        users_interests,users_interests_dict = self._process_primary_interests(users)

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
    
    
    def process(self, clicks: DataFrame, users: DataFrame) -> Tuple[DataFrame, DataFrame]:

        users = users[users['id'].isin(clicks['user_id'].unique())]
        users_interests,users_interests_dict = self._process_primary_interests(users)

        clicks['is_primary_category'] = clicks.apply(lambda x: 1 if x.category in users_interests_dict.get(x.user_id,[]) else 0, axis=1)

        return clicks, users_interests



class DataProcessorParent(DataProcessor):
    
    def __init__(self, shortlist_cat: DataFrame):
        super().__init__()
        self.buy_fill_func = {
            0 : lambda x : list(np.zeros(len(x))),
            1 : lambda x : list(np.ones(len(x))),
        }
        self.shortlist_cat = shortlist_cat
        self._process_categories()

    def _process_categories(self) -> DataFrame:
        self.categories_dict = self.shortlist_cat.set_index('category_id').to_dict(orient='index')

    
    def get_primary_parent_id(self,cat_id, parents_list):
        if self.categories_dict[cat_id]['parent_id']:
            parents_list.append(cat_id)
            return parents_list
        parents_list.append(cat_id)
        return self.get_primary_parent_id(self.categories_dict[cat_id]['parent_id'], parents_list)   



    def _parse_parent_clicks(self, clicks: DataFrame):
        new_list = []
        buy_list = []
        for category,is_buy_click in zip(clicks.category.values, clicks.is_buy.values):
            extented_clicks = self.get_primary_parent_id(category,[])
            extented_buys = self.buy_fill_func[is_buy_click](extented_clicks)
            new_list +=extented_clicks
            buy_list +=extented_buys
        return DataFrame(
            data = {'category' : new_list, 'is_buy' : buy_list}
        )
        

    def _extend_user_parent_categories(self, categories : list):
        new_categories_list = []
        for category in categories:
            is_shortlist = self.categories_dict.get(category, 0)
            if is_shortlist:
                has_parent = is_shortlist.get('parent_id', 0)
                if has_parent:
                    new_categories_list += self.get_primary_parent_id(category,[])
                else:
                    new_categories_list.append(
                        category
                    )                    
            else:
                new_categories_list.append(
                    category
                )
        return new_categories_list    
    

    def process(self, clicks: DataFrame, users: DataFrame) -> Tuple[DataFrame, DataFrame]:
        
        users = users[users['id'].isin(clicks['user_id'])]      

        users_interests,users_interests_dict = self._process_primary_interests_non_empty(users)

        for user_id in users_interests_dict.keys():
            new_cats = self._extend_user_parent_categories(users_interests_dict[user_id])
            users_interests_dict[user_id] = new_cats

        users_interests = super().dict_to_df(users_interests_dict)
        
        categories_with_parent = self.shortlist_cat[self.shortlist_cat['parent_id'] == 0]['category_id']
        non_parent_clicks = clicks[~clicks['category'].isin(categories_with_parent)]
        parent_clicks = clicks[clicks['category'].isin(categories_with_parent)]

        if not parent_clicks.empty:
            parent_clicks = parent_clicks.groupby('user_id').apply(self._parse_parent_clicks).reset_index().drop(columns = ['level_1'])

        clicks = concat(
            [non_parent_clicks,parent_clicks]
        )

        clicks['is_primary_category'] = clicks.apply(lambda x: 1 if x.category in users_interests_dict.get(x.user_id,[]) else 0, axis=1)

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

        return weights
