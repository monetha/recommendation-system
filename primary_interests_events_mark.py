#!/usr/bin/env python3

import pandas as pd
import numpy as np
import os
import sqlalchemy
import argparse
import json
from dotenv import load_dotenv

from utils.recommendations import RecommendationEngine, DataProcessorParent

from utils.functools import to_dict_not_zero

from params_config_engine import params

pd.options.mode.chained_assignment = None

parser = argparse.ArgumentParser()
parser.add_argument('--start_date', type=str, required=True,
                    help="Start date of clicks")
parser.add_argument('--clicks_count', type=str, required=True,
                    help="Count of last clicks per user")

args = parser.parse_args()
start_date = args.start_date
clicks_count = args.clicks_count

load_dotenv('.env')


def load_engine(base_name: str, engine: str, execution_options: dict):
    host = os.getenv(f'{base_name}_DB_HOST')
    db = os.getenv(f'{base_name}_DB_NAME')
    user = os.getenv(f'{base_name}_DB_USER')
    password = os.getenv(f'{base_name}_DB_PASSWORD')
    port = os.getenv(f'{base_name}_DB_PORT')
    connection_str = f'{engine}://{user}:{password}@{host}:{port}/{db}'
    return sqlalchemy.create_engine(connection_str, execution_options={})


def main():

    engine_affiliates_api = load_engine(
        'MONETHA_AFFILIATES', 'postgresql', {"stream_results": True})
    engine_identity_api = load_engine(
        'MONETHA_IDENTITY', 'postgresql', {"stream_results": True})
    engine_save = load_engine('DS', 'postgresql', None)

    q_clicks = open('sql/clicks_transactions.sql', 'r').read()
    clicks = pd.read_sql(q_clicks, engine_affiliates_api, params={
                         "start_date": start_date, "clicks_count": clicks_count})

    q_users = open('sql/users.sql', 'r').read()
    users = pd.read_sql(q_users, engine_identity_api)

    q_categories = open('sql/categories.sql', 'r').read()
    shortlist_cat = pd.read_sql(q_categories, engine_save)

    dp = DataProcessorParent(shortlist_cat)

    user_actions, user_primary_interests = dp.process(clicks, users)

    engine_recomendations = RecommendationEngine(
        **params
    )
    weights = engine_recomendations.fit(user_actions, user_primary_interests)

    weights_dict = to_dict_not_zero(weights)

    user_primary_interests_dict = to_dict_not_zero(user_primary_interests)

    result = pd.DataFrame(
        data={
            'weights': json.dumps(weights_dict),
            'primary_interests': json.dumps(user_primary_interests_dict)
        },
        index=[0]
    )

    result.to_sql('recommendation_logs',
                  con=engine_save, if_exists='append', schema='data',index=False, dtype={"weights": sqlalchemy.types.JSON, 'primary_interests': sqlalchemy.types.JSON})


if __name__ == "__main__":
    main()
