#!/usr/bin/env python3 

import pandas as pd
import numpy as np
import os
import sqlalchemy
import argparse
from dotenv import load_dotenv

from utils.recommendations import RecommendationEngine
from utils.recommendations import DataProcessorAll

from params_config_engine import params

pd.options.mode.chained_assignment = None

parser = argparse.ArgumentParser()
parser.add_argument('--start_date', type=str, required=True,
                    help="Start date of sessions for conversion attribution markup")
parser.add_argument('--end_date', type=str, required=True,
                    help="End date of sessions for conversion attribution markup")


args = parser.parse_args()
start_date = args.start_date
end_date = args.end_date

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

    engine_affiliates_api = load_engine('MONETHA', 'postgresql', {"stream_results": True})
    engine_identity_api = load_engine('MONETHA_AFFILIATES', 'postgresql', {"stream_results": True})
    engine_save = load_engine('DS', 'postgresql', None)

    q_clicks = f'''
    select * from affiliates.clicks_id ci
    left join(
        select m.id, m.category from affiliates.merchants m 
    ) m_c on m_c.id = ci.merchant_id
    where ci.merchant_id is not null and ci.created_at >= {start_date} and ci.created_at < {end_date}
    '''
    clicks  = pd.read_sql(q_clicks, engine_affiliates_api)

    q_transactions = f'''
    select * from affiliates.transactions t 
    where t.network in ('cj','awin') and t.created_at >= {start_date} and t.created_at < {end_date}
    '''
    transactions = pd.read_sql(q_transactions, engine_affiliates_api)

    q_users = '''
    select * from identity.users u 
    where u.profile is not null
    '''
    users = pd.read_sql(q_users, engine_identity_api)

    dp = DataProcessorAll()

    user_actions, user_primary_interests = dp.process(clicks, transactions, users)

    engine_recomendations = RecommendationEngine(
         **params
    )
    weights = engine_recomendations.fit(user_actions, user_primary_interests)

    #TODO : 
    # 1. How to handle new categories?
    # 2. Do we update, replace or update table? 
    weights.to_sql('recomemndation_weights',
                            con=engine_save, if_exists='append', schema='data')

if __name__ == "__main__": 
	main() 
        
