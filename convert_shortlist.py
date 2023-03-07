import pandas as pd
import argparse
from dotenv import load_dotenv
import os
import sqlalchemy

load_dotenv('.env')
parser = argparse.ArgumentParser()
parser.add_argument('--input_file', type=str, required=True,
                    help="Shortlist csv file of categories with parent ids")

args = parser.parse_args()
input_file = args.input_file
OUTPUT_FILE_NAME = 'shortlist_cat_cast.csv'


def load_engine(base_name: str, engine: str, execution_options: dict):
    host = os.getenv(f'{base_name}_DB_HOST')
    db = os.getenv(f'{base_name}_DB_NAME')
    user = os.getenv(f'{base_name}_DB_USER')
    password = os.getenv(f'{base_name}_DB_PASSWORD')
    port = os.getenv(f'{base_name}_DB_PORT')
    connection_str = f'{engine}://{user}:{password}@{host}:{port}/{db}'
    return sqlalchemy.create_engine(connection_str, execution_options={})

def main():

    engine_save = load_engine('DS', 'postgresql', None)

    shortlist_cat = pd.read_csv(input_file)
    shortlist_cat = shortlist_cat.iloc[:,0:3]
    shortlist_cat.iloc[:,0:2] = shortlist_cat.iloc[:,0:2].astype('int')
    shortlist_cat = shortlist_cat.rename(columns = {'Criterion ID' : 'category_id', 'Parent IDs' : 'parent_id'})
    shortlist_cat.to_csv(OUTPUT_FILE_NAME)
    shortlist_cat.to_sql('categories',
                    con=engine_save, if_exists='append', schema='data',index=False)


if __name__ == "__main__":
    main()