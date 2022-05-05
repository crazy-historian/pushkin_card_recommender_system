# TODO 1: passing model parameters
# TODO 2: model tuning
# TODO 3: add logger

import argparse
import pandas as pd
from tqdm import tqdm
import pipeline.filtering as ftr
from recommenders.implicit_models import ALSRecommender, BPRRecommender
from pathlib import Path

parser = argparse.ArgumentParser(
    prog='make_recommendations',
    description='Creating recommendations for the users.'
)

parser.add_argument(
    'recommender',
    type=str,
    help='short designation of the recommender model:\n'
         '- als: Alternative Least Squares;'
         '- bpr: Bayesian Personalized Ranking.'
)

parser.add_argument(
    'user_event_df',
    type=str,
    help='a user_event_df file name OR a name of the directory where these files are stored.'
)

parser.add_argument(
    'output_directory',
    type=str,
    help='a directory name where recommendation files will be saved.'
)

parser.add_argument(
    'output_file_type',
    type=str,
    help='a type of output file with user recommendation:\n'
         '-json'
         '-csv'
)

args = parser.parse_args()


def run_recommender(file_name: str) -> None:
    user_event_df = pd.read_csv(file_name)
    user_event_df = ftr.numerate_user_event_df(user_event_df)
    user_event_df = user_event_df.rename(columns={
            'event_id': 'item_id',
            'event_num': 'item_num',
            'clicks_count': 'rating'
        }
    )
    recommender.fit(user_event_df)
    recommender.get_all_recommendation()

    if args.output_file_type == 'json':
        recommender.to_json(f'{args.output_directory}/{Path(file_name).stem}_rec')
    elif args.output_file_type == 'csv':
        recommender.to_csv(f'{args.output_directory}/{Path(file_name).stem}_rec')
    else:
        raise ValueError(f'Incorrect value of "output_file_type" parameter: {args.output_file_type}')


if __name__ == "__main__":
    print('INFO: making recommendations ...')

    if args.recommender == 'als':
        recommender = ALSRecommender(
            confidence='alpha',
            alpha_value=15
        )
    elif args.recommedner == 'bpr':
        recommender = BPRRecommender()
    else:
        raise ValueError(f'Incorrect value of "recommender" parameter: {args.recommender}')

    if not Path(args.output_directory).is_dir():
        raise OSError(f'Incorrect value of "output_dir" parameter: directory {args.output_directory}'
                      f'does not exist.')

    if Path(args.user_event_df).is_file():
        run_recommender(args.user_event_df)
    elif Path(args.user_event_df).is_dir():
        count = 0
        for path in Path(args.user_event_df).iterdir():
            if path.is_file():
                count += 1
        for file in tqdm(iterable=Path(args.user_event_df).iterdir(), desc='Making recommendations', total=count):
            run_recommender(str(file))
    else:
        raise OSError(f'Incorrect value of "user_event_df" parameter: file or directory {args.user_event_df}'
                      f'does not exist.')
