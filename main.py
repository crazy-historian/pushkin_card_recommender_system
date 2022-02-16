import pandas as pd
import preprocessing.data_management as dm

from pathlib import Path
from recommenders.implicit_models import ALSRecommender

if __name__ == "__main__":
    for user_event_df_file_path in Path('data/user_event_dfs').iterdir():
        region_name = user_event_df_file_path.stem.split('_')[0]

        print(f'{region_name}_user_event dataframe is being processed ...')
        user_event_df = pd.read_csv(user_event_df_file_path)
        user_event_df.drop(columns='Unnamed: 0', inplace=True)
        extra_event_ids = dm.get_extra_events_ids(user_event_df)

        print('\t - preparing recommendations.')
        recommender = ALSRecommender()
        recommender.fit(user_event_df, extra_event_ids)
        recommender.get_all_recommendation()

        print('\t - saving as .json.')
        recommender.to_json(f"recommendations/{region_name}_rec")
