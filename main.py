import pandas as pd
from scripts import data_management as dm
from scripts import config as cfg

from pathlib import Path
from scripts.implicit_models import EventRecommender

if __name__ == "__main__":
    for user_event_df_file_path in Path(cfg.REGION_PATH).iterdir():
        region_name = user_event_df_file_path.stem.split('_')[0]

        print(f'{region_name}_user_event dataframe is being processed ...')
        user_event_df = pd.read_csv(user_event_df_file_path)
        user_event_df.drop(columns='Unnamed: 0', inplace=True)
        extra_event_ids = dm.get_extra_events_ids(user_event_df)

        print('\t - preparing recommendations.')
        recommender = EventRecommender(user_event_df, extra_event_ids=extra_event_ids, model_name='als')
        recommender.prepare_recommendations()

        print('\t - saving as .json.')
        recommender.save_as_json(cfg.RESULT_PATH + f"{region_name}_rec.json")
