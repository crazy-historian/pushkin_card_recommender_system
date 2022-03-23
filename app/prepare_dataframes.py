import argparse
from pathlib import Path

from pipeline import preprocessing as prs

parser = argparse.ArgumentParser(
    prog='prepare_dataframes',
    description='Process raw .csv tables and save preprocessed and cleaned tables (user table, events table, \n'
                'clicks table, future events table and user-event table) as .csv files into target directory'
)

parser.add_argument(
    'clicks_file',
    type=str,
    help='.csv file containing information about users clicks'
)

parser.add_argument(
    'all_events_file',
    type=str,
    help='.csv file containing information about all existing cultural events'
)

parser.add_argument(
    'current_events_file',
    type=str,
    help='.csv file containing information about all existing cultural events'
)

parser.add_argument(
    'organizations_file',
    type=str,
    help='.csv file containing information about current cultural events'
)

parser.add_argument(
    'users_file',
    type=str,
    help='.csv or .txt file containing information about users with their ids and birth dates.'
)

parser.add_argument(
    'regions_file',
    type=str,
    help='.csv or .txt file containing information about users and their registration regions'
)

parser.add_argument(
    'regions_codes',
    type=str,
    help='.csv or .txt file containing region codes'
)

parser.add_argument(
    'output_directory',
    type=str,
    help='directory for saving .csv files'
)

args = parser.parse_args()
args = vars(args)
paths = list(args.values())

if __name__ == "__main__":
    for path in paths[:-1]:
        if not Path(path).is_file():
            raise OSError(f'File {path} was not found.')
    else:
        if not Path(paths[-1]).is_dir():
            raise OSError(f'Directory {paths[-1]} does not exist.')

    target_dir = paths[-1]

    user_event_df = prs.get_user_event_dataframe(
        user_event_file_path=args['clicks_file']
    )
    user_event_df.to_csv(f'{target_dir}/user_event_df.csv', index=False)

    users_df = prs.get_user_dataframe(
        users_file_path=args['users_file'],
        regions_file_path=args['regions_file'],
        regions_nums_file_path=args['regions_codes']
    )
    users_df.to_csv(f'{target_dir}/user_df.csv', index=False)

    events_df = prs.get_events_dataframe(
        events_file_path=args['all_events_file'],
        organizations_file_path=args['organizations_file']
    )
    events_df.to_csv(f'{target_dir}/events_df.csv', index=False)

    future_events_df = prs.get_future_event_dataframe(
        future_event_file_path=args['current_events_file']
    )
    future_events_df.to_csv(f'{target_dir}/future_events_df.csv', index=False)
