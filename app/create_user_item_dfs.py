import argparse
import pandas as pd
from pathlib import Path
from pipeline import filtering as ftr

parser = argparse.ArgumentParser(
    prog='make_user_item_dfs',
    description='Create user-item matrices as .csv tables. Default mode - group all users and events by their regions.'
)

parser.add_argument(
    'user_df',
    type=str,
    help='a .csv file containing prepared information about users.'
)

parser.add_argument(
    'event_df',
    type=str,
    help='a path to the .csv file containing prepared information about cultural events.'
)

parser.add_argument(
    'user_event_df',
    type=str,
    help='a path to the .csv file containing prepared information about user activity.'
)

parser.add_argument(
    'target_dir',
    type=str,
    help='an output directory where .csv tables will be saved.',
)

parser.add_argument(
    '-user_region',
    type=int,
    help='a code number of the region where users live.',
    action='store'
)

parser.add_argument(
    '-event_region',
    type=int,
    help='a code number of the region where events are located.',
    action='store'
)

args = parser.parse_args()

if __name__ == "__main__":
    print('INFO: creating user-event dataframes ...')
    for path in args.user_df, args.event_df, args.user_event_df:
        if not Path(path).is_file():
            raise OSError(f'File {path} was not found.')

    if not Path(args.target_dir).is_dir():
        raise OSError(f'Directory {args.target_dir} does not exist.')

    user_event_df = pd.read_csv(args.user_event_df)
    user_df = pd.read_csv(args.user_df)
    event_df = pd.read_csv(args.event_df)

    if args.user_region is not None and args.event_region is not None:
        df = ftr.filter_user_event_df(
            user_event_df,
            user_df,
            event_df,
            user_region_code=args.user_region,
            event_region_code=args.event_region
        )
        df.to_csv(f'user_{args.user_region}_event_{args.event_region}.csv', index=False)
    else:
        user_region_codes = set(user_df.region_code)
        event_region_codes = set(event_df.region_code)
        region_codes = user_region_codes.intersection(event_region_codes)

        for region_code in region_codes:
            df = ftr.filter_user_event_df(
                user_event_df,
                user_df,
                event_df,
                user_region_code=region_code,
                event_region_code=region_code
            )
            df.to_csv(f'{args.target_dir}/user_{region_code}_event_{region_code}.csv', index=False)
