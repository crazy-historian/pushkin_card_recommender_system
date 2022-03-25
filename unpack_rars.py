import patoolib

from pathlib import Path

if __name__ == "__main__":
    if not Path('./data/cliks_add.csv').is_file():
        patoolib.extract_archive('./data/cliks_add.rar', outdir='./data/')

    if not Path('./data/events.csv').is_file() or \
            not Path('./data/events_pushka_accepted_30122021.csv').is_file():
        patoolib.extract_archive('./data/events.rar', outdir='./data/')

    if not Path('./data/users.txt').is_file():
        patoolib.extract_archive('./data/users.rar', outdir='./data/')

    if not Path('./data/organizations.csv').is_file() or \
            not Path('./data/region.txt').is_file() or \
            not Path('./data/RegionRussia.csv').is_file():
        patoolib.extract_archive('./data/org_and_reg.rar', outdir='./data/')

    preprocessed_dir = Path('./data/preprocessed/')
    if not preprocessed_dir.is_dir():
        preprocessed_dir.mkdir()
        print(f"INFO: {preprocessed_dir} was created")

    user_event_dfs_dir = Path('./data/user_event_dfs')
    if not user_event_dfs_dir.is_dir():
        user_event_dfs_dir.mkdir()
        print(f"INFO: {user_event_dfs_dir} was created")

    recommendations_dir = Path('./data/recommendations')
    if not recommendations_dir.is_dir():
        recommendations_dir.mkdir()
        print(f"INFO: {recommendations_dir} was created.")
