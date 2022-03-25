echo 'Installing dependencies ...'
pip install virtualenv
IF NOT EXIST .\venv (
python -m venv env
)
.\venv\Scripts\activate & pip install -r .\requirements.txt & python .\unpack_rars.py &  python .\app\prepare_dataframes.py^
    .\data\cliks_add.csv^
    .\data\events_pushka_accepted_30122021.csv^
    .\data\events.csv^
    .\data\organizations.csv^
    .\data\users.txt^
    .\data\region.txt^
    .\data\RegionRussia.csv^
    .\data\preprocessed\ & python .\app\create_user_item_dfs.py^
   .\data\preprocessed\user_df.csv^
    .\data\preprocessed\events_df.csv^
    .\data\preprocessed\user_event_df.csv^
    .\data\user_event_dfs & python .\app/make_recommendations.py als^
    .\data\user_event_dfs\^
    .\data\recommendations\ json
