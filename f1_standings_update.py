import os
import pandas as pd
from my_functions.update_functions import add_new_entries, update_standings

current_dir = os.getcwd()
staging_path = os.path.join(current_dir, '..', 'Data', 'Staging')
staging_path = os.path.normpath(staging_path)

results = pd.read_csv(os.path.join(staging_path, 'results.csv'), on_bad_lines='skip', header=0, delimiter=',', index_col='resultId')
races = pd.read_csv(os.path.join(staging_path, 'races.csv'), on_bad_lines='skip', header=0, delimiter=',')
sprint_results = pd.read_csv(os.path.join(staging_path, 'sprint_results.csv'), on_bad_lines='skip', header=0, delimiter=',', index_col='resultId')

driverpoint_df, constructorpoints_df = update_standings(results, sprint_results, races)

add_new_entries('driver_standings.csv', driverpoint_df, index_label='driverStandingsId', staging_path= staging_path)
add_new_entries('constructor_standings.csv', constructorpoints_df, index_label='constructorStandingsId', staging_path= staging_path)