import fastf1 as ff1
import logging
from my_functions.update_functions import ff1_retriever, Dim_Updater, load_dimension_tables, add_new_entries, update_qualifying, update_standings, update_laps, update_results

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Retrieve the level of the root logger
#root_logger_level = logging.getLogger().level

# Set the fastf1 logger to the same level
logging.getLogger('fastf1').setLevel(logging.WARNING)

# Get the schedule of a given year
schedule = ff1.get_event_schedule(2024)

# Retrieve lap data and results data 
R_laps, R_results = ff1_retriever(2024, 'R')
_, Q_results = ff1_retriever(2024, 'Qualifying')
Sprint_laps, Sprint_results = ff1_retriever(2024, 'Sprint')

dim_drivers, dim_constructors, dim_races = load_dimension_tables()

updater = Dim_Updater(dim_drivers)
updater.update_drivers(R_results)

updater = Dim_Updater(dim_constructors)
updater.update_constructors(R_results)

updater = Dim_Updater(dim_races)
updater.update_races(schedule)

add_new_entries('qualifying.csv', update_qualifying(Q_results))

R_laps, fastest_laps, laps_driven = update_laps(R_laps)
R_results = update_results(R_results, fastest_laps, laps_driven)

add_new_entries('results.csv', R_results, index_label = 'resultId')
add_new_entries('lap_times.csv', R_laps, index_label = None)

Sprint_laps, fastest_laps, laps_driven = update_laps(Sprint_laps)
Sprint_results = update_results(Sprint_results, fastest_laps, laps_driven)

add_new_entries('sprint_results.csv', Sprint_results, index_label = 'resultId')

current_dir = os.getcwd()
staging_path = os.path.join(current_dir, '..', 'Data', 'Staging')
staging_path = os.path.normpath(staging_path)

results = pd.read_csv(os.path.join(staging_path, 'results.csv'), on_bad_lines='skip', header=0, delimiter=',', index_col='resultId')
races = pd.read_csv(os.path.join(staging_path, 'races.csv'), on_bad_lines='skip', header=0, delimiter=',')
sprint_results = pd.read_csv(os.path.join(staging_path, 'sprint_results.csv'), on_bad_lines='skip', header=0, delimiter=',', index_col='resultId')

driverpoint_df, constructorpoints_df = update_standings(results, sprint_results, races)

add_new_entries('driver_standings.csv', driverpoint_df, index_label='driverStandingsId', staging_path= staging_path)
add_new_entries('constructor_standings.csv', constructorpoints_df, index_label='constructorStandingsId', staging_path= staging_path)