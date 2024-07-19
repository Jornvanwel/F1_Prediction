import pandas as pd
import numpy as np
import os

# Function to read CSV files from the staging path
def read_file(filename):
    current_dir = os.getcwd()
    staging_path = os.path.normpath(os.path.join(current_dir, '..', 'Data', 'Staging'))
    return pd.read_csv(os.path.join(staging_path, filename), dayfirst=True)

# Read files
results = read_file("results.csv")
driverstandings = read_file("driver_standings.csv")
constructorstandings = read_file("constructor_standings.csv")
constructors = read_file("constructors.csv")
drivers = read_file("drivers.csv")
races = read_file("races.csv")
laps = read_file("lap_times.csv")

# Rename columns for clarity
results.rename(columns={'positionOrder': 'results_position', 'points': 'results_points'}, inplace=True)
driverstandings.rename(columns={'position': 'driverstandings_position', 'positionText': 'driverstandings_positionText', 'points': 'driverstandings_points', 'wins': 'driverstandings_wins'}, inplace=True)
constructorstandings.rename(columns={'position': 'constructorstandings_position', 'positionText': 'constructorstandings_positionText', 'points': 'constructorstandings_points', 'wins': 'constructorstandings_wins'}, inplace=True)
races.rename(columns={'name': 'racename'}, inplace=True)
drivers.rename(columns={'number': 'drivernumber'}, inplace=True)
constructors.rename(columns={'name': 'constructorname'}, inplace=True)

# Create 'quarter' column
races = races.astype({'date': 'datetime64[ns]'})
races["quarter"] = races.groupby(['year'])['round'].transform(lambda x: pd.qcut(x, 4, labels=range(1, 5)))

races = races.sort_values(by=['year', 'round'])
races['date_diff'] = races['date'].diff().dt.days
races['is_round_1'] = races['round'].apply(lambda x: 1 if x == 1 else 0)

# Group 'laps' dataframe and calculate average lap time
laps_gr = laps.groupby(['raceId', 'driverId'], as_index=False).milliseconds.mean()
laps_gr.columns = ['raceId', 'driverId', 'laptime_avg']

# Merge dataframes
data = (results
        .merge(driverstandings, on=["raceId", "driverId"], how="left")
        .merge(constructorstandings, on=["raceId", "constructorId"], how="left")
        .merge(races, on="raceId", how="left")
        .merge(drivers, on="driverId", how="left")
        .merge(constructors, on="constructorId", how="left")
        .merge(laps_gr, on=['raceId', 'driverId'], how='left'))

# Replace null values
data.replace([r"\N", r"\\N"], np.NaN, inplace=True)

# Change data types
data = data.astype({
    'fastestLap': "float",
    'milliseconds': "float",
    'rank': "float",
    'date': "datetime64[ns]",
    'results_position': 'int64'
})

# Compute grid_end_diff
data["grid_end_diff"] = abs(data["results_position"] - data["grid"]).astype(int)

# Initialize an empty dataframe to store the results
df_overtakes = pd.DataFrame()

for year in sorted(data['year'].unique()):
    previous_years_data = data[data['year'] < year].copy()
    previous_years_data['overtakes_per_track'] = previous_years_data.groupby('circuitId')['grid_end_diff'].transform('mean')
    previous_years_data = previous_years_data[["overtakes_per_track", "circuitId"]].drop_duplicates()
    previous_years_data['year'] = year
    df_overtakes = pd.concat([df_overtakes, previous_years_data], ignore_index=True)

# Merge the overtake information back into the original dataframe
data = data.merge(df_overtakes, on=["circuitId", "year"], how="left")

# Sort the data by year, driverId, and round
data = data.sort_values(['driverId', 'year', 'round'])

# Columns to be shifted
columns_to_shift = ["grid", "results_position", "overtakes_per_track"]

# Group the data and shift the columns
data_gr = data.groupby(['driverId'])[columns_to_shift]
data_shifted = data_gr.shift(periods=-1)
data_shifted.columns = [f"{col}_t1" for col in data_shifted.columns]
data = data.join(data_shifted)

data["diff_grid_standing"] = data["grid_t1"] - data["driverstandings_position"]

# Conditions and values for results_position_t1
conditions_top3 = [(data['results_position_t1'] <= 3), (data['results_position_t1'] > 3)]
conditions_top2 = [(data['results_position_t1'] <= 2), (data['results_position_t1'] > 2)]
conditions_top1 = [(data['results_position_t1'] <= 1), (data['results_position_t1'] > 1)]
values = [1, 0]

# Calculate results_position_t1 top values
data['results_position_t1_num'] = data['results_position_t1']
data['results_position_t1_top1'] = np.select(conditions_top1, values)
data['results_position_t1_top2'] = np.select(conditions_top2, values)
data['results_position_t1'] = np.select(conditions_top3, values)

# Calculate grid_end_diff_overtakes and grid_end_diff_defense
data["grid_end_diff_overtakes"] = (data["results_position"] - data["grid"]).astype(int).clip(lower=0).astype(int)
data["grid_end_diff_defense"] = (data["results_position"] - data["grid"]).astype(int).clip(upper=0).astype(int)

# Calculate the sum of driver standings per constructor, year, and race
total_driverstanding = data.groupby(['constructorId', 'raceId'])['driverstandings_position'].transform('sum')
data['teammates_driverstanding'] = total_driverstanding - data['driverstandings_position']

# Sort the data by year, driverId, and raceId
data = data.sort_values(['driverId', 'raceId'])

# Define a function to calculate the expanding mean
def expanding_mean(x):
    return x.expanding().mean()

# Compute drivers_takeover_chance and teammates_takeover_chance
data[['drivers_takeover_chance']] = data.groupby('driverId')[['grid_end_diff_overtakes']].transform(expanding_mean)
data['drivers_takeover_chance'] = data['drivers_takeover_chance'].fillna(0)
total_takeover = data.groupby(['constructorId', 'raceId'])['drivers_takeover_chance'].transform('sum')
data['teammates_takeover_chance'] = total_takeover - data['drivers_takeover_chance']

# Compute drivers_defense_skills and teammates_defense
data[['drivers_defense']] = data.groupby('driverId')[['grid_end_diff_defense']].transform(expanding_mean)
data['drivers_defense'] = data['drivers_defense'].fillna(0)
total_defense = data.groupby(['constructorId', 'raceId'])['drivers_defense'].transform('sum')
data['teammates_defense'] = total_defense - data['drivers_defense']

current_dir = os.getcwd()
prepared_path = os.path.normpath(os.path.join(current_dir, '..', 'Data', 'Prepared'))
data.to_csv(os.path.join(prepared_path, 'F1_prepared.csv'))