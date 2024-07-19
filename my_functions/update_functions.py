import pandas as pd
from datetime import datetime
import fastf1 as ff1
import logging
import os
import numpy as np

def extract_important_info(session_info):
    """
    Extracts important information from the session info dictionary.

    Parameters:
        session_info (dict): The session information dictionary.

    Returns:
        dict: A dictionary containing extracted important information.
    """
    return {
        'race_name': session_info['Meeting']['Name'],
        'Location': session_info['Meeting']['Location'],
        'Country_Name': session_info['Meeting']['Country']['Name'],
        'Country_Code': session_info['Meeting']['Country']['Code'],
        'Circuit_ShortName': session_info['Meeting']['Circuit']['ShortName']
    }

def get_paths():
    """
    Retrieves the cache and staging paths.

    Returns:
        tuple: A tuple containing cache path and staging path.
    """
    current_dir = os.getcwd()
    cache_path = os.path.normpath(os.path.join(current_dir, '..', 'Data', 'cache'))
    staging_path = os.path.normpath(os.path.join(current_dir, '..', 'Data', 'Staging'))
    return cache_path, staging_path

def calculate_total_times(group):
    """
    Calculates the total times for a group of lap times.

    Parameters:
        group (pd.DataFrame): The dataframe containing lap times.

    Returns:
        pd.DataFrame: The updated dataframe with total times calculated.
    """
    first_driver_time = group.loc[group['Position'] == 1, 'Time'].values[0]
    group['milliseconds'] = group['Time'].apply(
        lambda x: first_driver_time if first_driver_time == x else first_driver_time + x
    )
    return group

def enable_cache():
    """
    Enables the cache for fastf1 data.
    """
    cache_path, _ = get_paths()
    ff1.Cache.enable_cache(cache_path)

def load_dimension_tables():
    """
    Loads the dimension tables from the staging path.

    Returns:
        tuple: A tuple containing the drivers, constructors, and races dataframes.
    """
    _, staging_path = get_paths()
    drivers = pd.read_csv(os.path.join(staging_path, 'drivers.csv'), on_bad_lines='skip', header=0, delimiter=',')
    constructors = pd.read_csv(os.path.join(staging_path, 'constructors.csv'), on_bad_lines='skip', header=0, delimiter=',')
    races = pd.read_csv(os.path.join(staging_path, 'races.csv'), on_bad_lines='skip', header=0, delimiter=',')
    return drivers, constructors, races

def ff1_retriever(year, racetype='R'):
    """
    Retrieves lap and race data for a given year and race type.

    Parameters:
        year (int): The year for which to retrieve data.
        racetype (str): The type of race (default is 'R' for race).

    Returns:
        tuple: A tuple containing dataframes for laps and results.
    """
    enable_cache()
    schedule = ff1.get_event_schedule(year)
    laps_total = pd.DataFrame()
    results_total = pd.DataFrame()
    current_date = datetime.now()

    for index, round in enumerate(schedule['EventName']):
        try:
            event = schedule.get_event_by_round(index)
            event_time = event['EventDate']
            if event_time <= current_date:
                session = event.get_session(racetype)
                session.load()
                session_laps = session.laps
                session_results = session.results
                session_info = session.session_info
                session_date = event_time
                
                session_laps['year'] = session_date.year
                session_results['year'] = session_date.year

                session_laps = session_laps.merge(session_results[['Abbreviation', 'DriverId']], how='left', left_on='Driver', right_on='Abbreviation')
                
                important_info = extract_important_info(session_info)
                for key, value in important_info.items():
                    session_laps[key] = value
                    session_results[key] = value

                laps_total = pd.concat([laps_total, session_laps], ignore_index=True)
                results_total = pd.concat([results_total, session_results], ignore_index=True)
        except:
            pass
    return laps_total, results_total

def add_new_entries(filename, new_data, staging_path=None, index_label=None):
    """
    Adds new entries to the specified file.

    Parameters:
        filename (str): The filename of the file to be updated.
        new_data (pd.DataFrame): The new data entries to be added.
        staging_path (str, optional): The path to the staging directory (default is None).
        index_label (str, optional): The label for the index column (default is None).
    """
    if staging_path is None:
        _, staging_path = get_paths()
    file_path = os.path.join(staging_path, filename)
    existing_data = pd.read_csv(file_path, on_bad_lines='skip', header=0, delimiter=',', index_col=index_label)
    new_entries = new_data[~new_data['raceId'].isin(existing_data['raceId'])]
    
    if not new_entries.empty:
        updated_data = pd.concat([existing_data, new_entries], ignore_index=True).reset_index(drop=True)
        updated_data.to_csv(file_path, header=True, index=True if index_label else False, index_label=index_label)
        entry_list = set(new_entries['raceId'].to_list())
        logging.info(f'Added new entries for the {filename}: {entry_list}')
    else:
        logging.info(f'No new entries found for the {filename}')
        
class Dim_Updater:
    """
    A class used to update dimension tables.

    Attributes:
        dim_table (pd.DataFrame): The dimension table to be updated.
    """

    def __init__(self, dim_table):
        """
        Initializes the Updater class.

        Parameters:
            dim_table (pd.DataFrame): The dimension table to be updated.
        """
        self.dim_table = dim_table

    def add_new_entries(self, new_data, match_column, filename, id_column, entity_name, rename_dict, required_columns):
        """
        Adds new entries to the dimension table.

        Parameters:
            new_data (pd.DataFrame): The new data to be added.
            match_column (list): The column(s) to match for existing data.
            filename (str): The filename for the updated table.
            id_column (str): The column name for the ID.
            entity_name (str): The name of the entity.
            rename_dict (dict): The dictionary to rename columns.
            required_columns (list): The list of required columns in the new data.

        Returns:
            pd.DataFrame: The updated dimension table.
        """
        try:
            missing_cols = [col for col in required_columns if col not in new_data.columns]
            if missing_cols:
                error_msg = f"Missing {', '.join(missing_cols)} in new data for {entity_name}."
                logging.error(error_msg)
                raise ValueError(error_msg)
            
            new_data = new_data[required_columns]
            new_data = new_data.rename(columns=rename_dict)
            
            existing_combinations = set(tuple(x) for x in self.dim_table[match_column].dropna().to_records(index=False))
            new_data_combinations = set(tuple(x) for x in new_data[match_column].to_records(index=False))
            new_combinations = new_data_combinations - existing_combinations
            new_entries = new_data[new_data.apply(lambda x: tuple(x[match_column]) in new_combinations, axis=1)]
            new_entries = new_entries.drop_duplicates(subset=match_column)

            if new_entries.empty:
                logging.info(f"No new entries to append for {entity_name}.")
                return self.dim_table

            new_entries.insert(0, id_column, range(self.dim_table[id_column].max() + 1,
                                                   self.dim_table[id_column].max() + 1 + len(new_entries)))
            updated_table = pd.concat([self.dim_table, new_entries]).reset_index(drop=True)

            logging.info(f"New entries appended successfully for {entity_name}.")

            current_dir = os.getcwd()
            staging_path = os.path.join(current_dir, '..', 'Data', 'Staging')
            staging_path = os.path.normpath(staging_path)

            updated_table.to_csv(os.path.join(staging_path, f'{filename}.csv'), header=True, index=False)
            return updated_table
        except Exception as e:
            logging.error(f"An error occurred in {entity_name}: {str(e)}")
            raise

    def update_drivers(self, new_data):
        """
        Updates the drivers dimension table with new data.

        Parameters:
            new_data (pd.DataFrame): The new driver data.

        Returns:
            pd.DataFrame: The updated drivers dimension table.
        """
        return self.add_new_entries(new_data, 
                                    match_column=['driverRef'],
                                    id_column='driverId',
                                    entity_name='drivers',
                                    rename_dict={'DriverId': 'driverRef', 'DriverNumber': 'number', 'Abbreviation': 'code', 'FirstName': 'forename', 'LastName': 'surname'},
                                    required_columns=['DriverId', 'DriverNumber', 'Abbreviation', 'FirstName', 'LastName'],
                                    filename='drivers')

    def update_constructors(self, new_data):
        """
        Updates the constructors dimension table with new data.

        Parameters:
            new_data (pd.DataFrame): The new constructor data.

        Returns:
            pd.DataFrame: The updated constructors dimension table.
        """
        return self.add_new_entries(new_data, 
                                    match_column=['constructorRef'],
                                    id_column='constructorId', 
                                    entity_name='constructors',
                                    rename_dict={'TeamId': 'constructorRef', 'TeamName': 'name'},
                                    required_columns=['TeamId', 'TeamName'],
                                    filename='constructors')

    def update_races(self, new_data):
        """
        Updates the races dimension table with new data.

        Parameters:
            new_data (pd.DataFrame): The new race data.

        Returns:
            pd.DataFrame: The updated races dimension table.
        """
        new_data['year'] = new_data['EventDate'].dt.year
        new_data['date'] = new_data['EventDate'].dt.date
        
        return self.add_new_entries(new_data,                                     
                                    match_column=['year', 'round'],
                                    id_column='raceId', 
                                    entity_name='races',
                                    rename_dict={'RoundNumber': 'round', 'EventName': 'name', 'Circuit_ShortName': 'CircuitId'},
                                    required_columns=['RoundNumber', 'EventName', 'EventDate', 'year', 'Circuit_ShortName'],
                                    filename='races')

def update_laps(laps):
    """
    Updates the laps data.

    Parameters:
        laps (pd.DataFrame): The laps data to be updated.

    Returns:
        tuple: A tuple containing updated laps, fastest laps, and laps driven dataframes.
    """
    laps.rename(columns={'DriverId': 'driverRef',
                         'race_name': 'name',
                         'LapNumber': 'lap',
                         'Position': 'position'},
                         inplace=True)
    laps = laps.astype({'lap': 'int64'})

    dim_driver, _, dim_races = load_dimension_tables()

    laps = laps.merge(dim_driver[['driverRef', 'driverId', 'number']], 
                      how='left', 
                      on='driverRef'
                      ).merge(dim_races[['name', 'year', 'raceId']], 
                              how='left',
                              on=['name', 'year'])

    laps['total_seconds'] = laps['LapTime'].dt.total_seconds()
    laps['time'] = laps['total_seconds'].apply(lambda x: f"{int(x // 60)}:{(x % 60):06.3f}" if pd.notna(x) else np.nan)
    laps['milliseconds'] = laps['total_seconds'].apply(lambda x: int(x * 1000) if pd.notna(x) else np.nan)
    laps.drop(['total_seconds', 'LapTime'], axis=1, inplace=True)
    laps.dropna(axis=0, subset='milliseconds', inplace=True)
    fastest_laps = laps.loc[laps.groupby(['raceId', 'driverId'])['milliseconds'].idxmin()]

    # Rename columns for merging later
    fastest_laps = fastest_laps.rename(columns={
        'lap': 'fastestLap',
        'time': 'fastestLapTime'
    })

    laps_driven = laps[['raceId', 'driverId', 'lap']].groupby(['raceId', 'driverId'], as_index=False).max().rename(columns={'lap': 'laps'})

    # Add a rank column based on fastestLapTime within each race
    fastest_laps['rank'] = fastest_laps.groupby('raceId')['fastestLapTime'].rank(method='min')

    return laps, fastest_laps, laps_driven

def update_results(results, fastest_laps, laps_driven):
    """
    Updates the race results data.

    Parameters:
        results (pd.DataFrame): The race results data.
        fastest_laps (pd.DataFrame): The fastest laps data.
        laps_driven (pd.DataFrame): The laps driven data.

    Returns:
        pd.DataFrame: The updated race results data.
    """
    dim_driver, dim_constructors, dim_races = load_dimension_tables()

    results = results.merge(dim_driver[['driverRef', 'driverId', 'number']], how='left', left_on='DriverId', right_on='driverRef')
    results = results.merge(dim_constructors[['constructorRef', 'constructorId']], how='left', left_on='TeamId', right_on='constructorRef')
    results = results.merge(dim_races[['name', 'year', 'raceId']], left_on=['race_name', 'year'], right_on=['name', 'year'])
    results = results.merge(fastest_laps[['raceId', 'driverId', 'fastestLap', 'rank', 'fastestLapTime']],
                            on=['raceId', 'driverId'], how='left')
    results = results.merge(laps_driven, on=['raceId', 'driverId'])

    # Apply the calculation within each raceId group
    results = results.groupby('raceId').apply(calculate_total_times)

    # Convert 'Total Time' to milliseconds
    results['milliseconds'] = results['milliseconds'].dt.total_seconds() * 1000

    results = results[['raceId', 'driverId', 'constructorId', 'DriverNumber', 'GridPosition', 'ClassifiedPosition', 'Position', 'Points', 'laps', 'Time', 'milliseconds', 'fastestLap', 'rank', 'fastestLapTime']]
    # Rename columns for merging later
    results = results.rename(columns={
        'DriverNumber': 'number',
        'GridPosition': 'grid',
        'ClassifiedPosition': 'positionText',
        'Position': 'positionOrder',
        'Points': 'points',
        'Time': 'time'
    })
    
    return results

def update_qualifying(Q_results):
    """
    Updates the qualifying results data.

    Parameters:
        Q_results (pd.DataFrame): The qualifying results data.

    Returns:
        pd.DataFrame: The updated qualifying results data.
    """
    dim_driver, dim_constructors, dim_races = load_dimension_tables()

    Q_results = Q_results.merge(dim_driver[['driverRef', 'driverId', 'number']], how='left', left_on='DriverId', right_on='driverRef')
    Q_results = Q_results.merge(dim_constructors[['constructorRef', 'constructorId']], how='left', left_on='TeamId', right_on='constructorRef')
    Q_results = Q_results.merge(dim_races[['name', 'year', 'raceId']], left_on=['race_name', 'year'], right_on=['name', 'year'])

    Q_results = Q_results[['raceId', 'driverId', 'constructorId', 'DriverNumber', 'Position', 'Q1', 'Q2', 'Q3']]
    Q_results = Q_results.rename(columns={'DriverNumber': 'number',
                                          'Position': 'position',
                                          'Q1': 'q1',
                                          'Q2': 'q2',
                                          'Q3': 'q3'})
    return Q_results

def update_standings(results, sprint_results, races):
    """
    Updates the driver and constructor standings.

    Parameters:
        results (pd.DataFrame): The race results data.
        sprint_results (pd.DataFrame): The sprint race results data.
        races (pd.DataFrame): The races data.

    Returns:
        tuple: A tuple containing updated driver standings and constructor standings dataframes.
    """
    results['win'] = results['positionOrder'].apply(lambda x: 1 if x == 1 else 0)
    sprint_results['win'] = sprint_results['positionOrder'].apply(lambda x: 1 if x == 1 else 0)

    results['Type'] = 'Race'
    sprint_results['Type'] = 'Sprint'

    results = pd.concat([results, sprint_results], ignore_index=True)
    results = results.merge(races, how='left', on='raceId')

    # Sort results to ensure cumulative sum respects the round order
    results.sort_values(by=['year', 'round'], inplace=True)

    # Create cumulative sum of points and wins per driver per year up to the current round
    results['cumulative_points'] = results.groupby(['driverId', 'year'])['points'].cumsum()
    results['cumulative_wins'] = results.groupby(['driverId', 'year', 'Type'])['win'].cumsum()

    # Filter only Race wins for the cumulative wins calculation
    results = results[results['Type'] == 'Race']

    # Select relevant columns for driver standings
    driverpoint_df = results[['raceId', 'driverId', 'constructorId', 'cumulative_points', 'cumulative_wins']].copy()
    driverpoint_df.rename(columns={'cumulative_points': 'points', 'cumulative_wins': 'wins'}, inplace=True)

    driverpoint_df['position'] = driverpoint_df.groupby('raceId')['points'].rank(method='dense', ascending=False).astype(int)
    driverpoint_df['positionText'] = driverpoint_df['position']

    constructorpoints_df = driverpoint_df[['raceId', 'constructorId', 'points', 'wins']].groupby(['raceId', 'constructorId'], as_index=False).sum()

    constructorpoints_df['position'] = constructorpoints_df.groupby('raceId')['points'].rank(method='dense', ascending=False).astype(int)
    constructorpoints_df['positionText'] = constructorpoints_df[['position']]

    driverpoint_df.drop(axis=1, columns='constructorId', inplace=True)
    return driverpoint_df, constructorpoints_df
