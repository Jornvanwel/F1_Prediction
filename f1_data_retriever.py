import os
import numpy as np
import pandas as pd 
from datetime import datetime, timezone
import fastf1 as ff1
from fastf1.core import Laps
from fastf1 import utils
from fastf1 import plotting
import csv
import unicodedata
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

current_dir = os.getcwd()
cache_path = os.path.join(current_dir, '..', 'Data', 'cache')
cache_path = os.path.normpath(cache_path)

staging_path = os.path.join(current_dir, '..', 'Data', 'Staging')
staging_path = os.path.normpath(staging_path)

ff1.Cache.enable_cache(cache_path)

current_date = datetime.now(timezone.utc)
current_year = current_date.year
schedule = ff1.get_event_schedule(2024)

def extract_important_info(session_info):
    return {
        'race_name': session_info['Meeting']['Name'],
        'Location': session_info['Meeting']['Location'],
        'Country_Name': session_info['Meeting']['Country']['Name'],
        'Country_Code': session_info['Meeting']['Country']['Code'],
        'Circuit_ShortName': session_info['Meeting']['Circuit']['ShortName']
    }

def ff1_retriever(schedule, racetype = 'R'):
    laps_total = pd.DataFrame()
    results_total = pd.DataFrame()
    for round in range(1, len(schedule)):
        try:
            event = schedule.get_event_by_round(round)
            event_time = event.get_session_date(round, utc=True).replace(tzinfo=timezone.utc)
            if event_time <= current_date:
                session = event.get_session(racetype)
                session.load()
                session_laps = session.laps
                session_results = session.results
                session_info = session.session_info
                session_date = event_time
                
                session_laps['year'] = session_date.year
                session_results['year'] = session_date.year

                session_laps = session_laps.merge(session_results[['Abbreviation', 'DriverId']], how = 'left', left_on = 'Driver', right_on = 'Abbreviation')
                
                important_info = extract_important_info(session_info)
                for key, value in important_info.items():
                    session_laps[key] = value
                    session_results[key] = value

                laps_total = pd.concat([laps_total, session_laps], ignore_index=True)
                results_total = pd.concat([results_total, session_results], ignore_index=True)
        except:
            pass
    return laps_total, results_total

def add_new_drivers(dim_table, new_data):
    """
    Appends new entries to a dimension table while updating the 'driverId' column to maintain a sequential order.
    
    Args:
    dim_table (pd.DataFrame): The dimension table containing existing data.
    new_data (pd.DataFrame): New data to be appended.
    
    Returns:
    pd.DataFrame: Updated dimension table with new entries appended.
    """
    try:
        # Check for required columns in new data
        if 'DriverId' not in new_data.columns:
            logging.error("Missing 'DriverId' in new data.")
            raise ValueError("New data must include 'DriverId' column.")
        
        # Identify new entries not present in the dimension table
        existing_ids = dim_table['driverRef'].unique()
        new_entries = new_data[~new_data['DriverId'].isin(existing_ids)]
        
        if new_entries.empty:
            logging.info("No new entries to append.")
            return dim_table
        
        # Prepare new entries to match dimension table structure
        new_entries = new_entries.rename(columns={'DriverId': 'driverRef',
                                                  'DriverNumber': 'number',
                                                  'Abbreviation': 'code',
                                                  'FirstName': 'forename',
                                                  'LastName': 'surname'})
        
        # Remove additional columns not needed in the dimension table
        new_entries = new_entries[['driverRef', 'number', 'code', 'forename', 'surname']]

        if new_entries.isnull().any().any():
            logging.warning("NA values found in the new entries. Proceeding with available data.")
        
        # Create a new driverId
        new_entries.insert(0, 'driverId', range(dim_table['driverId'].max() + 1, dim_table['driverId'].max() + 1 + len(new_entries)))
        
        # Append and reset index for good measure
        updated_table = pd.concat([dim_table, new_entries]).reset_index(drop=True)
        
        logging.info("New entries appended successfully.")
        return updated_table
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise

def add_new_constructors(dim_table, new_data):
    """
    Appends new entries to a dimension table while updating the 'driverId' column to maintain a sequential order.
    
    Args:
    dim_table (pd.DataFrame): The dimension table containing existing data.
    new_data (pd.DataFrame): New data to be appended.
    
    Returns:
    pd.DataFrame: Updated dimension table with new entries appended.
    """
    try:
        # Check for required columns in new data
        if 'TeamId' not in new_data.columns:
            logging.error("Missing 'TeamId' in new data.")
            raise ValueError("New data must include 'TeamId' column.")
        
        # Identify new entries not present in the dimension table
        existing_ids = dim_table['constructorRef'].unique()
        new_entries = new_data[~new_data['TeamId'].isin(existing_ids)]
        
        if new_entries.empty:
            logging.info("No new entries to append.")
            return dim_table
        
        # Prepare new entries to match dimension table structure
        new_entries = new_entries.rename(columns={'TeamId': 'constructorRef',
                                                  'TeamName': 'name'})
        
        # Remove additional columns not needed in the dimension table
        new_entries = new_entries[['constructorRef', 'name']]

        if new_entries.isnull().any().any():
            logging.warning("NA values found in the new entries. Proceeding with available data.")
        
        # Create a new driverId
        new_entries.insert(0, 'constructorId', range(dim_table['constructorId'].max() + 1, dim_table['constructorId'].max() + 1 + len(new_entries)))
        
        # Append and reset index for good measure
        updated_table = pd.concat([dim_table, new_entries]).reset_index(drop=True)
        
        logging.info("New entries appended successfully.")
        return updated_table
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise

def add_new_races(dim_table, new_data):
    """
    Appends new entries to a dimension table while updating the 'driverId' column to maintain a sequential order.
    
    Args:
    dim_table (pd.DataFrame): The dimension table containing existing data.
    new_data (pd.DataFrame): New data to be appended.
    
    Returns:
    pd.DataFrame: Updated dimension table with new entries appended.
    """
    try:
        # Check for required columns in new data
        if 'RoundNumber' not in new_data.columns:
            logging.error("Missing 'TeamId' in new data.")
            raise ValueError("New data must include 'TeamId' column.")
        
        # Identify new entries not present in the dimension table
        existing_ids = dim_table['year'].unique()

        new_data['year'] = new_data['EventDate'].dt.year

        new_entries = new_data[~new_data['year'].isin(existing_ids)]
        
        if new_entries.empty:
            logging.info("No new entries to append.")
            return dim_table
        
        # Prepare new entries to match dimension table structure
        new_entries = new_entries.rename(columns={'RoundNumber': 'round',
                                              'EventName': 'race_name',
                                              'EventDate': 'date'})
        
        # Remove additional columns not needed in the dimension table
        new_entries = new_entries[['year', 'round', 'race_name', 'date']]

        if new_entries.isnull().any().any():
            logging.warning("NA values found in the new entries. Proceeding with available data.")
        
        # Create a new driverId
        new_entries.insert(0, 'raceId', range(dim_table['raceId'].max() + 1, dim_table['raceId'].max() + 1 + len(new_entries)))
        
        # Append and reset index for good measure
        updated_table = pd.concat([dim_table, new_entries]).reset_index(drop=True)
        
        logging.info("New entries appended successfully.")
        return updated_table
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise

R_laps, R_results = ff1_retriever(schedule, 'R')
Q_laps, Q_results = ff1_retriever(schedule, 'Q')
S_laps, S_results = ff1_retriever(schedule, 'S')

dim_driver = pd.read_csv(os.path.join(staging_path, 'drivers.csv'), on_bad_lines='skip', header= 0, delimiter= ',')
updated_dim_driver = add_new_drivers(dim_driver, R_results)

dim_constructors = pd.read_csv(os.path.join(staging_path, 'constructors.csv'), on_bad_lines='skip', header= 0, delimiter= ',')
updated_dim_constructor = add_new_constructors(dim_constructors, R_results)

dim_races = pd.read_csv(os.path.join(staging_path, 'races.csv'), on_bad_lines='skip', header= 0, delimiter= ',')
updated_dim_races = add_new_races(dim_races, schedule)

R_laps_updated = R_laps.merge(updated_dim_races[['race_name', 'year', 'raceId']], on = ['race_name', 'year'])
R_laps_updated = R_laps_updated.merge(updated_dim_driver[['driverRef', 'driverId']], how = 'left', left_on = 'DriverId', right_on = 'driverRef')
R_laps_updated = R_laps_updated[['raceId', 'driverId', 'LapNumber', 'Position', 'LapTime']]

R_laps_updated = R_laps_updated.rename(columns={'LapNumber': 'lap',
                                                'Position': 'position'})

R_laps_updated['total_seconds'] = R_laps_updated['LapTime'].dt.total_seconds()
R_laps_updated['time'] = R_laps_updated['total_seconds'].apply(lambda x: f"{int(x // 60)}:{(x % 60):06.3f}" if pd.notna(x) else np.nan)
R_laps_updated['milliseconds'] = R_laps_updated['total_seconds'].apply(lambda x: int(x * 1000) if pd.notna(x) else np.nan)
R_laps_updated.drop(['total_seconds', 'LapTime'], axis=1, inplace=True)
R_laps_updated.dropna(inplace= True)

fact_laps = pd.read_csv(os.path.join(staging_path, 'lap_times.csv'), on_bad_lines='skip', header= 0, delimiter= ',')
fact_laps_updated = pd.concat([fact_laps, R_laps_updated], ignore_index = True)

R_results_updated = R_results.merge(updated_dim_driver, how = 'left', left_on = 'DriverId', right_on = 'driverRef')
R_results_updated = R_results_updated.merge(updated_dim_constructor, how = 'left', left_on = 'TeamId', right_on = 'constructorRef')
R_results_updated = R_results_updated.merge(updated_dim_races[['race_name', 'year', 'raceId']], on = ['race_name', 'year'])

# Find the fastest lap per driver per race
fastest_laps = fact_laps_updated.loc[fact_laps_updated.groupby(['raceId', 'driverId'])['milliseconds'].idxmin()]

# Rename columns for merging later
fastest_laps = fastest_laps.rename(columns={
    'lap': 'fastestLap',
    'time': 'fastestLapTime'
})

# Add a rank column based on fastestLapTime within each race
fastest_laps['rank'] = fastest_laps.groupby('raceId')['fastestLapTime'].rank(method='min')

# Reset index if necessary
fastest_laps.reset_index(drop=True, inplace=True)

# Merge with results_df to add fastest lap info to race results
R_results_updated = R_results_updated.merge(fastest_laps[['raceId', 'driverId', 'fastestLap', 'rank', 'fastestLapTime']],
                              on=['raceId', 'driverId'], how='left')

R_results_merged = R_results_updated[['raceId', 'driverId', 'constructorId', 'DriverNumber', 'GridPosition', 'ClassifiedPosition', 'Position', 'Points', 'Time', 'fastestLap', 'rank', 'fastestLapTime']]
# Rename columns for merging later
R_results_merged = R_results_merged.rename(columns={
    'DriverNumber': 'number',
    'GridPosition': 'grid',
    'ClassifiedPosition': 'positionText',
    'Position': 'positionOrder',
    'Points': 'points',
    'Time': 'time'
    })

fact_results = pd.read_csv(os.path.join(staging_path, 'results.csv'), on_bad_lines='skip', header= 0, delimiter= ',')
fact_results_updated = pd.concat([fact_results, R_results_merged], ignore_index = True)

Q_results = R_results_updated.rename(columns={
    'DriverNumber': 'number',
    'GridPosition': 'position',
    'Q1': 'q1',
    'Q2': 'q2',
    'Q3': 'q3'
    })

Q_results = Q_results[['raceId', 'driverId', 'constructorId', 'number', 'position', 'q1', 'q2', 'q3']]
fact_qualifying = pd.read_csv(os.path.join(staging_path, 'qualifying.csv'), on_bad_lines='skip', header= 0, delimiter= ',')
fact_qualifying_updated = pd.concat([fact_qualifying, Q_results])

