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

                session_laps['date'] = session_date
                session_results['date'] = session_date

                session_laps['info'] = session_info
                session_results['info'] = session_info

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

#Q_laps, Q_results, Q_drivers = ff1_retriever(schedule, 'Q')
R_laps, R_results = ff1_retriever(schedule, 'R')
Q_laps, Q_results = ff1_retriever(schedule, 'Q')

dim_driver = pd.read_csv(os.path.join(staging_path, 'drivers.csv'), on_bad_lines='skip', header= 0, delimiter= ',')
updated_dim_table = add_new_drivers(dim_driver, R_results)

dim_constructors = pd.read_csv(os.path.join(staging_path, 'constructors.csv'), on_bad_lines='skip', header= 0, delimiter= ',')