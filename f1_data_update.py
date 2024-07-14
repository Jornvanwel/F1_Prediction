import fastf1 as ff1
from my_functions.update_functions import ff1_retriever, Updater, load_dimension_tables

# Get the schedule of a given year
schedule = ff1.get_event_schedule(2024)

# Retrieve lap data and results data 
R_laps, R_results = ff1_retriever(2024, 'R')

dim_drivers, dim_constructors, dim_races = load_dimension_tables()

updater = Updater(dim_drivers)
updated_drivers = updater.update_drivers(R_results)

updater = Updater(dim_constructors)
updated_constructors = updater.update_constructors(R_results)

updater = Updater(dim_races)
updated_races = updater.update_races(schedule)

