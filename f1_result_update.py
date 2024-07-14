import fastf1 as ff1
from my_functions.update_functions import ff1_retriever, add_new_entries, update_results, update_laps

schedule = ff1.get_event_schedule(2024)

R_laps, R_results = ff1_retriever(2024, 'R')

R_laps, fastest_laps, laps_driven = update_laps(R_laps)
R_results = update_results(R_results, fastest_laps, laps_driven)

add_new_entries('results.csv', R_results, index_label = 'resultId')
add_new_entries('lap_times.csv', R_laps, index_label = None)
