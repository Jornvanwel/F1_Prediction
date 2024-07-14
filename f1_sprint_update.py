import fastf1 as ff1
from my_functions.update_functions import ff1_retriever, add_new_entries, update_results, update_laps

# Get the schedule of a given year
schedule = ff1.get_event_schedule(2024)

Sprint_laps, Sprint_results = ff1_retriever(2024, 'Sprint')

Sprint_laps, fastest_laps, laps_driven = update_laps(Sprint_laps)
Sprint_results = update_results(Sprint_results, fastest_laps, laps_driven)

add_new_entries('sprint_results.csv', Sprint_results, index_label = 'resultId')
