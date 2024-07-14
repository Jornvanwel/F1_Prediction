import fastf1 as ff1
import logging
from my_functions.update_functions import ff1_retriever, add_new_entries, update_qualifying

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get the schedule of a given year
schedule = ff1.get_event_schedule(2024)

_, Q_results = ff1_retriever(2024, 'Qualifying')

Q_results = update_qualifying(Q_results)
add_new_entries('qualifying.csv', Q_results)
