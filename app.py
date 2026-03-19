import streamlit as st
import pandas as pd
from matplotlib import pyplot
import concurrent.futures

from db import fetch_tracking_numbers
from response_parsing import find_delivery_zone
from api import find_package_details, aggregate_weights, find_payable_weight
from plot import plot_weight_chart, plot_area_chart

# start and end date picker
start_date = st.date_input("pick start date")
end_date = st.date_input("pick end date")
st.write("start date:", start_date, "end date:", end_date) 

# button to fetch from db & write the response 
# use session state so it doesn't rerun 
if st.button("fetch from db"):
   st.session_state.tracking_numbers = fetch_tracking_numbers(start_date, end_date)
if "tracking_numbers" in st.session_state:
    st.write(st.session_state.tracking_numbers)

# fake tracking data for testing
test_tracking_data = [
    {"tracking_number": "ZX34020380"},
    {"tracking_number": "ZX34067506"},
    {"tracking_number": "ZX34069792"},
    {"tracking_number": "ZX34144316"},
    {"tracking_number": "ZX34147985"},
    {"tracking_number": "ZX34186423"},
    {"tracking_number": "ZX34259421"},
    {"tracking_number": "ZX34297652"},
]

# returns an array of data taken from the package to be added to the dataframe. 
def process_package(package, area_zip_df):
  response = find_package_details(package["tracking_number"])
  payable_weight = find_payable_weight(response)
  zipcode = find_delivery_zone(response, area_zip_df)
  return [payable_weight, zipcode]

@st.cache_data
def load_area_zips():
  return pd.read_csv("data/zip-to-area.csv", dtype={"zipcode": str})

def run_weight_area_calculations(tracking_numbers):
  weights = []
  counter_placeholder = st.empty()

  delivery_areas = []
  area_zip_df = load_area_zips()

  # Use ThreadPoolExecutor to run multiple API calls in parallel
  with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    # map returns results in the same order as input
    for package, result in enumerate(executor.map(lambda pkg: process_package(pkg, area_zip_df), tracking_numbers), start=1):
      weights.append(result[0])
      delivery_areas.append(result[1])
      counter_placeholder.write(f"Processed: {package}")

  weights_df = pd.DataFrame(weights, columns=["weights"])
  counts = aggregate_weights(weights_df)
  plot_weight_chart(counts)
  areas_df = pd.DataFrame(delivery_areas, columns=["areas"])
  areas = areas_df["areas"].value_counts()
  plot_area_chart(areas)

# button to run payable weights function 
if st.button("calculate payable weights from db results (click db button first)"):
  run_weight_area_calculations(st.session_state.tracking_numbers)
if st.button("use test data to calculate"):
  run_weight_area_calculations(test_tracking_data)