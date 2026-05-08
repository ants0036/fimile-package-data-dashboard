import streamlit as st
import pandas as pd
import concurrent.futures
import json 

from db import fetch_tracking_numbers, fetch_router_message
from response_parsing import find_delivery_zone, aggregate_weights, find_payable_weight, find_pickup_address, find_shipper_name
from api import find_package_details
from plot import plot_weight_chart, plot_area_chart, plot_pickup_chart

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
  response = fetch_router_message(package["tracking_number"])
  raw_json = response[0]["router_messages"]
  response_data = json.loads(raw_json)
  try:
    logs = response_data.get("listItemReadableStatusLogs")

    payable_weight = find_payable_weight(logs)
    # zipcode = find_delivery_zone(logs, area_zip_df)
    # pickup_address = find_pickup_address(logs)
    # shipper = find_shipper_name(logs)

    # package_customer = logs[len(logs)-1]["item"]["customerName"]
    # print(package_customer)
    # print(pickup_address)
    # return [payable_weight, zipcode, pickup_address, shipper]
    return [payable_weight]
  except: 
    st.write(response_data)

@st.cache_data
def load_area_zips():
  return pd.read_csv("data/zip-to-area.csv", dtype={"zipcode": str})

def run_weight_area_calculations(tracking_numbers):
  weights = []
  # delivery_areas = []
  # pickup_addresses = []
  # shippers = []
  
  area_zip_df = load_area_zips()
  counter_placeholder = st.empty()
  # Use ThreadPoolExecutor to run multiple API calls in parallel
  '''with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    # map returns results in the same order as input
    for package, result in enumerate(executor.map(lambda pkg: process_package(pkg, area_zip_df), tracking_numbers), start=1):
      weights.append(result[0])
      delivery_areas.append(result[1])
      pickup_addresses.append(result[2])
      shippers.append(result[3])
      counter_placeholder.write(f"Processed: {package}")'''
  
  for number in tracking_numbers:
    result = process_package(number, area_zip_df)
    weights.append(result[0])
    # delivery_areas.append(result[1])
    # pickup_addresses.append(result[2])
    # shippers.append(result[3])
    counter_placeholder.write(f"Processed: {number}")

  weights_df = pd.DataFrame(weights, columns=["weights"])
  counts = aggregate_weights(weights_df)
  plot_weight_chart(counts)

  # areas_df = pd.DataFrame(delivery_areas, columns=["areas"])
  # areas = areas_df["areas"].value_counts()
  # plot_area_chart(areas)

  # pickup_df = pd.DataFrame(pickup_addresses, columns=["pickup"])
  # pickup = pickup_df["pickup"].value_counts()
  # st.write(pickup)

  # shippers_df = pd.DataFrame(shippers, columns=["shipper"])
  # shippers = shippers_df["shipper"].value_counts()
  # st.write(shippers)

  # plot_pickup_chart(pickup)

# button to run payable weights function 
if st.button("calculate data from db results (click db button first)"):
  run_weight_area_calculations(st.session_state.tracking_numbers)
if st.button("use test data to calculate"):
  run_weight_area_calculations(test_tracking_data)

