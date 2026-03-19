import streamlit as st
import pandas as pd
from matplotlib import pyplot
import concurrent.futures

from db import fetch_tracking_numbers
from api import find_package_details

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

# given an api response, find the payable weight of the item 
def find_payable_weight(response):
  response_data = response.json() 
  logs = response_data.get("listItemReadableStatusLogs")
  if logs is None:
    return 0
  dims = logs[0]["item"]["dimensions"]["dims"]
  volume = dims[3]["v"]
  weight = dims[2]["v"]
  return max((int(volume)/250), int(weight))

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

def aggregate_weights(weights_df):
  # sort each weight into a category, then aggregate the categories 
  weights_df["range"] = pd.cut(
    weights_df["weights"],
    bins=[0, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150],
    labels=["0-30", "30-40", "40-50", "50-60", "60-70", "70-80", "80-90", "90-100", "100-110", "110-120", "120-130", "130-140", "140-150"]
  )
  counts = weights_df["range"].value_counts()
  counts = counts.sort_index()
  return counts

def plot_weight_chart(counts):
  fig, ax = pyplot.subplots()
  counts.plot.pie(ax=ax, 
                  autopct=lambda pct: f'{pct:.1f}%' if pct > 2 else '', )
  ax.set_title("Weights")
  st.pyplot(fig)

def plot_area_chart(areas):
  fig, ax = pyplot.subplots()
  areas.plot.pie(ax=ax, 
                  autopct=lambda pct: f"{int(round(pct * areas.sum() / 100))} ({pct:.1f}%)" if pct > 2 else "")
  ax.set_title("Areas")
  st.pyplot(fig)

@st.cache_data
def load_area_zips():
  return pd.read_csv("data/zip-to-area.csv", dtype={"zipcode": str})

def find_delivery_zone(response, area_zip_df):
  response_data = response.json() 
  logs = response_data.get("listItemReadableStatusLogs")
  if logs is None:
    return "None"
  package_zip = logs[len(logs)-1]["item"]["addressComponents"]["zipcode"]
  area = area_zip_df.loc[area_zip_df["zipcode"] == str(package_zip), ["area"]]
  if area.empty:
    return "None"
  else: 
    return area.iloc[0]["area"]

def run_weight_area_calculations(tracking_numbers):
  weights = []
  processed_num = 0
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