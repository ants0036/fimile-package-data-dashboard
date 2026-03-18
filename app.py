import pymysql
import streamlit as st
from dotenv import load_dotenv
import os 
import requests 
from datetime import datetime, time
import pandas as pd

# start and end date picker
start_date = st.date_input("pick start date")
end_date = st.date_input("pick end date")
st.write("start date:", start_date, "end date:", end_date)

# cache so it doesn't rerun every time a widget changes
# given a start date and end date, fetch all tracking numbers within that date from the db 
@st.cache_data
def fetch_tracking_numbers():
  # connect to the SQL database with the enviroment variables 
  load_dotenv()
  conn = pymysql.connect(
    host=str(os.getenv("MYSQL_HOST")),
    port=int(os.getenv("MYSQL_PORT")),
    user=str(os.getenv("MYSQL_USERNAME")),
    password=str(os.getenv("MYSQL_PASSWORD")),
    database=str(os.getenv("MYSQL_DATABASE")),
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=True,
  )
  rows = []
  try:
    # instead of cur = conn.cursor() because with automatically closes
    with conn.cursor() as cur:
      sql = """
          SELECT DISTINCT tracking_number
          FROM waybill_waybills
          WHERE created_at >= %s AND created_at < %s
          AND tracking_number IS NOT NULL AND tracking_number <> ''
          ORDER BY tracking_number ASC
      """
      # convert to datetime object 
      start_dt = datetime.combine(start_date, time.min)
      end_dt   = datetime.combine(end_date, time.max)
      cur.execute(sql, (start_dt, end_dt))
    # could use fetchmany but lazy? not enough rows to do so?
    rows = cur.fetchall()
  finally:
      conn.close()
  return rows 

# button to fetch from db & write the response 
# use session state so it doesn't rerun 
if st.button("fetch from db"):
   st.session_state.tracking_numbers = fetch_tracking_numbers()
if "tracking_numbers" in st.session_state:
    st.write(st.session_state.tracking_numbers)

# given a singular tracking number, fetch the details of the package from the beans api 
# idk if i should cache this since it's unlikely this will be repeated again but it's worth a shot?
@st.cache_data
def find_package_details (tracking_number):
  API_URL_TEMPLATE = "https://isp.beans.ai/enterprise/v1/lists/status_logs?tracking_id={tracking_id}&readable=true&include_item=true"
  url = API_URL_TEMPLATE.format(tracking_id=tracking_number)
  api_key = os.getenv("API_TOKEN")
  headers = {"Accept": "application/json",
            "Authorization": api_key}

  response = requests.get(url, headers=headers)
  return response

# given an api response, find the payable weight of the item 
def find_payable_weight(response):
  response_data = response.json() 
  logs = response_data.get("listItemReadableStatusLogs")
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

# button to run payable weights function 
if st.button("calculate payable weights from db results (test)"):
  # for x in st.session_state.tracking_numbers:
  weights = []
  processed_num = 0
  counter_placeholder = st.empty()

  for package in test_tracking_data:
    processed_num += 1
    response = find_package_details(package["tracking_number"])
    payable_weight = find_payable_weight(response)
    counter_placeholder.write(f"Processed: {processed_num}")
    weights.append(payable_weight)
  weights_df = pd.DataFrame(weights, columns=["weights"])
  st.write(weights_df)
    