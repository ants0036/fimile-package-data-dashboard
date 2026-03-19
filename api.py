import streamlit as st
import requests 

# given a singular tracking number, fetch the details of the package from the beans api 
# idk if i should cache this since it's unlikely this will be repeated again but it's worth a shot?
@st.cache_data
def find_package_details (tracking_number):
  API_URL_TEMPLATE = "https://isp.beans.ai/enterprise/v1/lists/status_logs?tracking_id={tracking_id}&readable=true&include_item=true"
  url = API_URL_TEMPLATE.format(tracking_id=tracking_number)
  api_key = st.secrets.API_TOKEN
  headers = {"Accept": "application/json",
            "Authorization": api_key}
  response = requests.get(url, headers=headers)
  return response