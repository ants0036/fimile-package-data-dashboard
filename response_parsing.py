import pandas as pd

# using the zone csv, find the zone the package is from 
def find_delivery_zone(logs, area_zip_df):
  if logs is None:
    return "None"
  package_zip = logs[len(logs)-1]["item"]["addressComponents"]["zipcode"]
  area = area_zip_df.loc[area_zip_df["zipcode"] == str(package_zip), ["area"]]
  if area.empty:
    return "None"
  else: 
    return area.iloc[0]["area"]
  
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

# given an api response, find the payable weight of the item 
def find_payable_weight(logs):
  if logs is None:
    return 0
  dims = logs[0]["item"]["dimensions"]["dims"]
  volume = dims[3]["v"]
  weight = dims[2]["v"]
  return max((int(volume)/250), int(weight))

# using the zone csv, find the zone the package is from 
def find_pickup_address(logs):
  if logs is None:
    return "None"
  pickup_address = logs[0]["item"]["formattedAddress"]
  return pickup_address