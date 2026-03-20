import pymysql
import streamlit as st
from datetime import datetime, time

# cache so it doesn't rerun every time a widget changes
# given a start date and end date, fetch all tracking numbers within that date from the db 
@st.cache_data
def fetch_tracking_numbers(start_date, end_date):
  # connect to the SQL database with the enviroment variables 
  conn = pymysql.connect(
    host=str(st.secrets.MYSQL_HOST),
    port=int(st.secrets.MYSQL_PORT),
    user=str(st.secrets.MYSQL_USERNAME),
    password=str(st.secrets.MYSQL_PASSWORD),
    database=str(st.secrets.MYSQL_DATABASE),
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=True,
  )
  rows = []
  try:
    # instead of cur = conn.cursor() because with automatically closes
    with conn.cursor() as cur:
      sql = """
          SELECT DISTINCT tracking_number, created_at
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
