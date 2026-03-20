import streamlit as st
from matplotlib import pyplot

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

def plot_pickup_chart(pickup):
  fig, ax = pyplot.subplots()
  pickup.plot.bar(ax=ax)
  ax.set_title("Pickup Addresses")
  st.pyplot(fig)

