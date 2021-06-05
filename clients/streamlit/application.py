import json

import streamlit as st
import altair as alt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from datetime import datetime, timedelta

from colour import Color
from altair.expr import datum
from google.cloud import bigquery

client = bigquery.Client()

# load data index

# helpers

def fetch(query):
    return client.query(query).to_dataframe()

# website

st.set_page_config(page_title='Stargaze Follower')
st.markdown('''
    <style>
      .reportview-container .main .block-container {
        max-width: 1024px;
        padding-top: 0rem;
        padding-right: 2rem;
        padding-left: 2rem;
      }
    </style>
  ''',
  unsafe_allow_html=True,
)

st.markdown('# Stargaze Follower')

# generate

query = '''
    SELECT
        *
    FROM
        `danarchy-io.stargaze.sensor`
    ORDER BY
        date ASC
    limit 1
'''

df = fetch(query)

st.dataframe(df.T)

st.image('https://storage.cloud.google.com/danarchy-io/iotcore/images/last.jpg')