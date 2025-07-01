import streamlit as st, duckdb, pandas as pd, plotly.express as px, os

# read the latest parquet produced by GitHub Action
file = "holdings.parquet"
if not os.path.exists(file):
    st.error("No data yet – wait for the first scrape to run.")
    st.stop()

df = duckdb.read_parquet(file)
latest = df[df.filing_date == df.filing_date.max()]

st.title("Scion (Michael Burry) 13-F Tracker")
st.caption("Auto-updated after every new SEC filing")

st.subheader("Current holdings")
st.dataframe(latest[['ticker','shares','value']])

st.subheader("Allocation by ticker (value)")
fig = px.pie(latest, names="ticker", values="value", hole=0.4)
st.plotly_chart(fig, use_container_width=True)

# simple GPT summary button
import openai, os, json, textwrap
openai.api_key = os.getenv("OPENAI_API_KEY")

if st.button("Explain the latest filing with GPT-3.5"):
    prompt = (
        "Summarise this table of holdings for a finance-savvy reader:\n\n"
        + latest[['ticker','shares','value']].to_csv(index=False)
    )
    resp = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-0125",
        messages=[{"role":"user","content":prompt}],
        temperature=0.4,
    )
    st.write(resp.choices[0].message.content.strip())
