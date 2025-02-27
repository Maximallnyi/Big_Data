from confluent_kafka import Consumer
import streamlit as st
import json
import pandas as pd
import time
import yaml
import matplotlib.pyplot as plt

st.set_page_config(page_title="Ticket Price Prediction", layout="wide")

if "MSE" not in st.session_state:
    st.session_state["MSE"] = []
if "true_values" not in st.session_state:
    st.session_state["true_values"] = []
if "predicted_values" not in st.session_state:
    st.session_state["predicted_values"] = []
if "airlines" not in st.session_state:
    st.session_state["airlines"] = []

with open('config.yaml') as cfg: 
    config = yaml.load(cfg, Loader=yaml.Loader)

model_results_consumer = Consumer(config['visualization_config'])
model_results_consumer.subscribe(["model_results"])

metrics_consumer = Consumer(config['visualization_config'])
metrics_consumer.subscribe(["metrics"])

st.subheader("MSE")
chart_holder_mse = st.empty()

st.subheader("True and Prediction values")
predictions_chart_holder = st.empty()

st.subheader("Last predictions")
predictions_table_holder = st.empty()

st.subheader("Price distribution")
price_distribution_holder = st.empty()

st.subheader("Airlines")
airlines_holder = st.empty()

while True:
    model_message = model_results_consumer.poll(10)
    if model_message is not None:
        model_data = json.loads(model_message.value().decode("utf-8"))
        true_value = model_data.get("true_value")
        predicted_value = model_data.get("predicted_value")

        if true_value is not None and predicted_value is not None:

            st.session_state["true_values"].append(true_value)
            st.session_state["predicted_values"].append(predicted_value)

            predictions_df = pd.DataFrame({
                "Real value": st.session_state["true_values"],
                "Predict value": st.session_state["predicted_values"]
            })

            predictions_chart_holder.line_chart(predictions_df)
            predictions_table_holder.dataframe(predictions_df.tail(5))

            st.session_state["airlines"].append(model_data.get("airline"))
            a = pd.Series(st.session_state["airlines"]).value_counts().to_dict()
            if st.session_state["airlines"]:
                fig, ax = plt.subplots()
                labels = list(a.keys())
                values = list(a.values())
                plt.bar(range(len(a)), values)
                plt.xticks(range(len(a)), labels)
                airlines_holder.pyplot(fig)
                plt.close(fig) 

            if st.session_state["true_values"]:
                fig, ax = plt.subplots()
                ax.hist(st.session_state["true_values"], bins=15, color='blue', alpha=0.6)
                ax.set_title("Ticket prices")
                price_distribution_holder.pyplot(fig)
                plt.close(fig) 

    metrics_message = metrics_consumer.poll(1.0)
    if metrics_message is not None:
        metrics_data = json.loads(metrics_message.value().decode("utf-8"))
        mse = metrics_data.get("MSE")
        if mse is not None:
            st.session_state["MSE"].append(mse)
            chart_holder_mse.line_chart(st.session_state["MSE"])
    time.sleep(0.1)