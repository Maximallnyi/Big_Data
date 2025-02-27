import json
import time
import random
import yaml
from confluent_kafka import Consumer, Producer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import pandas as pd
import numpy as np

class DataAnalysis:
    def __init__(self, config):
        
        self.consumer = Consumer(config['model_consumer_config'])
        self.training_producer = Producer(config['model_producer_config'])
        self.metrics_producer = Producer(config['analysis_producer_config'])

    def start_model(self, config):

        data = []
        ys = []

        model = LinearRegression()
        init_data = pd.read_csv('data/init.csv')
        model.fit(init_data.iloc[:,:-1], init_data.iloc[:,-1])

        self.consumer.subscribe([config['processing']['processing_topic']]) 

        while True:
            message = self.consumer.poll(1000)
            if message is None:
                continue
            message_value = message.value().decode('utf-8')
            try:
                sample = json.loads(message_value)
                prepared_data_tf = pd.Series(sample['transformed'])
                data.append(pd.Series(sample['transformed'])[:-1])
                ys.append(pd.Series(sample['transformed'])[-1])
                y_true = prepared_data_tf[-1]
                if len(data) == 1:
                    model.fit(prepared_data_tf[:-1].to_numpy().reshape(1,-1), np.array([y_true]))
                else:
                    model.fit(data, ys)
                y_pred = model.predict(prepared_data_tf[:-1].to_numpy().reshape(1,-1))[0]
                send_data = {   'true_value': y_true,
                                'predicted_value': y_pred,
                                'metric_rmse': mean_squared_error([y_true], [y_pred]),
                                'airline': sample['not_transformed']['airline']}
                self.training_producer.produce(
                    config['model_topic'], key='1',
                    value=json.dumps(send_data)
                )
                self.training_producer.flush()

                self.metrics_producer.produce(
                    config['analysis_topic'], key='1',
                    value=json.dumps({"MSE": mean_squared_error([y_true], [y_pred])})
                )
                self.metrics_producer.flush()

                time.sleep(random.uniform(0.5, 1.0))

            except json.JSONDecodeError as e:
                print(f"Ошибка: {e}")
                print(f"Сообщение: {message_value}")

def main():
    with open('config.yaml') as cfg: 
        config = yaml.load(cfg, Loader=yaml.Loader)

    model = DataAnalysis(config)
    model.start_model(config)

if __name__ == "__main__":
    main()
    