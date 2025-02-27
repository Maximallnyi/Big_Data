import json
import time
import random
import yaml
from confluent_kafka import Producer
import threading
import pandas as pd


class DataLoader:
    def __init__(self, data_path, config, topic):
        self.data_path = data_path
        self.config = config
        self.topic = topic
        self.data = pd.read_csv(data_path)
        

    def start_producer(self, producer_id):
        producer = Producer(self.config)

        while True:
            input_i = random.randint(0, self.data.shape[0] - 1)
            sample = self.data.iloc[input_i].to_dict()
            message = json.dumps(sample)
            
            producer.produce(self.topic, key=str(producer_id), value=message)
            producer.flush()
            print(f"Producer {producer_id} sent: {message}")
            time.sleep(random.uniform(0.5, 1.0))


def main():

    with open('config.yaml') as cfg: 
        config = yaml.load(cfg, Loader=yaml.Loader)

    num_producers = 3

    Data_Miner = DataLoader(
        data_path = 'data/Clean_Dataset.csv',
        config=config['raw_data_config'],
        topic = config['raw_data_topic']
    )

    threads = []
    for i in range(num_producers):
        thread = threading.Thread(
            target=Data_Miner.start_producer,
            args=(i + 1,)
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    

if __name__ == "__main__":
    main()