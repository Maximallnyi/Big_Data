import json
import yaml
from confluent_kafka import Consumer, Producer
import threading


class DataProcessing:
    def __init__(self, config, topic, input_topic, columns):
        self.config = config
        self.topic = topic
        
        self.encoder_dict = {}

        for i in columns:
            self.encoder_dict[i] = {}

        self.consumer = Consumer(config['processing_consumer_config'])
        self.consumer.subscribe([input_topic])
        
        self.producer = Producer(config['processing_producer_config'])
    
    def start_process(self):
        while True:
            msg = self.consumer.poll(10)
            
            if msg is not None:
                
                data_dict = json.loads(msg.value())
                message = {}
                message['not_transformed'] = data_dict.copy()
                for i in data_dict:
                    if type(data_dict[i]) == str:
                        if len(self.encoder_dict[i].keys()) == 0:
                            self.encoder_dict[i][data_dict[i]] = 0
                        if data_dict[i] not in self.encoder_dict[i].keys():
                            self.encoder_dict[i][data_dict[i]] = max(self.encoder_dict[i].values()) + 1
                        data_dict[i] = self.encoder_dict[i][data_dict[i]]

                
                message['transformed'] = data_dict
                self.producer.produce(topic=self.topic, key='1', value=json.dumps(message))
                self.producer.flush()
                
def main():
    with open('config.yaml') as cfg: 
        config = yaml.load(cfg, Loader=yaml.Loader)

    num_producers = 3

    Data_Process = DataProcessing(
        config=config['processing'],
        topic = config['processing']['processing_topic'],
        input_topic = config['raw_data_topic'],
        columns = config['columns']
    )

    threads = []
    for i in range(num_producers):
        thread = threading.Thread(
            target=Data_Process.start_process
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    

if __name__ == "__main__":
    main()