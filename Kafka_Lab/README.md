# Предсказание цен на авиабилеты

Для выполнения данного проекта был выбран следующий датасет https://www.kaggle.com/datasets/shubhambathwal/flight-price-prediction/code?datasetId=1957837&sortBy=voteCount

Данные считываются с помощью Apache Kafka, а затем строится визуализация при помощи streamlit  

## Визуализация
### График MSE
![image](https://github.com/user-attachments/assets/6231cabb-9626-4e21-b0a1-afbb88ba9d8c)
### График реальных и предсказанных значений
![image](https://github.com/user-attachments/assets/3ef1aeb2-0d0a-4d91-9ab2-f074778fc64a)
### Последние 5 предсказаний
![image](https://github.com/user-attachments/assets/006e7e3e-b1c7-46c7-9c66-d66662fc9bde)
### Распределение цен на билеты
![image](https://github.com/user-attachments/assets/f434a151-7e4f-44a1-8164-2a5579d1d5d9)
### Количество рейсов по авиалиниям
![image](https://github.com/user-attachments/assets/585be2de-d210-4460-83d5-c6dec561c4b0)


## Запуск
- Скачайте docker образ kafka: `docker pull bitnami/kafka`
- `pip install -r requirements.txt`
- `docker-compose up -d`
- Запуск скриптов:
```
python3 backend/data_load.py 
python3 backend/processing.py
python3 backend/model.py
streamlit run frontend/visualizer.py
```
