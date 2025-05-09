# Предсказание цен на авиабилеты с использованием Spark и Hadoop

Этот проект демонстрирует обработку данных о авиаперелётах с использованием Apache Spark и Hadoop HDFS. Проект включает сравнение производительности базовой и оптимизированной версий приложения на кластерах с разным количеством узлов.

## Описание задания
Использован датасет с данными по авиаперелётам с 100000 строками и 11 признаками https://www.kaggle.com/datasets/shubhambathwal/flight-price-prediction. Было проведено сравнение производительности для 4 экспериментов:
   - 1 DataNode
   - 1 DataNode + Opt
   - 3 DataNode
   - 3 DataNode + Opt

## Запуск
Был реализован bash скрипт для запуска контейнеров с Hadoop и Spark для различных конфигураций
Пример запуска оптимизированой версии для 3 DataNode
```bash
./run.sh
```

## Результаты
График плотности времени для 4 экспериментов
![photo_2025-04-21_17-23-33](https://github.com/user-attachments/assets/311389b8-4267-4c6d-80f1-fcb049a8a3a7)

График плотности памяти для 4 экспериментов
![photo_2025-04-21_17-23-36](https://github.com/user-attachments/assets/807a722b-92d1-4ac4-97e1-1058abad015e)

Таблица результатов со средним временем и средней затраченной памятью для всех экспериментов

![photo_2025-04-21_17-25-31](https://github.com/user-attachments/assets/fc9eccfe-c12a-49f8-80dd-366a0f5d7247)


**Выводы:**
- Оптимизированная версия на 3 узлах показала наилучшее время выполнения
- Неоптимизированная версия на 3 узлах показала наименьшие затраты по памяти
- Обе версии на 3 узлах работаю лучше, чем версии на 1 узле
