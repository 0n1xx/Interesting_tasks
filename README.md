### О репозитории:
____

В данном репозитории будут находиться интересные задачи, которые я встречал с тестовых заданий, проектов, курсов или просто где-то на форумах.

- [Общие элементы](https://github.com/0n1xx/Interesting_tasks/blob/main/Personal_projects/common_elements/code.ipynb):

  Главная задача этого проекта написать скрипт, который рекурентно будет искать общие строки в датафрейме, такая вещь может понадобиться при вычесление преступников в транзакциях. К примеру, мы знаем, что у нас есть данные о странной транзакции и можно отследить где эти параметры еще встречаются.
  
- [Парсинг квартир](https://github.com/0n1xx/Interesting_tasks/blob/main/Personal_projects/parcing_flats/parcing_flats_avito.ipynb):

Основная цель данного проекта изучить как можно парсить данные, используя библиотеку Beatiful Soup, а также изучить Docker, сейчас проект находится в стадии разработки, при этом я уже написал парсер и скрипт обработки, который отбирает квартиры по 15% персентилю в зависимости от станции метро и после фильтров отправляет в [телеграмм канал](https://t.me/moscow_flats_bot). 

Что уже сделано:

1. Написан парсер, при этом точно стоит заняться его оптимизацией;
2. Написаны функции по очистке, фильтрации и отправке данных о квартир в телеграм;
3. Развернуты все контейнеры кроме airflow, а именно clickhouse и superset, при этом объединены в сеть, чтобы они видели друг друга;

Что осталось сделать:

1. Оптимизировать скрипт;
2. Развернуть airflow и добавить его в сеть с 2 описанными выше контейнерами;
3. Построить дашборд в Superset и подумать какие ключевые метрики там можно трекать.


- [Даг по трудоустройству](https://github.com/0n1xx/Interesting_tasks/blob/main/Personal_projects/employment_dag/employment_dag.py):

Основная цель проекта - это собирать данные с excel файла по трудоустройтсву студентов и после отправлять очищенные и пред подготовленные данные в бд, а после на дашборд. Получившийся дашборд можно посмотреть [тут](https://github.com/0n1xx/Interesting_tasks/blob/main/Personal_projects/employment_dag/employment_dag.png), при этом в Superset я насписал большой запрос, который считает показатель трудоустройства в зависимости от группы, а также применяя некоторые фильтры, сам запрос [тут](https://github.com/0n1xx/Interesting_tasks/blob/main/Personal_projects/employment_dag/employment_kpi.sql).

