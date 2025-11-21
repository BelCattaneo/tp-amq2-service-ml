# Trabajo Práctico - MLOps1
### CEIA - FIUBA

## Tarea a realizar

La tarea es implementar el modelo que desarrollaron en Aprendizaje de Máquina en este ambiente productivo. Para ello, pueden usar y crear los buckets y bases de datos que necesiten. Lo mínimo que deben realizar es:

- Un DAG en Apache Airflow. Puede ser cualquier tarea que se desee realizar, como entrenar el modelo, un proceso ETL, etc.
- Un experimento en MLflow de búsqueda de hiperparámetros.
- Servir el modelo implementado en AMq1 en el servicio de RESTAPI.
- Documentar (comentarios y docstring en scripts, notebooks, y asegurar que la documentación de FastAPI esté de acuerdo al modelo).

Desde **ML Models and something more Inc.** autorizan a extender los requisitos mínimos. También pueden utilizar nuevos servicios (por ejemplo, una base de datos no relacional, otro orquestador como MetaFlow, un servicio de API mediante NodeJs, etc.).

## Ejemplo

El [branch `example_implementation`](https://github.com/facundolucianna/amq2-service-ml/tree/example_implementation) contiene un ejemplo de aplicación para guiarse. Se trata de una implementación de un modelo de clasificación utilizando los datos de [Heart Disease](https://archive.ics.uci.edu/dataset/45/heart+disease).

Además se cuenta con una implementación ejemplo de predicción en bache con una parte que funciona gran parte de local en [branch `batch-example`](https://github.com/facundolucianna/amq2-service-ml/tree/example_implementation)
