FROM apache/airflow:2.5.1

USER airflow

# Copiar el archivo de requerimientos y luego instalar los paquetes necesarios
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt