FROM apache/airflow:2.10.3

# Set the working directory
WORKDIR /opt/airflow

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the entire repository to the container
COPY . .

# Ensure the Airflow plugins directory is set correctly
ENV AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/airflow_book/plugins
ENV PYTHONPATH=/opt/airflow:/opt/airflow/airflow_book:$PYTHONPATH
