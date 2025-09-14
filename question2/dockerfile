FROM apache/airflow:2.8.0

USER airflow

# Copy requirements.txt
COPY requirement.txt /requirement.txt

# Install dependencies as airflow user
RUN pip install --no-cache-dir -r /requirement.txt
 