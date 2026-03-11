FROM apache/airflow:2.10.3

# Switch to the airflow user to install packages
USER airflow

# Copy the requirements file into the image
COPY requirements.txt /requirements.txt

# Install the requirements
RUN pip install --no-cache-dir -r /requirements.txt