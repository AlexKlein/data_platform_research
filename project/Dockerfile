# Use Apache Airflow as the base image
FROM apache/airflow:2.7.3

# Developer and maintainer of the project
LABEL maintainer="Aleksandr Klein"

# Set environment varialbes
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV FLYWAY_VERSION 7.7.3

# Set the working directory for convenience
WORKDIR /opt/airflow

# Switch to ROOT user for installing mandatory packages
USER root

# Install mandatory packages
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        vim \
 && apt-get autoremove -yqq --purge \
 && apt-get clean \
 && apt-get install -y libpq-dev gcc \
 && rm -rf /var/lib/apt/lists/*

# Install Flyway
RUN curl -L https://repo1.maven.org/maven2/org/flywaydb/flyway-commandline/${FLYWAY_VERSION}/flyway-commandline-${FLYWAY_VERSION}-linux-x64.tar.gz | tar -xz  \
 && mv flyway-${FLYWAY_VERSION} /opt/flyway  \
 && ln -s /opt/flyway/flyway /usr/local/bin/flyway  \
 && chown -R airflow: /opt/flyway

# Switch back to the default Airflow user
USER airflow

# Copy requirements.txt into the Docker container
COPY requirements.txt /opt/airflow/requirements.txt

# Install needed Python packages
RUN pip install --upgrade pip \
 && pip install --trusted-host pypi.python.org -r /opt/airflow/requirements.txt \
 && mkdir -p /tmp/downloads/data

# Copy your dags folder to the container
COPY airflow/dags /opt/airflow/dags

# Copy entrypoint script to the container
COPY entrypoint.sh /entrypoint.sh

# Run the ini script
ENTRYPOINT ["/entrypoint.sh"]
