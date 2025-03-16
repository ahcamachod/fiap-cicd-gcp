# this is purely to run dbt commands
FROM python:3.12-slim

# Update and install system packages
RUN apt-get update -y && \
    apt-get install --no-install-recommends -y -q \
    python3-pip python-dev-is-python3 build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir /routines 

# Install DBT
RUN pip install -U pip
RUN pip install google-cloud-bigquery

COPY . /routines

ENV DBT_DIR /routines

# Set working directory
WORKDIR $DBT_DIR

# Run python
CMD [ "python"]