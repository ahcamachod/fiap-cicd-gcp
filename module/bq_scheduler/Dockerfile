# this is purely to run utils commands to create or replace bigquery procedures
FROM python:3.12-slim

# Update and install system packages
RUN apt-get update -y && \
    apt-get install --no-install-recommends -y -q \
    python3-pip python-dev-is-python3 build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir /bq_scheduler

# Install Biqguery module
RUN pip install -U pip
RUN pip install google-cloud-bigquery
RUN pip install google-cloud-bigquery-datatransfer
RUN pip install protobuf

COPY . /bq_scheduler


ENV BQ_DIR /bq_scheduler

# Set working directory
WORKDIR $BQ_DIR

# Run python
CMD [ "python"]