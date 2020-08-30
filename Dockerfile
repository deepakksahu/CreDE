FROM python:3.7
USER root

WORKDIR /app

COPY . ./

RUN apt-get update && \
    apt-get install -y apt-utils && \
    apt-get upgrade -y && \
    apt-get install -y \
    python-dev \
    libssl-dev \
    libsasl2-dev\
    libatlas-base-dev\
    gcc \
    g++ \
    cron

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY config.py alerts.py models.py routes.py entry.sh ./
RUN chmod 777 routes.py
RUN chmod 777 /entry.sh
RUN "/entry.sh"

CMD cron && tail -f /var/log/cron.log