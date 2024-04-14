FROM apache/airflow:2.8.3
COPY requirements.txt .
RUN pip install -r requirements.txt

USER root
# install google chrome
RUN apt-get -y update
RUN apt-get install -y wget
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable

# install chromedriver
RUN apt-get install -yqq unzip
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

# Set permissions to allow writing and reading
RUN mkdir /usr/local/airflow/dags
RUN chmod 777 /usr/local/airflow/dags

# set display port to avoid crash
ENV DISPLAY=:99

USER 1001