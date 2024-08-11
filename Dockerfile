FROM apache/airflow:2.4.2

RUN python -m pip install --upgrade pip
RUN pip install python-dotenv
RUN pip install boto3
RUN pip install pandas
RUN pip install pyyaml

# ALternatively Copy requirements.txt and Install its Contents.
#RUN python -m pip install --upgrade pip
#COPY requirements.txt requirements.txt
#RUN pip install -r requirements.txt