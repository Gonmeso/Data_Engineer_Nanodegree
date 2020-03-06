FROM python:3.6-slim
ADD requirements.txt /
RUN pip install --no-cache -r requirements.txt
WORKDIR /opt/dashboard
CMD  ["python", "app.py"]