FROM python:3.6-slim
WORKDIR /opt/dashboard
COPY . .
RUN pip install --no-cache -r requirements.txt
WORKDIR /opt/dashboard
CMD  ["python", "app.py"]