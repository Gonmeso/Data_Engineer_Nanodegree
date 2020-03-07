FROM python:3.6-slim
WORKDIR /opt/dev_env
COPY . .
RUN chmod +x start.sh
RUN pip install --no-cache -r requirements.txt
CMD ["./start.sh"]