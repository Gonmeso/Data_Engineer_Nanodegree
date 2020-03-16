FROM python:3.6-slim
WORKDIR /opt/dev_env
ADD requirements.txt ./
RUN pip install --no-cache -r requirements.txt
CMD ["sh","-c", "jupyter lab --notebook-dir='/opt/dev_env' --ip=0.0.0.0 --no-browser --allow-root --port=8888 --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.allow_origin='*'"]