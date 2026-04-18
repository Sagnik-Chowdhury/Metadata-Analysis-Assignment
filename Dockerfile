FROM python:3.11-slim
RUN pip install requests
COPY main.py .
CMD ["python", "main.py"]