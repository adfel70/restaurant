FROM python:3.11
WORKDIR /app
ADD requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app/
EXPOSE 80
CMD [ "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "80" ]




