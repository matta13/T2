FROM python:3.11-slim
WORKDIR /app

# Instalar dependencias del cliente
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el script del cliente
COPY client.py /app/client.py

ENV API_URL=http://qa_api:8000/ask

CMD ["tail", "-f", "/dev/null"]