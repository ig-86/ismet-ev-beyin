# Taban imaj sabit: Supervisor'ın BUILD_FROM argümanı (HA Alpine tabanı) kullanılmıyor,
# çünkü firebase-admin'in gRPC bağımlılığı Debian (glibc) üzerinde hazır paketle kuruluyor.
FROM python:3.13-slim-bookworm

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY beyin.py .

CMD ["python", "-u", "/app/beyin.py"]
