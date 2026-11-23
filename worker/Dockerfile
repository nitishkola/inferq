# Build context is the repo root (the worker uses the shared /lua scripts).
FROM python:3.12-slim
WORKDIR /app
COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY lua /app/lua
COPY worker/inferq_worker /app/inferq_worker
ENV LUA_DIR=/app/lua PYTHONUNBUFFERED=1
EXPOSE 9100
STOPSIGNAL SIGTERM
CMD ["python", "-m", "inferq_worker"]
