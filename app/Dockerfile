FROM python:3.12-slim
WORKDIR /workspace
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip pip install --timeout 120 --retries 8 -r requirements.txt
COPY app app
COPY postgres postgres
COPY tests tests
COPY scripts scripts
RUN useradd --uid 10001 --create-home pipeline
USER pipeline
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "app.producer"]