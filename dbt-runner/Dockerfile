FROM python:3.12-slim
RUN --mount=type=cache,target=/root/.cache/pip pip install --timeout 120 --retries 8 dbt-core==1.9.4 dbt-postgres==1.9.0
WORKDIR /workspace
COPY dbt_project/flight_analytics .
COPY dbt-runner/run_loop.py /run_loop.py
ENV DBT_PROFILES_DIR=/workspace
CMD ["python", "-u", "/run_loop.py"]
