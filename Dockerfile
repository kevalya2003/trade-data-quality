# Airflow image with both halves of the system installed.
#
# The build context is the directory *above* this one, because the DAG needs the
# pipeline project as well as this one: tradepnl supplies the load and aggregation
# steps, tradedq the validation and quarantine steps. Once the pipeline project is
# published, this would install it from its repository instead and the context could
# shrink back to this directory.
#
# Build with:  docker compose build
FROM apache/airflow:3.3.0-python3.12

COPY --chown=airflow:root trade-pnl-pipeline /opt/projects/trade-pnl-pipeline
COPY --chown=airflow:root trade-data-quality /opt/projects/trade-data-quality

RUN pip install --no-cache-dir \
        /opt/projects/trade-pnl-pipeline \
        /opt/projects/trade-data-quality

# Great Expectations writes its project directory and the generated data docs here.
# Creating it with the right ownership in the image matters because Docker seeds a
# named volume from the image's content on first use -- mount it over a root-owned
# path and the tasks, which run as the airflow user, cannot write to it.
USER root
RUN mkdir -p /opt/airflow/gx-project \
    && chown -R airflow:root /opt/airflow/gx-project \
    && chmod -R g+w /opt/airflow/gx-project
USER airflow
