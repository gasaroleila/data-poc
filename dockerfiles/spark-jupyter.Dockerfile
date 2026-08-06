FROM python:3.11-slim

# Install Java (required by PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PYSPARK_PYTHON=python3

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    delta-spark==3.2.1 \
    jupyterlab==4.2.5 \
    pandas \
    numpy

WORKDIR /workspace

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]
