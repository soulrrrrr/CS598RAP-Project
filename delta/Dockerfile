FROM python:3.11-bullseye

# Install OpenJDK 11 from Debian Bullseye repos
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless curl procps && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install PySpark 3.4.1 and Delta Lake 2.4.0 (compatible versions)
RUN pip3 install --no-cache-dir \
    pyspark==3.4.1 \
    delta-spark==2.4.0

# Create working directory
WORKDIR /workspace

CMD ["/bin/bash"]
