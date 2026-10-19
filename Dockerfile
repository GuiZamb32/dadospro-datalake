
# Imagem única do projeto: gerador, testes e pipeline PySpark.
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /projeto

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Extensão S3 do DuckDB baixada no build.
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs')"

# Conector s3a: versões compatíveis com o Hadoop usado pelo PySpark.
ARG HADOOP_AWS=3.5.0
ARG AWS_SDK_V2_BUNDLE=2.35.4
ARG ANALYTICS_ACCELERATOR=1.3.1

RUN JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
 && MAVEN=https://repo.maven.apache.org/maven2 \
 && curl -fsSL --retry 5 --retry-all-errors --retry-delay 2 \
      -o "$JARS/hadoop-aws-$HADOOP_AWS.jar" \
      "$MAVEN/org/apache/hadoop/hadoop-aws/$HADOOP_AWS/hadoop-aws-$HADOOP_AWS.jar" \
 && curl -fsSL --retry 5 --retry-all-errors --retry-delay 2 \
      -o "$JARS/bundle-$AWS_SDK_V2_BUNDLE.jar" \
      "$MAVEN/software/amazon/awssdk/bundle/$AWS_SDK_V2_BUNDLE/bundle-$AWS_SDK_V2_BUNDLE.jar" \
 && curl -fsSL --retry 5 --retry-all-errors --retry-delay 2 \
      -o "$JARS/analyticsaccelerator-s3-$ANALYTICS_ACCELERATOR.jar" \
      "$MAVEN/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/$ANALYTICS_ACCELERATOR/analyticsaccelerator-s3-$ANALYTICS_ACCELERATOR.jar"

# Integração do Log4j utilizada na configuração original do projeto.
ARG LOG4J=2.25.4

RUN JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
 && test -f "$JARS/log4j-core-$LOG4J.jar" \
 && curl -fsSL --retry 5 --retry-all-errors --retry-delay 2 \
      -o "$JARS/log4j-slf4j-impl-$LOG4J.jar" \
      "https://repo.maven.apache.org/maven2/org/apache/logging/log4j/log4j-slf4j-impl/$LOG4J/log4j-slf4j-impl-$LOG4J.jar"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python
