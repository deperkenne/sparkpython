FROM openjdk:17.0.12-jdk
# Téléchargez et installez Spark
ENV SPARK_VERSION=3.5.3
RUN curl -fSL "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o spark.tgz \
    && tar -xzf spark.tgz -C /opt \
    && rm spark.tgz \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

# Définir les variables d'environnement pour Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Exposer les ports Spark
EXPOSE 7077 8080

# Point d'entrée par défaut
ENTRYPOINT ["/opt/spark/bin/spark-shell"]