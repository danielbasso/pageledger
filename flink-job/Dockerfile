# PageLedger Flink image: official Flink 1.20.5 (Ubuntu 22.04 Jammy, Java 17) + PyFlink.
#
# Python 3.11 — NOT 3.12 — on purpose: apache-flink==1.20.5 pins apache-beam<2.49,
# which has no cp312 wheels (Beam added 3.12 only in ~2.53), so 3.12 forces a source
# build of beam that fails. 3.11 is inside PyFlink 1.20's supported 3.9–3.12 range and
# installs entirely from binary wheels. Producer/API images stay on 3.12.
FROM flink:1.20.5-java17

USER root

# Python 3.11 via deadsnakes (Jammy has no bundled python), pip via ensurepip.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends software-properties-common gpg-agent curl; \
    add-apt-repository -y ppa:deadsnakes/ppa; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3.11 python3.11-venv python3.11-dev; \
    ln -sf /usr/bin/python3.11 /usr/local/bin/python; \
    ln -sf /usr/bin/python3.11 /usr/local/bin/python3; \
    python3.11 -m ensurepip --upgrade; \
    python3.11 -m pip install --no-cache-dir --upgrade pip; \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN python3.11 -m pip install --no-cache-dir -r /tmp/requirements.txt

# Connector JARs on the cluster classpath (/opt/flink/lib is auto-loaded).
# Versions verified against Maven Central for the Flink 1.20 line.
# NOTE on the JDBC connector version: PyFlink 1.20.5's DataStream JdbcSink.sink Python
# binding calls internal.JdbcOutputFormat.createRowJdbcStatementBuilder(int[]) and the
# old org.apache.flink.connector.jdbc.JdbcSink. In connector 3.2.0+ that method was moved
# to a renamed class (RowJdbcOutputFormat), so 3.2/3.3 raise NoSuchMethodException.
# 3.1.2-1.18 is the newest release that keeps the old layout the bundled PyFlink expects;
# its JDBC output-format internals are stable on the 1.20 runtime. (Verified by grepping
# the method across connector versions against Maven Central.)
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar /opt/flink/lib/
RUN chmod 644 /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar \
             /opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar \
             /opt/flink/lib/postgresql-42.7.13.jar

# The PyFlink job drivers.
COPY *.py /opt/flink/usrlib/

USER flink
