FROM confluentinc/cp-kafka-connect:6.1.1

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

COPY --chown=appuser:appuser build/libs/kafka-smt-jsonpath-tools.jar /usr/share/java

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:latest
