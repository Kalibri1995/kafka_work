FROM confluentinc/cp-kafka-connect:latest

# Указываем папку для плагинов
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

# Устанавливаем JDBC Connector и драйвер для PostgreSQL
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest \
    && curl -o /usr/share/java/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.2.27.jar
