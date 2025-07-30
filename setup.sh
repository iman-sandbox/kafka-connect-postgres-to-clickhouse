#!/usr/bin/env bash
set -e

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
  echo "Creating default .env file..."
  cat <<EOF > .env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=postgres
EOF
fi

# Load env variables
source .env

echo "Starting Docker Compose services..."
docker compose down -v
docker compose up -d

echo "Waiting for services to be healthy..."
for service in zookeeper broker postgres connect; do
  until [ "$(docker inspect -f '{{.State.Health.Status}}' $(docker compose ps -q $service))" = "healthy" ]; do
    printf "."
    sleep 2
  done
  echo "$service is healthy."
done

echo "Registering Debezium PostgreSQL source connector..."
curl -s -X POST http://localhost:8083/connectors   -H "Content-Type: application/json"   --data '{
    "name": "postgres-source-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "plugin.name": "pgoutput",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "'"$POSTGRES_USER"'",
      "database.password": "'"$POSTGRES_PASSWORD"'",
      "database.dbname": "'"$POSTGRES_DB"'",
      "database.server.name": "postgres_cdc",
      "topic.prefix": "postgres_cdc",
      "slot.name": "debezium_slot",
      "publication.autocreate.mode": "filtered",
      "table.include.list": "public.users",
      "snapshot.mode": "initial",
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.delete.handling.mode": "drop"
    }
  }'
echo ""
echo "Debezium source connector registered. Kafka topic 'postgres_cdc.public.users' will receive events."

echo "Running integration test..."

# Insert a test row into Postgres
docker exec -i postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
"INSERT INTO users (username, account_type) VALUES ('cdc_test_user', 'Tester');"

# Give Debezium time to publish the event
sleep 5

# Read the last 10 messages from Kafka topic into a log file
docker exec broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic postgres_cdc.public.users \
  --from-beginning \
  --timeout-ms 10000 \
  --max-messages 10 > /tmp/kafka_test_output.log

# Show latest messages
echo "Last CDC messages in Kafka topic:"
cat /tmp/kafka_test_output.log

# Check for the test user in payload
if grep -q '"username":"cdc_test_user"' /tmp/kafka_test_output.log; then
  echo "✅ CDC pipeline test passed: test user found in Kafka topic."
else
  echo "❌ CDC pipeline test FAILED: test user not found in Kafka topic."
fi
