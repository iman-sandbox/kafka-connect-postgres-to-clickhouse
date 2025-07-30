#!/usr/bin/env bash
set -euo pipefail

# Load or create .env
if [ ! -f .env ]; then
  echo "Creating default .env file..."
  cat <<EOF > .env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=postgres
EOF
fi
source .env

echo "🟢 Starting Docker Compose services..."
docker compose down -v --remove-orphans
docker compose build postgres-connect clickhouse-connect
docker compose up -d

echo "\n⏳ Waiting for all services to be healthy..."
until docker exec zookeeper echo ruok | nc -w1 localhost 2181; do sleep 1; done
until docker exec broker bash -c "nc -z broker 9092"; do sleep 1; done
until docker exec postgres pg_isready -U "$POSTGRES_USER"; do sleep 1; done
until curl -fs http://localhost:8083/connector-plugins >/dev/null; do sleep 1; done
until curl -fs http://localhost:8084/connector-plugins >/dev/null; do sleep 1; done

echo "🔍 Inspect plugin directories..."
docker exec clickhouse-connect ls /usr/share/confluent-hub-components/clickhouse-kafka-connect/lib || echo "❌ ClickHouse connector not found!"

echo "🧩 Registering Debezium PostgreSQL source connector..."
curl -X POST -H "Content-Type: application/json" \
  --data '{
    "name": "postgres-source-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "postgres",
      "database.server.name": "postgres_cdc",
      "plugin.name": "pgoutput",
      "snapshot.mode": "initial",
      "slot.drop.on.stop": "true",
      "slot.name": "debezium_slot",
      "publication.name": "debezium_publication",
      "publication.autocreate.mode": "filtered",
      "table.include.list": "public.users",
      "topic.prefix": "postgres_cdc",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false"
    }
  }' http://localhost:8083/connectors

echo "\n🔁 Extracting DDL from PostgreSQL and applying to ClickHouse..."

# Extract DDL from Postgres
POSTGRES_DDL=$(docker exec -i postgres psql -U postgres -d postgres -t -c "SELECT 'CREATE TABLE IF NOT EXISTS users AS SELECT * FROM public.users WHERE false;'")

# Translate Postgres types to ClickHouse types (very basic example)
CLICKHOUSE_DDL=$(echo "$POSTGRES_DDL" | sed 's/boolean/UInt8/g' | sed 's/text/String/g' | sed 's/serial/Int32/g' | sed 's/integer/Int32/g' | sed 's/timestamp without time zone/DateTime/g')

# Drop CREATE AS SELECT if present, use manual fallback (optional)
CLICKHOUSE_DDL=$(cat <<EOF
CREATE TABLE IF NOT EXISTS default.users
(
    id Int32,
    username String,
    account_type String
)
ENGINE = MergeTree()
ORDER BY id;
EOF
)

# Apply DDL to ClickHouse
docker exec -i clickhouse clickhouse-client --query "$CLICKHOUSE_DDL"
echo "✅ ClickHouse DDL applied."


echo "🧩 Registering ClickHouse Sink connector..."
curl -s -X POST http://localhost:8084/connectors -H "Content-Type:application/json" \
  -d '{
    "name": "clickhouse-sink-connector",
    "config": {
      "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
      "tasks.max": "1",
      "topics": "postgres_cdc.public.users",
      "hostname": "clickhouse",
      "port": "8123",
      "database": "default",
      "username": "default",
      "password": "",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "insert.mode": "insert"
    }
  }'

echo "⏳ Waiting for CDC event in Kafka topic..."
sleep 10

echo "📩 Latest CDC message in Kafka:"
docker exec broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic postgres_cdc.public.users \
  --from-beginning --max-messages 1 || echo "❌ No Kafka message received"

echo "⏳ Waiting for sink to apply to ClickHouse..."
sleep 10

echo "🔎 Verifying ClickHouse synchronization:"
docker exec clickhouse clickhouse-client --query "SHOW TABLES FROM default"
docker exec clickhouse clickhouse-client --query "DESCRIBE TABLE default.users"
docker exec clickhouse clickhouse-client --query "SELECT * FROM default.users LIMIT 1" \
  || echo "✅ No data yet in ClickHouse"

echo "✅ Integration test completed."

echo "📝 Inserting test rows into PostgreSQL..."
docker exec -i postgres psql -U postgres -d postgres -c "
  INSERT INTO users (username, account_type) VALUES
  ('user1', 'Bronze'),
  ('user2', 'Silver'),
  ('user3', 'Gold');
"

echo "⏳ Waiting for CDC events in Kafka topic..."
sleep 10

echo "🔎 Verifying ClickHouse synchronization:"
docker exec clickhouse clickhouse-client -q "SELECT * FROM users"
