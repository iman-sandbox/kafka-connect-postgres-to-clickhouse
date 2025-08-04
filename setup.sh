#!/usr/bin/env bash
set -e

function wait_for_service() {
  local name="$1"
  local url="$2"
  local tries=60
  echo -n "⏳ Waiting for $name to be fully ready..."
  for i in $(seq 1 $tries); do
    if curl -s "$url" >/dev/null; then
      echo "✅"
      return 0
    fi
    sleep 1
    echo -n "."
  done
  echo "❌ $name did not start in time"
  exit 1
}

function wait_for_tcp() {
  local name="$1"
  local host="$2"
  local port="$3"
  local tries=60
  echo -n "⏳ Waiting for $name to be fully ready..."
  for i in $(seq 1 $tries); do
    if nc -z "$host" "$port"; then
      echo "✅"
      return 0
    fi
    sleep 1
    echo -n "."
  done
  echo "❌ $name did not start in time"
  exit 1
}

echo "🟢 Starting Docker Compose services..."
docker compose down -v --remove-orphans
docker compose up -d --build

wait_for_tcp "Zookeeper" "localhost" 2181
wait_for_tcp "Kafka Broker" "localhost" 9092
wait_for_tcp "Postgres" "localhost" 5432
wait_for_service "ClickHouse" "http://localhost:8123/ping"
wait_for_service "Postgres Connect" "http://localhost:8083/"
wait_for_service "ClickHouse Connect" "http://localhost:8084/"

echo "🗃 Creating ClickHouse database and schema 'iman'..."
docker exec clickhouse clickhouse-client --query "CREATE DATABASE IF NOT EXISTS iman;"

echo
echo "📦 Extracting PostgreSQL schema and generating ClickHouse DDLs for all tables..."
docker exec -i postgres psql -U postgres -d iman -c "\d+ iman.*"

for table in users; do
  echo
  echo "🔍 Processing table: iman.$table"
  CH_DDL="CREATE TABLE IF NOT EXISTS iman.$table (user_id UInt32, username String, account_type String, updated_at DateTime64(6), created_at DateTime64(6)) ENGINE = ReplacingMergeTree() ORDER BY user_id;"
  echo "🛠 Applying DDL to ClickHouse for table iman.$table:"
  echo "$CH_DDL"
  docker exec clickhouse clickhouse-client --query "$CH_DDL"
done

echo
echo "🧩 Registering Postgres DDL connector..."
curl -s -X POST -H "Content-Type: application/json" \
    --data @<(cat <<EOF
{
  "name": "postgres-ddl-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "iman",
    "schema.include.list": "iman",
    "database.server.name": "postgres_cdc_ddl",
    "topic.prefix": "postgres_cdc_ddl",
    "plugin.name": "wal2json",
    "slot.name": "debezium_slot_ddl",
    "publication.autocreate.mode": "all_tables",
    "publication.name": "dbz_publication_ddl",
    "snapshot.mode": "initial",
    "slot.drop.on.stop": "true",
    "include.schema.changes": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true",
    "name": "postgres-ddl-connector"
  }
}
EOF
) http://localhost:8083/connectors

echo "🧩 Registering ClickHouse DDL Sink connector..."
curl -s -X POST -H "Content-Type: application/json" \
    --data @<(cat <<EOF
{
  "name": "clickhouse-ddl-sink-connector",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "topics": "schema-changes.postgres_cdc_ddl",
    "hostname": "clickhouse",
    "port": "8123",
    "database": "iman",
    "user": "default",
    "password": "password",
    "auto.create.tables": "true",
    "auto.evolve.tables": "true",
    "table.engine": "ReplacingMergeTree",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true",
    "consumer.override.auto.offset.reset": "earliest",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",
    "tasks.max": "1",
    "name": "clickhouse-ddl-sink-connector"
  }
}
EOF
) http://localhost:8084/connectors

echo -e "\n🧩 Registering Debezium PostgreSQL source connector..."
curl -X PUT http://localhost:8083/connectors/postgres-source-connector/config \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-source-connector",
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "iman",
    "schema.include.list": "iman",
    "database.server.name": "postgres_cdc",
    "topic.prefix": "postgres_cdc",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.autocreate.mode": "filtered",
    "snapshot.mode": "initial",
    "slot.drop.on.stop": "true",
    "tombstones.on.delete": "false",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": true,
    "value.converter.schemas.enable": true,

    "producer.override.acks": "all",
    "producer.override.retries": 10,
    "producer.override.delivery.timeout.ms": 60000,

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "true"
  }'

# 🔌 Register ClickHouse Sink Connector
echo -e "\n🧩 Registering ClickHouse Sink connector..."
curl -X PUT http://localhost:8084/connectors/clickhouse-sink-connector/config \
  -H "Content-Type: application/json" \
  -d '{
    "name": "clickhouse-sink-connector",
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "topics": "postgres_cdc.iman.users",

    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "^postgres_cdc\\.iman\\.(.*)$",
    "transforms.route.replacement": "$1",



    "table.name.format": "${topic}",

    "hostname": "clickhouse",
    "port": "8123",
    "database": "iman",
    "user": "default",
    "password": "password",

    "auto.create.tables": "true",
    "auto.evolve.tables": "true",
    "table.engine": "ReplacingMergeTree",
    "table.primaryKey": "user_id",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true",

    "consumer.override.auto.offset.reset": "earliest",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",
    "tasks.max": "1"
  }'



echo "📝 Inserting test rows into PostgreSQL..."
docker exec -i postgres psql -U postgres -d iman <<EOF
INSERT INTO iman.users (user_id, username, account_type, updated_at, created_at) VALUES
  (1, 'user1', 'Bronze', now(), now()),
  (2, 'user2', 'Silver', now(), now()),
  (3, 'user3', 'Gold', now(), now());
EOF

echo
echo "📊 Rows in PostgreSQL:"
docker exec -i postgres psql -U postgres -d iman -c "SELECT * FROM iman.users;"

echo
echo "📬 Messages in Kafka topic:"
docker exec -i broker kafka-console-consumer --bootstrap-server broker:29092 \
  --topic postgres_cdc.iman.users --from-beginning --timeout-ms 10000 | tee /dev/tty | tail -n 3

echo
echo "🏁 Rows in ClickHouse:"
docker exec clickhouse clickhouse-client --query "SELECT * FROM iman.users FORMAT Pretty"

echo
echo "🧪 Deleting a user in Postgres..."
docker exec -i postgres psql -U postgres -d iman -c "DELETE FROM iman.users WHERE user_id = 2;"

echo
echo "📋 Verifying deletion in ClickHouse..."
docker exec clickhouse clickhouse-client --query "SELECT * FROM iman.users FORMAT Pretty"

echo
echo "🧪 Inserting then updating a user..."
docker exec -i postgres psql -U postgres -d iman <<EOF
INSERT INTO iman.users (user_id, username, account_type, updated_at, created_at) VALUES (999, 'test_user', 'Test', now(), now());
UPDATE iman.users SET username = 'updated_user' WHERE user_id = 999;
EOF

echo
echo "📋 Verifying insert and update in ClickHouse..."
docker exec clickhouse clickhouse-client --query "SELECT * FROM iman.users FORMAT Pretty"

echo
echo "⏳ Waiting for DDL connector to be fully ready..."
for i in {1..30}; do
  status=$(curl -s http://localhost:8083/connectors/postgres-ddl-connector/status | grep '"state"' | grep -c RUNNING)
  if [ "$status" -ge 1 ]; then
    echo "✅ DDL connector is RUNNING."
    break
  fi
  sleep 1
done

echo
echo "🛠 Applying ALTER TABLE on Postgres..."
docker exec -i postgres psql -U postgres -d iman -c "ALTER TABLE iman.users ADD COLUMN new_col TEXT;"

# --- Wait for ClickHouse schema update after ALTER TABLE ---
echo "⏳ Waiting for ClickHouse to receive new_col..."
for i in {1..60}; do
  has_col=$(docker exec clickhouse clickhouse-client --query "DESCRIBE TABLE iman.users" | grep -c new_col || true)
  if [ "$has_col" -ge 1 ]; then
    echo "✅ new_col appeared in ClickHouse."
    break
  fi
  sleep 1
done

echo
echo "➕ Inserting 5 rows with new_col..."
for i in {1001..1005}; do
  docker exec -i postgres psql -U postgres -d iman -c "INSERT INTO iman.users (user_id, username, account_type, updated_at, created_at, new_col) VALUES ($i, 'user${i}', 'Bronze', now(), now(), 'test$i');"
done

echo
echo "✏️ Updating one of the new rows..."
docker exec -i postgres psql -U postgres -d iman -c "UPDATE iman.users SET new_col = 'updated_val' WHERE user_id = 1003;"

echo
echo "📋 Verifying ALTER + INSERT + UPDATE in ClickHouse..."
docker exec clickhouse clickhouse-client --query "SELECT user_id, username, new_col FROM iman.users WHERE user_id BETWEEN 1001 AND 1005 FORMAT Pretty"
