#!/bin/bash
set -euo pipefail

PG_CONTAINER=postgres
CH_CONTAINER=clickhouse
DB=iman
USER=postgres

# 🧹 Cleanup
docker compose down -v

# 🚀 Start containers
echo "🟢 Starting Docker Compose services..."
docker compose up -d --build

# ⏳ Wait for readiness
sleep 10
until docker exec $PG_CONTAINER pg_isready -U $USER; do sleep 2; done
until docker exec zookeeper bash -c 'echo ruok | nc -w 1 localhost 2181'; do sleep 2; done

# Ensure ClickHouse database exists
echo "🗃 Creating ClickHouse database and schema '$DB'..."
docker exec -i $CH_CONTAINER clickhouse-client --query "CREATE DATABASE IF NOT EXISTS $DB;"
docker exec -i $CH_CONTAINER clickhouse-client --query "USE $DB;"

# 📦 Extract schema and generate DDL
echo "📦 Extracting PostgreSQL schema and generating ClickHouse DDL..."
DDL=$(docker exec $PG_CONTAINER psql -U $USER -d $DB -Atc "SELECT 'CREATE TABLE IF NOT EXISTS iman.users (' ||
  string_agg(column_name || ' ' ||
    CASE data_type
      WHEN 'integer' THEN 'Int32'
      WHEN 'bigint' THEN 'Int64'
      WHEN 'numeric' THEN 'Decimal(18,2)'
      WHEN 'text' THEN 'String'
      WHEN 'character varying' THEN 'String'
      WHEN 'timestamp without time zone' THEN 'DateTime'
      WHEN 'uuid' THEN 'UUID'
      ELSE 'String'
    END, ', ') || ') ENGINE = ReplacingMergeTree() ORDER BY user_id;' \
  FROM information_schema.columns \
  WHERE table_name = 'users';")

# 🛠 Apply DDL
echo "🛠 Applying generated DDL to ClickHouse..."
docker exec $CH_CONTAINER clickhouse-client --query "$DDL"

# 🔌 Register PostgreSQL Source Connector
echo "🧩 Registering Debezium PostgreSQL source connector..."
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
echo "🧩 Registering ClickHouse Sink connector..."
curl -X POST http://localhost:8084/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "clickhouse-sink-connector",
    "config": {
      "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
      "topics": "postgres_cdc.iman.users",
      "topics.mapping": "postgres_cdc.iman.users:users",
      "hostname": "clickhouse",
      "port": "8123",
      "database": "iman",
      "user": "default",
      "password": "password",
      "secure": "false",
      "compression": "false",
      "group.id": "clickhouse-connect-group",
      "consumer.override.auto.offset.reset": "earliest",
      "consumer.override.enable.auto.commit": "false",
      "consumer.override.max.poll.records": "1000",
      "consumer.override.session.timeout.ms": "60000",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": true,
      "value.converter.schemas.enable": true,
      "tasks.max": "1",
      "auto.create.tables": "true",
      "auto.evolve.tables": "true",
      "table.engine": "ReplacingMergeTree",
      "table.primaryKey": "user_id",
      "deduplication.policy": "preserve_insert_order",
      "errors.tolerance": "all",
      "errors.log.enable": "true",
      "errors.log.include.messages": "true"
    }
  }'


# 📝 Insert test rows into Postgres
echo "📝 Inserting test rows into PostgreSQL..."
docker exec -i $PG_CONTAINER psql -U $USER -d $DB <<EOF
INSERT INTO iman.users (user_id, username, account_type, updated_at, created_at)
VALUES
  (1, 'user1', 'Bronze', now(), now()),
  (2, 'user2', 'Silver', now(), now()),
  (3, 'user3', 'Gold', now(), now());
EOF

# ⌛ Wait for sync
sleep 10

# ✅ Integration verification
echo "📊 Rows in PostgreSQL:"
docker exec -i $PG_CONTAINER psql -U $USER -d $DB -c "SELECT * FROM iman.users"

echo "📬 Messages in Kafka topic:"
docker exec broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic postgres_cdc.iman.users \
  --from-beginning --timeout-ms 5000

echo "🏁 Rows in ClickHouse:"
docker exec $CH_CONTAINER clickhouse-client --query "SELECT * FROM iman.users FORMAT Vertical"

# ✅ Final assertion
ACTUAL=$(docker exec $CH_CONTAINER clickhouse-client --query "SELECT user_id, username, account_type FROM iman.users ORDER BY user_id FORMAT TSV" || echo "ERROR")
EXPECTED=$'1\tuser1\tBronze\n2\tuser2\tSilver\n3\tuser3\tGold'

if [[ "$ACTUAL" == "$EXPECTED" ]]; then
  echo "✅ Integration test passed. Data synced correctly."
else
  echo "❌ Integration test failed. Data mismatch."
  echo -e "\nExpected:\n$EXPECTED\nGot:\n$ACTUAL"
  exit 1
fi
