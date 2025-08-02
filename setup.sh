#!/bin/bash
set -euo pipefail

PG_CONTAINER=postgres
CH_CONTAINER=clickhouse
DB=iman
USER=postgres

# 🧹 Cleanup
docker compose down -v

# 🚀 Start containers
echo -e "\n🟢 Starting Docker Compose services..."
docker compose up -d --build

# ⏳ Wait for readiness
sleep 10
until docker exec $PG_CONTAINER pg_isready -U $USER; do sleep 2; done
until docker exec zookeeper bash -c 'echo ruok | nc -w 1 localhost 2181'; do sleep 2; done

# Ensure ClickHouse database exists
echo -e "\n🗃 Creating ClickHouse database and schema '$DB'..."
docker exec -i $CH_CONTAINER clickhouse-client --query "CREATE DATABASE IF NOT EXISTS $DB;"
docker exec -i $CH_CONTAINER clickhouse-client --query "USE $DB;"

# 📦 Extract schema and generate DDL
echo -e "\n📦 Extracting PostgreSQL schema and generating ClickHouse DDL..."
DDL=$(docker exec $PG_CONTAINER psql -U $USER -d $DB -Atc "
  SELECT
    'CREATE TABLE IF NOT EXISTS iman.users (' ||
    string_agg(
      column_name || ' ' ||
      CASE data_type
        WHEN 'integer' THEN 'UInt32'
        WHEN 'bigint' THEN 'UInt64'
        WHEN 'numeric' THEN 'Decimal(18,2)'
        WHEN 'text' THEN 'String'
        WHEN 'character varying' THEN 'String'
        WHEN 'timestamp without time zone' THEN 'DateTime64(6)'
        WHEN 'uuid' THEN 'UUID'
        ELSE 'String'
      END,
      ', '
    ) || ') ENGINE = ReplacingMergeTree() ORDER BY ' ||
    COALESCE(
      (SELECT column_name FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'user_id' LIMIT 1),
      (SELECT column_name FROM information_schema.columns WHERE table_name = 'users' LIMIT 1)
    ) || ';'
  FROM information_schema.columns
  WHERE table_name = 'users';
")


# 🛠 Apply DDL
echo -e "\n🛠 Applying generated DDL to ClickHouse..."
echo $DDL
docker exec $CH_CONTAINER clickhouse-client --query "$DDL"

# 🔌 Register PostgreSQL Source Connector
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



# 📝 Insert test rows into Postgres
echo -e "\n📝 Inserting test rows into PostgreSQL..."
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
echo -e "\n📊 Rows in PostgreSQL:"
docker exec -i $PG_CONTAINER psql -U $USER -d $DB -c "SELECT * FROM iman.users"

echo -e "\n📬 Messages in Kafka topic:"
docker exec broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic postgres_cdc.iman.users \
  --from-beginning --timeout-ms 5000

echo -e "\n🏁 Rows in ClickHouse:"
docker exec $CH_CONTAINER clickhouse-client --query "SELECT * FROM iman.users FORMAT Vertical"

# ✅ Final assertion
ACTUAL=$(docker exec $CH_CONTAINER clickhouse-client --query "SELECT user_id, username, account_type FROM iman.users ORDER BY user_id FORMAT TSV" || echo -e "\nERROR")
EXPECTED=$'1\tuser1\tBronze\n2\tuser2\tSilver\n3\tuser3\tGold'

if [[ "$ACTUAL" == "$EXPECTED" ]]; then
  echo -e "\n✅ Integration test passed. Data synced correctly."
else
  echo -e "\n❌ Integration test failed. Data mismatch."
  echo -e "\nExpected:\n$EXPECTED\nGot:\n$ACTUAL"
  exit 1
fi
