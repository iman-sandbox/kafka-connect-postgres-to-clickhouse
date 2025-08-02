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

echo -e "\n⏳ Waiting for all services to be healthy..."
until docker exec zookeeper echo ruok | nc -w1 localhost 2181; do sleep 1; done
until docker exec broker bash -c "nc -z broker 9092"; do sleep 1; done
until docker exec postgres pg_isready -U "$POSTGRES_USER"; do sleep 1; done
until curl -fs http://localhost:8083/connector-plugins >/dev/null; do sleep 1; done
until curl -fs http://localhost:8084/connector-plugins >/dev/null; do sleep 1; done

echo "📦 Extracting PostgreSQL schema and generating ClickHouse DDL..."
docker exec postgres psql -U postgres -d postgres -Atc "
SELECT
  'CREATE TABLE IF NOT EXISTS ' || c.table_name || '_cdc (' ||
  string_agg('after_' || c.column_name || ' ' ||
             CASE
               WHEN c.data_type = 'integer' THEN 'Int32'
               WHEN c.data_type = 'bigint' THEN 'Int64'
               WHEN c.data_type = 'character varying' THEN 'String'
               WHEN c.data_type = 'text' THEN 'String'
               WHEN c.data_type = 'timestamp without time zone' THEN 'UInt64'
               ELSE 'String'
             END, ', ') ||
  ', op String) ENGINE = MergeTree ORDER BY after_' || pk.column_name || ';' ||

  ' CREATE TABLE IF NOT EXISTS ' || c.table_name || ' (' ||
  string_agg(c.column_name || ' ' ||
             CASE
               WHEN c.data_type = 'integer' THEN 'Int32'
               WHEN c.data_type = 'bigint' THEN 'Int64'
               WHEN c.data_type = 'character varying' THEN 'String'
               WHEN c.data_type = 'text' THEN 'String'
               WHEN c.data_type = 'timestamp without time zone' THEN 'DateTime'
               ELSE 'String'
             END, ', ') ||
  CASE WHEN bool_or(c.column_name = 'updated_at') THEN ''
       ELSE ', updated_at DateTime DEFAULT now()'
  END ||
  ') ENGINE = ReplacingMergeTree(updated_at) ORDER BY ' || pk.column_name || ';' ||

  ' CREATE MATERIALIZED VIEW IF NOT EXISTS ' || c.table_name || '_mv TO ' || c.table_name || ' AS SELECT ' ||
  string_agg('after_' || c.column_name || ' AS ' || c.column_name, ', ') ||
  CASE WHEN bool_or(c.column_name = 'updated_at') THEN ''
       ELSE ', now() AS updated_at'
  END ||
  ' FROM ' || c.table_name || '_cdc WHERE op IN (''c'', ''u'', ''r'');'
FROM information_schema.columns c
JOIN (
  SELECT ku.table_name, ku.column_name
  FROM information_schema.key_column_usage ku
  JOIN information_schema.table_constraints tc
    ON ku.constraint_name = tc.constraint_name
   AND ku.table_schema = tc.table_schema
  WHERE tc.constraint_type = 'PRIMARY KEY'
) pk ON pk.table_name = c.table_name
WHERE c.table_schema = 'public'
GROUP BY c.table_name, pk.column_name;" > clickhouse-ddl.sql

echo "🛠 Applying generated DDL to ClickHouse..."
docker exec -i clickhouse clickhouse-client --multiquery < clickhouse-ddl.sql

echo "🧩 Registering Debezium PostgreSQL source connector..."
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
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
      "publication.autocreate.mode": "filtered",
      "schema.include.list": "public",
      "topic.prefix": "postgres_cdc",
      "tombstones.on.delete": "false",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false",
      "transforms": "flatten",
      "transforms.flatten.type": "org.apache.kafka.connect.transforms.Flatten$Value",
      "transforms.flatten.delimiter": "_"
    }
  }'

echo "🧩 Registering ClickHouse Sink connector..."
curl -s -X POST http://localhost:8084/connectors -H "Content-Type:application/json" \
  -d '{
    "name": "clickhouse-sink-connector",
    "config": {
      "clickhouse.url": "http://clickhouse:8123",
      "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
      "tasks.max": "1",
      "topics.regex": "postgres_cdc.public.*",
      "hostname": "clickhouse",
      "port": "8123",
      "database": "default",
      "username": "default",
      "password": "",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "insert.mode": "insert",
      "errors.log.enable": true,
      "errors.log.include.messages": true,
      "errors.retry.timeout": "60000",
      "errors.retry.delay.max.ms": "5000"
    }
  }'

echo "📝 Inserting test rows into PostgreSQL..."
docker exec -i postgres psql -U postgres -d postgres -c "
  INSERT INTO users (username, account_type) VALUES
  ('user1', 'Bronze'),
  ('user2', 'Silver'),
  ('user3', 'Gold');
"

echo "⏳ Waiting for CDC event in Kafka topic..."
for i in {1..10}; do
  OUT=$(docker exec broker kafka-console-consumer \
    --bootstrap-server broker:29092 \
    --topic postgres_cdc.public.users \
    --from-beginning --timeout-ms 2000 --max-messages 1 2>/dev/null)
  if echo "$OUT" | grep -q after_username; then
    echo "$OUT"
    break
  fi
  sleep 2
done

echo -e "\n⏳ Waiting for sink to apply to ClickHouse..."
sleep 10

echo "🔎 Verifying ClickHouse synchronization:"
docker exec clickhouse clickhouse-client --query "SELECT * FROM default.users FORMAT JSON"
