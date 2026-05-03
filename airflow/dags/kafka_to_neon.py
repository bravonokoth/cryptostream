import os
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Read requirements for virtualenv task to avoid hardcoded deps
req_path = '/opt/airflow/requirements.txt'
with open(req_path, 'r') as f:
    VENV_REQ = [line.strip() for line in f if line.strip() and not line.startswith('#')]

@dag(
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["crypto", "ingestion", "dbt"],
)
def kafka_to_neon():
    @task.virtualenv(
        task_id="consume_kafka_and_load_neon",
        requirements=VENV_REQ,
        system_site_packages=True
    )
    def consume_and_load():
        import json
        import io
        import os
        import struct
        import requests
        import psycopg2
        from kafka import KafkaConsumer, TopicPartition
        import fastavro

        # Environment variables configured in docker-compose.yml
        KAFKA_BROKER = "kafka:9092"
        SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
        TOPIC = "crypto.prices.raw"
        NEON_URL = os.environ.get("NEON_DATABASE_URL")
        
        if not NEON_URL:
            print("Missing NEON_DATABASE_URL environment variable.")
            return

        print("Connecting to Neon PostgreSQL...")
        conn = psycopg2.connect(NEON_URL)
        cur = conn.cursor()

        # Ensure raw table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.raw_prices (
                event_id UUID PRIMARY KEY,
                ingested_at BIGINT,
                coin_id VARCHAR(100),
                symbol VARCHAR(50),
                name VARCHAR(200),
                current_price_usd DOUBLE PRECISION,
                market_cap_usd DOUBLE PRECISION,
                total_volume_usd DOUBLE PRECISION,
                price_change_24h DOUBLE PRECISION,
                price_change_pct_24h DOUBLE PRECISION,
                market_cap_rank INTEGER,
                ath_usd DOUBLE PRECISION,
                circulating_supply DOUBLE PRECISION,
                source VARCHAR(50)
            )
        """)
        conn.commit()

        print("Connecting to Kafka...")
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            enable_auto_commit=False, # Manual offset commits
            group_id='airflow-neon-ingestor'
        )

        schema_cache = {}
        messages_inserted = 0

        # Poll for all available messages
        while True:
            # Poll with short timeout to drain the queue quickly
            batch = consumer.poll(timeout_ms=2000)
            if not batch:
                print("No more messages to process.")
                break
                
            for tp, messages in batch.items():
                for msg in messages:
                    val = msg.value
                    if not val:
                        continue
                    
                    # Confluent Avro wire format: magic byte (1) + schema ID (4)
                    magic, schema_id = struct.unpack('>bI', val[:5])
                    if schema_id not in schema_cache:
                        print(f"Fetching schema {schema_id} from registry...")
                        r = requests.get(f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}")
                        if r.status_code == 200:
                            schema_str = r.json()['schema']
                            schema_cache[schema_id] = fastavro.parse_schema(json.loads(schema_str))
                        else:
                            print(f"Failed to fetch schema {schema_id}: {r.text}")
                            continue
                    
                    b = io.BytesIO(val[5:])
                    try:
                        record = fastavro.schemaless_reader(b, schema_cache[schema_id])
                        
                        # Insert into Postgres
                        cur.execute("""
                            INSERT INTO public.raw_prices (
                                event_id, ingested_at, coin_id, symbol, name,
                                current_price_usd, market_cap_usd, total_volume_usd,
                                price_change_24h, price_change_pct_24h, market_cap_rank,
                                ath_usd, circulating_supply, source
                            ) VALUES (
                                %(event_id)s, %(ingested_at)s, %(coin_id)s, %(symbol)s, %(name)s,
                                %(current_price_usd)s, %(market_cap_usd)s, %(total_volume_usd)s,
                                %(price_change_24h)s, %(price_change_pct_24h)s, %(market_cap_rank)s,
                                %(ath_usd)s, %(circulating_supply)s, %(source)s
                            ) ON CONFLICT (event_id) DO NOTHING
                        """, record)
                        messages_inserted += 1

                    except Exception as e:
                        print(f"Error processing record: {e}")
                        continue
                        
            # Commit the DB transaction for this batch
            conn.commit()
            # Manually commit kafka offsets
            consumer.commit()
            
        cur.close()
        conn.close()
        consumer.close()
        
        print(f"Successfully processed and inserted {messages_inserted} records into Neon.")

    # Trigger dbt run after load
    dbt_run = BashOperator(
        task_id="run_dbt_models",
        bash_command="dbt run --profiles-dir /opt/airflow/dbt_transform --project-dir /opt/airflow/dbt_transform",
    )

    consume_and_load() >> dbt_run

# Instantiate the DAG
kafka_to_neon_dag = kafka_to_neon()

