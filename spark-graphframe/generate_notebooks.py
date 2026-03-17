from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent

NOTEBOOK_DIR = Path("/home/hadoop/homework/spark-graphframe")


def markdown_cell(source: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": dedent(source).strip() + "\n",
    }


def code_cell(source: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": dedent(source).strip() + "\n",
    }


def notebook(cells: list[dict]) -> dict:
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "homework",
                "language": "python",
                "name": "homework",
            },
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.10.20",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def write_notebook(path: Path, cells: list[dict]) -> None:
    path.write_text(json.dumps(notebook(cells), indent=1))


def build_part1_notebook() -> list[dict]:
    return [
        markdown_cell(
            """
            # Homework GraphFrames Part 1

            Name: `________________`

            This notebook solves Part 1 of the GraphFrames homework using the Bay Area Bike Share `station.csv` and `trip.csv` data.
            """
        ),
        code_cell(
            """
            import sys
            sys.path.insert(0, "/home/hadoop/homework/spark-graphframe")

            from pyspark.sql import functions as F

            from spark_graphframe_homework import create_spark_session, ensure_data, build_graphframes
            """
        ),
        code_cell(
            """
            paths = ensure_data(sync_hdfs=True)
            spark = create_spark_session("graphframes-homework-part1")

            paths
            """
        ),
        code_cell(
            """
            graph_payload = build_graphframes(spark, source=paths["local"]["station_csv"])

            station_df = graph_payload["station_df"]
            trip_df = graph_payload["trip_df"]
            vertices = graph_payload["vertices"]
            trip_edges = graph_payload["trip_edges"]
            route_edges = graph_payload["route_edges"]
            trip_graph = graph_payload["trip_graph"]
            route_graph = graph_payload["route_graph"]

            {
                "stations": station_df.count(),
                "trips": trip_df.count(),
                "trip_edges": trip_edges.count(),
                "route_edges": route_edges.count(),
            }
            """
        ),
        markdown_cell(
            """
            ## Question 1

            Find the indegree and outdegree of all stations.
            """
        ),
        code_cell(
            """
            degree_df = (
                vertices.select("id", "name")
                .join(trip_graph.inDegrees, on="id", how="left")
                .join(trip_graph.outDegrees, on="id", how="left")
                .fillna({"inDegree": 0, "outDegree": 0})
                .orderBy(F.desc("inDegree"), F.desc("outDegree"), F.asc("name"))
            )

            degree_df.show(vertices.count(), truncate=False)
            """
        ),
        markdown_cell(
            """
            ## Question 2

            Find any two stations that have distance greater than 5 km.
            """
        ),
        code_cell(
            """
            long_distance_routes = (
                route_edges
                .filter(F.col("distance_km") > 5)
                .join(vertices.select(F.col("id").alias("src"), F.col("name").alias("src_name")), on="src")
                .join(vertices.select(F.col("id").alias("dst"), F.col("name").alias("dst_name")), on="dst")
                .select("src_name", "dst_name", "trip_count", "distance_km", "distance_m")
                .orderBy(F.desc("distance_km"), F.desc("trip_count"))
            )

            long_distance_routes.show(20, truncate=False)
            """
        ),
        markdown_cell(
            """
            ## Question 3

            Find any two stations `A` and `C` that are connected by one hop `B` and have a total distance greater than `150`.

            The homework statement does not specify a unit for `150`, so this notebook follows the text literally and applies the threshold to meters. If your instructor intended `15 km`, change the filter from `150` to `15000`.
            """
        ),
        code_cell(
            """
            one_hop_paths = (
                route_graph.find("(a)-[ab]->(b); (b)-[bc]->(c)")
                .filter("a.id <> c.id")
                .withColumn("total_distance_m", F.col("ab.distance_m") + F.col("bc.distance_m"))
                .withColumn("total_distance_km", F.round(F.col("total_distance_m") / F.lit(1000.0), 3))
                .filter(F.col("total_distance_m") > 150)
                .select(
                    F.col("a.name").alias("station_a"),
                    F.col("b.name").alias("station_b"),
                    F.col("c.name").alias("station_c"),
                    F.col("ab.trip_count").alias("a_to_b_trips"),
                    F.col("bc.trip_count").alias("b_to_c_trips"),
                    "total_distance_m",
                    "total_distance_km",
                )
                .dropDuplicates(["station_a", "station_b", "station_c"])
                .orderBy(F.desc("total_distance_m"), F.desc("a_to_b_trips"), F.desc("b_to_c_trips"))
            )

            one_hop_paths.show(20, truncate=False)
            """
        ),
        markdown_cell(
            """
            ## Question 4

            Run PageRank to find the importance of all stations.
            """
        ),
        code_cell(
            """
            pagerank_df = (
                route_graph.pageRank(resetProbability=0.15, maxIter=10)
                .vertices
                .select("id", "pagerank")
                .join(vertices.select("id", "name"), on="id", how="left")
                .orderBy(F.desc("pagerank"), F.asc("name"))
            )

            pagerank_df.show(20, truncate=False)
            """
        ),
        code_cell(
            """
            spark.stop()
            """
        ),
    ]


def build_producer_notebook() -> list[dict]:
    return [
        markdown_cell(
            """
            # Homework GraphFrames Part 2 Producer

            Name: `________________`

            This notebook creates the Kafka topic `trip_data` and streams bike trip rows into it line by line.
            """
        ),
        code_cell(
            """
            import json
            import sys
            import time

            sys.path.insert(0, "/home/hadoop/homework/spark-graphframe")

            from kafka import KafkaProducer

            from spark_graphframe_homework import (
                BOOTSTRAP_SERVERS,
                TOPIC_NAME,
                create_kafka_topic,
                ensure_data,
                iter_trip_messages,
                make_run_id,
                write_latest_run_id,
            )
            """
        ),
        code_cell(
            """
            ensure_data(sync_hdfs=False)
            create_kafka_topic(TOPIC_NAME)

            RUN_ID = write_latest_run_id(make_run_id())
            MESSAGE_LIMIT = 250  # Set to None to stream the full CSV.
            SLEEP_SECONDS = 0.01

            preview_messages = list(iter_trip_messages(limit=3, run_id=RUN_ID))
            {"run_id": RUN_ID, "preview_messages": preview_messages}
            """
        ),
        code_cell(
            """
            producer = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
                key_serializer=lambda key: str(key).encode("utf-8"),
                acks="all",
            )


            def stream_trip_rows(limit=MESSAGE_LIMIT, sleep_seconds=SLEEP_SECONDS, run_id=RUN_ID):
                sent = 0
                started = time.time()
                last_offset = None

                for message in iter_trip_messages(limit=limit, run_id=run_id):
                    future = producer.send(TOPIC_NAME, key=message["trip_id"], value=message)
                    metadata = future.get(timeout=30)
                    last_offset = metadata.offset
                    sent += 1
                    if sleep_seconds:
                        time.sleep(sleep_seconds)

                producer.flush()
                elapsed = round(time.time() - started, 2)
                return {
                    "run_id": run_id,
                    "sent_messages": sent,
                    "elapsed_seconds": elapsed,
                    "last_offset": last_offset,
                }


            producer_result = stream_trip_rows()
            producer.close()
            producer_result
            """
        ),
    ]


def build_consumer_notebook() -> list[dict]:
    return [
        markdown_cell(
            """
            # Homework GraphFrames Part 2 Consumer

            Name: `________________`

            This notebook consumes from Kafka topic `trip_data`, updates the route graph in real time, computes the number of trips between each station, and alerts when a bike travels more than 15 km in total.
            """
        ),
        code_cell(
            """
            import json
            import sys
            import time
            from collections import defaultdict

            sys.path.insert(0, "/home/hadoop/homework/spark-graphframe")

            from kafka import KafkaConsumer, TopicPartition
            from pyspark.sql import functions as F

            from spark_graphframe_homework import (
                BOOTSTRAP_SERVERS,
                TOPIC_NAME,
                build_realtime_route_graph,
                create_kafka_topic,
                create_spark_session,
                distance_between_stations_km,
                load_station_lookup,
                read_latest_run_id,
                route_count_rows,
            )
            """
        ),
        code_cell(
            """
            create_kafka_topic(TOPIC_NAME)

            RUN_ID = read_latest_run_id()
            MAX_MESSAGES = 250
            spark = create_spark_session("graphframes-homework-consumer")
            station_lookup = load_station_lookup()

            {"run_id": RUN_ID, "max_messages": MAX_MESSAGES, "known_stations": len(station_lookup)}
            """
        ),
        code_cell(
            """
            def consume_trip_stream(run_id=RUN_ID, max_messages=MAX_MESSAGES, consumer_timeout_ms=5000):
                consumer = KafkaConsumer(
                    bootstrap_servers=[BOOTSTRAP_SERVERS],
                    enable_auto_commit=False,
                    consumer_timeout_ms=consumer_timeout_ms,
                    value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
                )

                topic_partitions = [TopicPartition(TOPIC_NAME, 0)]
                consumer.assign(topic_partitions)
                consumer.seek_to_beginning(*topic_partitions)

                route_counts = defaultdict(int)
                bike_distance_km = defaultdict(float)
                alerted_bikes = set()
                alerts = []
                processed = 0

                try:
                    for record in consumer:
                        message = record.value
                        if run_id and message.get("run_id") != run_id:
                            continue

                        src = int(message["src"])
                        dst = int(message["dst"])
                        bike_id = int(message["bike_id"])

                        if src not in station_lookup or dst not in station_lookup:
                            continue

                        distance_km = distance_between_stations_km(station_lookup, src, dst)
                        route_counts[(src, dst)] += 1
                        bike_distance_km[bike_id] += distance_km

                        if bike_distance_km[bike_id] > 15 and bike_id not in alerted_bikes:
                            alerted_bikes.add(bike_id)
                            alerts.append(
                                {
                                    "bike_id": bike_id,
                                    "total_distance_km": round(bike_distance_km[bike_id], 3),
                                    "src_name": station_lookup[src]["name"],
                                    "dst_name": station_lookup[dst]["name"],
                                }
                            )

                        processed += 1
                        if processed >= max_messages:
                            break
                finally:
                    consumer.close()

                route_graph = build_realtime_route_graph(spark, dict(route_counts), station_lookup)
                route_rows = route_count_rows(dict(route_counts), station_lookup)

                route_schema = "src long, src_name string, dst long, dst_name string, trip_count long, distance_km double"
                alert_schema = "bike_id long, total_distance_km double, src_name string, dst_name string"

                route_counts_df = (
                    spark.createDataFrame(route_rows)
                    if route_rows
                    else spark.createDataFrame([], schema=route_schema)
                )
                alerts_df = (
                    spark.createDataFrame(alerts)
                    if alerts
                    else spark.createDataFrame([], schema=alert_schema)
                )

                return {
                    "processed_messages": processed,
                    "route_graph": route_graph,
                    "route_counts_df": route_counts_df,
                    "alerts_df": alerts_df,
                }
            """
        ),
        code_cell(
            """
            consumer_result = consume_trip_stream()
            consumer_result["processed_messages"]
            """
        ),
        code_cell(
            """
            if consumer_result["processed_messages"] == 0:
                print("No matching Kafka messages were consumed. Run producer.ipynb first.")
            else:
                trip_summary_df = (
                    consumer_result["route_graph"]
                    .triplets
                    .select(
                        F.col("edge.trip_count").alias("trip_count"),
                        F.col("src.name").alias("from_station"),
                        F.col("dst.name").alias("to_station"),
                        F.col("edge.distance_km").alias("distance_km"),
                    )
                    .orderBy(F.desc("trip_count"), F.desc("distance_km"))
                )
                trip_summary_df.show(10, truncate=False)
            """
        ),
        code_cell(
            """
            consumer_result["alerts_df"].orderBy(F.desc("total_distance_km"), F.asc("bike_id")).show(20, truncate=False)
            """
        ),
        code_cell(
            """
            spark.stop()
            """
        ),
    ]


def main() -> None:
    write_notebook(NOTEBOOK_DIR / "part1_graphframes.ipynb", build_part1_notebook())
    write_notebook(NOTEBOOK_DIR / "producer.ipynb", build_producer_notebook())
    write_notebook(NOTEBOOK_DIR / "consumer.ipynb", build_consumer_notebook())


if __name__ == "__main__":
    main()
