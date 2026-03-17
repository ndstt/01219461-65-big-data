from __future__ import annotations

import csv
import json
import math
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import urlretrieve

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path("/home/hadoop/homework")
NOTEBOOK_DIR = PROJECT_ROOT / "spark-graphframe"
DATA_DIR = NOTEBOOK_DIR / "data"
STATION_CSV = DATA_DIR / "station.csv"
TRIP_CSV = DATA_DIR / "trip.csv"

STATION_URL = "https://inferentialthinking.com/data/station.csv"
TRIP_URL = "https://inferentialthinking.com/data/trip.csv"

HDFS_DIR = "/user/hadoop/graph_sf_by"
HDFS_BASE = "hdfs://localhost:9000"

SPARK_HOME = "/home/hadoop/spark"
GRAPHFRAMES_JAR = "/home/hadoop/graphframes-0.8.2-spark3.1-s_2.12.jar"
GRAPHFRAMES_PYTHON_ROOT = "/home/hadoop"
VENV_PYTHON = str(PROJECT_ROOT / ".venv/bin/python")

KAFKA_BIN = Path("/home/hadoop/kafka/bin")
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "trip_data"
RUN_ID_FILE = NOTEBOOK_DIR / "latest_trip_run_id.txt"


def _download(url: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not destination.exists():
        urlretrieve(url, destination)
    return destination


def ensure_local_data() -> dict[str, str]:
    _download(STATION_URL, STATION_CSV)
    _download(TRIP_URL, TRIP_CSV)
    return {
        "station_csv": str(STATION_CSV),
        "trip_csv": str(TRIP_CSV),
    }


def _resolve_hdfs_command() -> list[str]:
    candidates: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()

    def add_candidate(command: list[str]) -> None:
        key = tuple(command)
        if key not in seen:
            seen.add(key)
            candidates.append(command)

    if hdfs_path := shutil.which("hdfs"):
        add_candidate([hdfs_path, "dfs"])
    if hadoop_path := shutil.which("hadoop"):
        add_candidate([hadoop_path, "fs"])

    for root in filter(None, (os.environ.get("HADOOP_HOME"), "/home/hadoop/hadoop")):
        root_path = Path(root)
        hdfs_path = root_path / "bin" / "hdfs"
        if hdfs_path.exists():
            add_candidate([str(hdfs_path), "dfs"])
        hadoop_path = root_path / "bin" / "hadoop"
        if hadoop_path.exists():
            add_candidate([str(hadoop_path), "fs"])

    if candidates:
        return candidates[0]

    raise FileNotFoundError(
        "Unable to find an HDFS CLI. Install Hadoop or set HADOOP_HOME so "
        "`sync_data_to_hdfs()` can use `hdfs dfs` or `hadoop fs`."
    )


def sync_data_to_hdfs() -> dict[str, str]:
    local = ensure_local_data()
    hdfs_command = _resolve_hdfs_command()
    commands = [
        hdfs_command + ["-mkdir", "-p", HDFS_DIR],
        hdfs_command + ["-put", "-f", local["station_csv"], f"{HDFS_DIR}/station.csv"],
        hdfs_command + ["-put", "-f", local["trip_csv"], f"{HDFS_DIR}/trip.csv"],
    ]
    for command in commands:
        subprocess.run(command, check=True)

    return {
        "station_csv": f"{HDFS_BASE}{HDFS_DIR}/station.csv",
        "trip_csv": f"{HDFS_BASE}{HDFS_DIR}/trip.csv",
    }


def ensure_data(sync_hdfs: bool = True) -> dict[str, dict[str, str]]:
    payload = {"local": ensure_local_data()}
    if sync_hdfs:
        payload["hdfs"] = sync_data_to_hdfs()
    return payload


def make_run_id() -> str:
    return datetime.now(timezone.utc).strftime("trip-run-%Y%m%dT%H%M%SZ")


def write_latest_run_id(run_id: str) -> str:
    RUN_ID_FILE.write_text(run_id)
    return run_id


def read_latest_run_id(default: str | None = None) -> str | None:
    if RUN_ID_FILE.exists():
        return RUN_ID_FILE.read_text().strip()
    return default


def create_kafka_topic(topic: str = TOPIC_NAME) -> str:
    command = [
        str(KAFKA_BIN / "kafka-topics.sh"),
        "--zookeeper",
        "localhost:2181",
        "--create",
        "--if-not-exists",
        "--topic",
        topic,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
    ]
    subprocess.run(command, check=True)
    return topic


def load_station_lookup() -> dict[int, dict[str, Any]]:
    ensure_local_data()
    lookup: dict[int, dict[str, Any]] = {}
    with STATION_CSV.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            station_id = int(row["station_id"])
            lookup[station_id] = {
                "id": station_id,
                "name": row["name"],
                "lat": float(row["lat"]),
                "lon": float(row["long"]),
                "landmark": row["landmark"],
            }
    return lookup


def haversine_km_value(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    return radius_km * 2 * math.asin(math.sqrt(a))


def distance_between_stations_km(
    station_lookup: dict[int, dict[str, Any]],
    src_station_id: int,
    dst_station_id: int,
) -> float:
    src = station_lookup[src_station_id]
    dst = station_lookup[dst_station_id]
    return haversine_km_value(src["lat"], src["lon"], dst["lat"], dst["lon"])


def iter_trip_messages(limit: int | None = None, run_id: str | None = None):
    ensure_local_data()
    effective_run_id = run_id or make_run_id()

    with TRIP_CSV.open(newline="", encoding="utf-8") as handle:
        for index, row in enumerate(csv.DictReader(handle)):
            if limit is not None and index >= limit:
                break

            yield {
                "run_id": effective_run_id,
                "trip_id": int(row["Trip ID"]),
                "duration_seconds": int(row["Duration"]),
                "start_date": row["Start Date"],
                "start_station": row["Start Station"],
                "src": int(row["Start Terminal"]),
                "end_date": row["End Date"],
                "end_station": row["End Station"],
                "dst": int(row["End Terminal"]),
                "bike_id": int(row["Bike #"]),
                "subscriber_type": row["Subscriber Type"],
                "zip_code": row["Zip Code"] or None,
            }


def _configure_graphframes_python() -> None:
    if GRAPHFRAMES_PYTHON_ROOT not in sys.path:
        sys.path.insert(0, GRAPHFRAMES_PYTHON_ROOT)


def create_spark_session(
    app_name: str,
    master: str = "local[*]",
    shuffle_partitions: int = 8,
) -> SparkSession:
    os.environ["SPARK_HOME"] = SPARK_HOME
    os.environ["PYSPARK_PYTHON"] = VENV_PYTHON
    os.environ["PYSPARK_DRIVER_PYTHON"] = VENV_PYTHON
    _configure_graphframes_python()

    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.jars", GRAPHFRAMES_JAR)
        .config("spark.pyspark.python", VENV_PYTHON)
        .config("spark.pyspark.driver.python", VENV_PYTHON)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", str(shuffle_partitions))
    )
    return builder.getOrCreate()


def _haversine_km_expr(lat1: str, lon1: str, lat2: str, lon2: str):
    lat1_col = F.radians(F.col(lat1))
    lat2_col = F.radians(F.col(lat2))
    delta_lat = F.radians(F.col(lat2) - F.col(lat1))
    delta_lon = F.radians(F.col(lon2) - F.col(lon1))
    a = (
        F.pow(F.sin(delta_lat / F.lit(2.0)), 2)
        + F.cos(lat1_col) * F.cos(lat2_col) * F.pow(F.sin(delta_lon / F.lit(2.0)), 2)
    )
    return F.lit(6371.0) * F.lit(2.0) * F.asin(F.sqrt(a))


def load_station_df(spark: SparkSession, source: str | None = None) -> DataFrame:
    source_path = source or str(STATION_CSV)
    return (
        spark.read.option("header", True)
        .csv(source_path)
        .select(
            F.col("station_id").cast("long").alias("id"),
            F.col("name"),
            F.col("lat").cast("double").alias("lat"),
            F.col("long").cast("double").alias("lon"),
            F.col("dockcount").cast("int").alias("dock_count"),
            F.col("landmark"),
            F.to_date(F.col("installation"), "M/d/yyyy").alias("installation_date"),
        )
        .dropna(subset=["id", "lat", "lon"])
        .dropDuplicates(["id"])
    )


def load_trip_df(spark: SparkSession, source: str | None = None) -> DataFrame:
    source_path = source or str(TRIP_CSV)
    return (
        spark.read.option("header", True)
        .csv(source_path)
        .select(
            F.col("Trip ID").cast("long").alias("trip_id"),
            F.col("Duration").cast("long").alias("duration_seconds"),
            F.to_timestamp(F.col("Start Date"), "M/d/yyyy H:mm").alias("start_at"),
            F.col("Start Station").alias("start_station"),
            F.col("Start Terminal").cast("long").alias("src"),
            F.to_timestamp(F.col("End Date"), "M/d/yyyy H:mm").alias("end_at"),
            F.col("End Station").alias("end_station"),
            F.col("End Terminal").cast("long").alias("dst"),
            F.col("Bike #").cast("long").alias("bike_id"),
            F.col("Subscriber Type").alias("subscriber_type"),
            F.col("Zip Code").alias("zip_code"),
        )
        .dropna(subset=["trip_id", "src", "dst", "bike_id"])
    )


def build_graph_inputs(
    spark: SparkSession,
    source: str | None = None,
) -> dict[str, DataFrame]:
    stations = load_station_df(spark, source=source)
    trips = load_trip_df(
        spark,
        source=str(TRIP_CSV) if source is None else source.replace("station.csv", "trip.csv"),
    )

    src_station = stations.select(
        F.col("id").alias("src"),
        F.col("name").alias("src_name"),
        F.col("lat").alias("src_lat"),
        F.col("lon").alias("src_lon"),
    )
    dst_station = stations.select(
        F.col("id").alias("dst"),
        F.col("name").alias("dst_name"),
        F.col("lat").alias("dst_lat"),
        F.col("lon").alias("dst_lon"),
    )

    trip_edges = (
        trips.join(src_station, on="src", how="left")
        .join(dst_station, on="dst", how="left")
        .filter(
            F.col("src_name").isNotNull()
            & F.col("dst_name").isNotNull()
            & F.col("src_lat").isNotNull()
            & F.col("dst_lat").isNotNull()
        )
        .withColumn("distance_km", _haversine_km_expr("src_lat", "src_lon", "dst_lat", "dst_lon"))
        .withColumn("distance_m", F.round(F.col("distance_km") * F.lit(1000.0), 2))
        .select(
            "src",
            "dst",
            "trip_id",
            "duration_seconds",
            "start_at",
            "start_station",
            "end_at",
            "end_station",
            "bike_id",
            "subscriber_type",
            "zip_code",
            F.round(F.col("distance_km"), 3).alias("distance_km"),
            "distance_m",
        )
    )

    route_edges = (
        trip_edges.groupBy("src", "dst")
        .agg(
            F.count("*").alias("trip_count"),
            F.first("distance_km", ignorenulls=True).alias("distance_km"),
            F.first("distance_m", ignorenulls=True).alias("distance_m"),
        )
        .orderBy(F.desc("trip_count"), F.desc("distance_km"))
    )

    return {
        "vertices": stations,
        "station_df": stations,
        "trip_df": trips,
        "trip_edges": trip_edges,
        "route_edges": route_edges,
    }


def build_graphframes(
    spark: SparkSession,
    source: str | None = None,
) -> dict[str, Any]:
    _configure_graphframes_python()
    from graphframes import GraphFrame

    payload = build_graph_inputs(spark, source=source)
    payload["trip_graph"] = GraphFrame(payload["vertices"], payload["trip_edges"])
    payload["route_graph"] = GraphFrame(payload["vertices"], payload["route_edges"])
    return payload


def build_realtime_route_graph(
    spark: SparkSession,
    route_counts: dict[tuple[int, int], int],
    station_lookup: dict[int, dict[str, Any]],
):
    _configure_graphframes_python()
    from graphframes import GraphFrame

    vertices = load_station_df(spark)

    rows = []
    for (src, dst), trip_count in route_counts.items():
        if src not in station_lookup or dst not in station_lookup:
            continue
        distance_km = distance_between_stations_km(station_lookup, src, dst)
        rows.append(
            (
                int(src),
                int(dst),
                int(trip_count),
                round(distance_km, 3),
                round(distance_km * 1000.0, 2),
            )
        )

    edge_schema = "src long, dst long, trip_count long, distance_km double, distance_m double"
    edges = spark.createDataFrame(rows, schema=edge_schema) if rows else spark.createDataFrame([], schema=edge_schema)
    return GraphFrame(vertices, edges)


def route_count_rows(
    route_counts: dict[tuple[int, int], int],
    station_lookup: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (src, dst), trip_count in route_counts.items():
        if src not in station_lookup or dst not in station_lookup:
            continue
        distance_km = distance_between_stations_km(station_lookup, src, dst)
        rows.append(
            {
                "src": src,
                "src_name": station_lookup[src]["name"],
                "dst": dst,
                "dst_name": station_lookup[dst]["name"],
                "trip_count": trip_count,
                "distance_km": round(distance_km, 3),
            }
        )
    return rows


def dumps_json(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload).encode("utf-8")
