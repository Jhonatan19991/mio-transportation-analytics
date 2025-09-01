# tests/test_gps_pipeline.py
import json
import pytest
from pyspark.sql import Row
from datetime import datetime, timezone
from unittest.mock import patch

# Importa tus funciones refactorizadas
from utilities.utils import (
    parse_gps_raw,
    flatten_gps,
    enrich_gps,
    route_segment_speed
)

# ---------- Helpers ----------
def row_to_dict(r):
    return r.asDict()

# ---------- Tests ----------
def test_parse_gps_raw_array_input(spark):
    raw = spark.createDataFrame([
        {"gps_data": [{"element": {"BUSNUMBER": "1", "GPSX": "120000000", "GPSY": "50000000", "DELAY": "15"}}], "timestamp": 1693526400.0}
    ])
    out = parse_gps_raw(raw)  
    assert out.count() == 1

def test_parse_gps_raw_string_json_input(spark):
    payload = json.dumps([{"element": {"BUSNUMBER": "1", "GPSX": "120000000", "GPSY": "50000000", "DELAY": "5"}}])
    raw = spark.createDataFrame([{"gps_data": payload, "timestamp": 1693526400.0}])
    out = parse_gps_raw(raw)
    row = out.collect()[0]
    assert row["gps_data_parsed"] is not None


def test_flatten_gps_scaling_and_timestamp(spark):
    gps_element = {"element": {"BUSNUMBER": "1", "GPSX": "120000000", "GPSY": "50000000", "DELAY": "7"}}
    bronze = spark.createDataFrame([{"gps_data_parsed": [gps_element], "timestamp": 1693526400.0}])
    silver = flatten_gps(bronze)
    r = silver.collect()[0]
    assert abs(r["GPSX"] - 12.0) < 1e-6
    assert abs(r["GPSY"] - 5.0) < 1e-6
    assert r["DELAY"] == 7
    assert r["date"] is not None

def test_enrich_haversine_and_velocity(spark):
    rows = [
        {"BUSNUMBER":"1","RUTA":"A","TRIPID":"t1","date":"2025-09-01","ORIENTATION":1,"row_number":1,"GPSY":4.0,"GPSX":-74.0,"timestamp":datetime(2025,9,1,8,0,0, tzinfo=timezone.utc)},
        {"BUSNUMBER":"1","RUTA":"A","TRIPID":"t1","date":"2025-09-01","ORIENTATION":1,"row_number":2,"GPSY":4.0001,"GPSX":-73.9999,"timestamp":datetime(2025,9,1,8,0,10, tzinfo=timezone.utc)}
    ]
    enriched_in = spark.createDataFrame(rows)
    stops = spark.createDataFrame([{"STOPID":"s","LINE":"A","ORIENTATION":1,"row_number":1}])
    enriched = enrich_gps(enriched_in, stops)
    r = enriched.orderBy("timestamp").collect()[1]
    assert r["haversine_dist"] is None or r["haversine_dist"] >= 0  

    assert "VELOCITY" in enriched.columns

def test_trip_detection_and_final_filters(spark):
    news = spark.createDataFrame([{"BUSNUMBER":"1","RUTA":"A","TRIPID":"t1","date":"2025-09-01","ORIENTATION":1,"timestamp":datetime(2025,9,1,7,0,0, tzinfo=timezone.utc),"row_number":1}])
    olds = spark.createDataFrame([{"BUSNUMBER":"1","RUTA":"A","TRIPID":"t1","date":"2025-09-01","ORIENTATION":1,"timestamp":datetime(2025,9,1,8,0,0, tzinfo=timezone.utc),"row_number":10,"AVGVELOCITY":10,"AVGDELAY":2}])

    candidates = trips_candidates_from_news_olds(news, olds)  
    assert "TRIPDURATION" in candidates.columns

def test_route_segment_speed_aggregation(spark):
    rows = [
        {"BUSNUMBER":"1","TRIPID":"t1","date":"2025-09-01","RUTA":"A","ORIENTATION":1,"row_number":1,"next_row":2,"VELOCITY":2.5,"haversine_dist":100.0},
        {"BUSNUMBER":"1","TRIPID":"t1","date":"2025-09-01","RUTA":"A","ORIENTATION":1,"row_number":2,"next_row":3,"VELOCITY":3.5,"haversine_dist":150.0},
    ]
    enriched = spark.createDataFrame(rows)
    agg = route_segment_speed(enriched)
    assert agg.count() >= 1
    first = agg.collect()[0]
    assert "AVG_SPEED_M_S" in agg.columns
