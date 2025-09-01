# Notebook: dlt_pipeline_gps.py
import dlt
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import requests
import pandas as pd

# Constantes
TZ = "America/Bogota"
GPS_SCALE = 1e7
EARTH_METERS = 6371000.0

# -----------------------
# BRONZE: ingesta raw
# -----------------------
# ---------- BRONZE corregido ----------
@dlt.table(name="bronze_gps_raw", comment="Raw ingestion from workspace.default.gps_delta with normalized gps_data_parsed.")
def bronze_gps_raw():
    raw = spark.table("workspace.default.gps_delta")  # usa la tabla exacta que tienes

    # esquema esperado dentro del array (si viene como string JSON habrá que parsearlo)
    inner = T.StructType([
        T.StructField("BUSNUMBER", T.StringType(), True),
        T.StructField("DELAY", T.StringType(), True),
        T.StructField("GPSX", T.StringType(), True),
        T.StructField("GPSY", T.StringType(), True),
        T.StructField("PARADA", T.StringType(), True),
        T.StructField("RUTA", T.StringType(), True),
        T.StructField("RUTERO", T.StringType(), True),
        T.StructField("STOPID", T.StringType(), True),
        T.StructField("TASKID", T.StringType(), True),
        T.StructField("TELEGRAMDATE", T.StringType(), True),
        T.StructField("TRIPID", T.StringType(), True)
    ])
    expected_array_schema = T.ArrayType(T.StructType([T.StructField("element", inner, True)]), containsNull=True)

    # Detectar tipo real de gps_data en la tabla
    gps_field = next((f for f in raw.schema.fields if f.name == "gps_data"), None)
    if gps_field is None:
        # no existe la columna gps_data (muy raro) -> devolver raw con columna vacía
        df = raw.withColumn("gps_data_parsed", F.array())
    else:
        dt = gps_field.dataType
        # Si es string => parsear JSON; si es array => reuse; si es otra cosa => intentar coerción segura
        if isinstance(dt, T.StringType) or type(dt).__name__ == "StringType":
            df = raw.withColumn("gps_data_parsed", F.from_json("gps_data", expected_array_schema))
        elif isinstance(dt, T.ArrayType):
            # ya viene como array (tu caso): aseguramos que la columna exista como 'gps_data_parsed'
            df = raw.withColumn("gps_data_parsed", F.col("gps_data"))
        else:
            # fallback: intentar parsear por si viene como string-like
            df = raw.withColumn("gps_data_parsed", F.when(F.col("gps_data").isNotNull(), F.col("gps_data")).otherwise(F.array()))

    # mantener ingest_time para fallback de timestamps
    df = df.withColumn("ingest_time", F.current_timestamp())

    # Expect básico sobre la columna normalizada (no usamos expect_or_drop aquí)
    dlt.expect("bronze_has_gps_data_parsed", F.col("gps_data_parsed").isNotNull())

    return df


@dlt.table(name="bronze_paradas_raw", comment="Raw paradas. Se lee JSON público y se crea DataFrame.")
def bronze_paradas_raw():
    # URL pública con paradas (raw JSON). Cambia si lo necesitas.
    url_paradas = "https://raw.githubusercontent.com/Jhonatan19991/images/refs/heads/main/paradas.json"
    resp = requests.get(url_paradas)
    resp.raise_for_status()
    paradas_data = resp.json()

    # crear DataFrame spark directamente desde la lista/dict del JSON
    # si paradas_data es un dict principal con key 'paradas' adaptaremos:
    if isinstance(paradas_data, dict) and "paradas" in paradas_data and not isinstance(paradas_data["paradas"], list):
        # si la estructura es inesperada, envolverla
        paradas_data = [paradas_data]

    # convertir a pandas por compatibilidad y luego spark
    dt = pd.DataFrame(paradas_data)
    sp_df = spark.createDataFrame(dt)
    return sp_df


# -----------------------
# SILVER: limpieza y normalización
# -----------------------
# ---------- SILVER corregido ----------
@dlt.table(name="silver_gps_flat", comment="GPS flatten + types cast + timestamp normalizado.")
def silver_gps_flat():
    src = dlt.read("bronze_gps_raw")

    # Aplico la expectativa sobre la fuente (antes de eliminar gps_data_parsed)
    dlt.expect("valid_gps_array", F.col("gps_data_parsed").isNotNull() & (F.size(F.col("gps_data_parsed")) > 0))

    # Determinar columna timestamp (tu table tiene 'timestamp' double, si no, usar ingest_time)
    timestamp_col = "timestamp" if "timestamp" in [f.name for f in src.schema.fields] else "ingest_time"

    df = (
        src
        .select(F.col("gps_data_parsed"), F.col(timestamp_col).alias("raw_timestamp"))
        .withColumn("item", F.explode("gps_data_parsed"))
        .select(
            F.col("item.element.BUSNUMBER").alias("BUSNUMBER"),
            F.col("item.element.DELAY").cast("int").alias("DELAY"),
            F.col("item.element.GPSX").alias("GPSX_raw"),
            F.col("item.element.GPSY").alias("GPSY_raw"),
            F.col("item.element.PARADA").alias("PARADA"),
            F.col("item.element.RUTA").alias("RUTA"),
            F.col("item.element.RUTERO").alias("RUTERO"),
            F.col("item.element.STOPID").alias("STOPID"),
            F.col("item.element.TASKID").alias("TASKID"),
            F.col("item.element.TELEGRAMDATE").alias("TELEGRAMDATE"),
            F.col("item.element.TRIPID").alias("TRIPID"),
            # convertir epoch double->timestamp (tu 'timestamp' es double en la tabla)
            F.to_timestamp(F.from_unixtime(F.col("raw_timestamp").cast("long"))).alias("timestamp_utc")
        )
        .withColumn("timestamp", F.from_utc_timestamp("timestamp_utc", TZ))
        .drop("timestamp_utc")
        .withColumn("GPSX", F.when(F.col("GPSX_raw").isNotNull(), F.col("GPSX_raw").cast("double") / F.lit(GPS_SCALE)).otherwise(None))
        .withColumn("GPSY", F.when(F.col("GPSY_raw").isNotNull(), F.col("GPSY_raw").cast("double") / F.lit(GPS_SCALE)).otherwise(None))
        .drop("GPSX_raw", "GPSY_raw")
        .withColumn("date", F.to_date("timestamp"))
    )

    dlt.expect("gps_has_timestamp", F.col("timestamp").isNotNull())

    return df






@dlt.table(name="silver_paradas_exploded", comment="Paradas normalizadas con columnas útiles.")
def silver_paradas_exploded():
    src = dlt.read("bronze_paradas_raw")

    # Validar que la columna 'paradas' exista
    if "paradas" not in [f.name for f in src.schema.fields]:
        # si la estructura es distinta (ej: root array), devolver la tabla tal cual
        return src

    paradas_field = next((f for f in src.schema.fields if f.name == "paradas"), None)
    elem_type = None
    if isinstance(paradas_field.dataType, T.ArrayType):
        elem_type = paradas_field.dataType.elementType

    if isinstance(elem_type, T.StructType):
        # Caso ideal: paradas es Array[Struct] -> expandir con element.*
        df = src.withColumn("element", F.explode(F.col("paradas"))).select("element.*", "ruta")
        return df
    else:
        # Fallback: explotar y tomar keys esperados desde mapa o struct irregular
        exploded = src.withColumn("element", F.explode(F.col("paradas")))
        expected_cols = [
            "DECIMALLATITUDE", "DECIMALLONGITUDE", "DESCRIPTION", "LINE", "LINEID", "LINEVA",
            "LONGNAME", "ORIENTATION", "RNK", "ROWNUM", "SENTIDO", "SHORTNAME", "STOPCHECK", "STOPID"
        ]
        selected = []
        for c in expected_cols:
            # element puede ser MapType o StructType disfrazado; usamos getItem para seguridad
            selected.append(F.col("element").getItem(c).alias(c))
        selected.append("ruta")
        df = exploded.select(*selected)
        return df


@dlt.table(name="silver_stops_routes", comment="Stops con row_number por linea+orientacion y rutas agregadas (FIRST/LAST).")
def silver_stops_routes():
    src = dlt.read("silver_paradas_exploded")
    df = src.select("STOPID", "LINE", F.col("ORIENTATION").cast("int").alias("ORIENTATION"), F.col("ROWNUM").cast("int").alias("ROWNUM"))
    window_ln = Window.partitionBy("LINE", "ORIENTATION").orderBy("ROWNUM")
    df = df.withColumn("row_number", F.row_number().over(window_ln))
    return df


@dlt.table(name="silver_routes_meta", comment="Metadatos de ruta: FIRSTPARADA y LASTPARADA para cada LINE+ORIENTATION.")
def silver_routes_meta():
    stops = dlt.read("silver_stops_routes")
    routes = (
        stops.groupby("LINE", "ORIENTATION")
             .agg(F.collect_list("row_number").alias("paradas"))
             .withColumn("FIRSTPARADA", F.element_at("paradas", 1).cast("int"))
             .withColumn("LASTPARADA", F.element_at("paradas", -1).cast("int"))
    )
    return routes


# -----------------------
# SILVER -> JOIN y cálculos espaciales/temporales
# -----------------------

@dlt.table(name="silver_gps_enriched", comment="GPS unido con stops + cálculos de distancia (haversine) y velocidades.")
@dlt.expect_or_drop("gpsy_positive", "GPSY IS NOT NULL AND GPSY > 1")
def silver_gps_enriched():
    gps = dlt.read("silver_gps_flat")
    stops = dlt.read("silver_stops_routes")

    # Renombrar STOPID de stops para evitar duplicados al hacer el join
    stops_sel = stops.select(
        F.col("STOPID").alias("STOPID_stop"),  # evita colisión con gps.STOPID
        F.col("LINE"),
        F.col("ORIENTATION"),
        F.col("row_number")  # mantenemos el nombre porque lo usamos más adelante
    )

    # Join evitando colisiones de nombres
    df = gps.join(
        stops_sel,
        (gps.STOPID == stops_sel.STOPID_stop) & (gps.RUTA == stops_sel.LINE),
        how="left"
    )

    # Ahora la columna ORIENTATION viene de stops (si no hay match será null)
    # Ventana por viaje (usa ORIENTATION que ahora existe en el dataframe)
    w = Window.partitionBy("BUSNUMBER", "RUTA", "TRIPID", "date", "ORIENTATION").orderBy("timestamp")

    df = df.withColumn("prev_rownum", F.lag("row_number").over(w)) \
           .withColumn("prev_time", F.lag("timestamp").over(w)) \
           .withColumn("next_time", F.lead("timestamp").over(w)) \
           .withColumn("AVGDELAY", F.round(F.avg("DELAY").over(w))) \
           .withColumn("LASTLATITUDE", F.lag("GPSY").over(w)) \
           .withColumn("LASTLONGITUDE", F.lag("GPSX").over(w))

    # haversine (vectorizado)
    df = df.withColumn("dlat", F.radians(F.col("GPSY") - F.col("LASTLATITUDE"))) \
           .withColumn("dlon", F.radians(F.col("GPSX") - F.col("LASTLONGITUDE")))

    df = df.withColumn(
        "haversine_dist",
        F.when(
            F.col("LASTLATITUDE").isNotNull() & F.col("LASTLONGITUDE").isNotNull(),
            2 * F.lit(EARTH_METERS) *
            F.asin(
                F.sqrt(
                    F.sin(F.col("dlat") / 2) ** 2 +
                    F.cos(F.radians(F.col("LASTLATITUDE"))) *
                    F.cos(F.radians(F.col("GPSY"))) *
                    (F.sin(F.col("dlon") / 2) ** 2)
                )
            )
        ).otherwise(None)
    ).drop("dlat", "dlon")

    # tiempos y velocidad protegida
    df = df.withColumn("time_s", (F.unix_timestamp("next_time") - F.unix_timestamp("prev_time")))
    df = df.withColumn("VELOCITY", F.when(F.col("time_s") > 0, F.round(F.col("haversine_dist") / F.col("time_s"), 2)).otherwise(None))
    df = df.withColumn("AVGVELOCITY", F.round(F.avg("VELOCITY").over(w), 2))

    return df


# -----------------------
# GOLD: detección de trips y tabla final
# -----------------------
@dlt.table(name="gold_trips_candidates", comment="Tabla intermedia con inicio/fin candidato de cada TRIPID.")
def gold_trips_candidates():
    enriched = dlt.read("silver_gps_enriched")

    df_new_trip = enriched.filter(F.col("prev_time").isNull()).withColumn("LINE", F.col("RUTA"))
    df_final_trip = enriched.filter(F.col("next_time").isNull()).withColumn("LINE", F.col("RUTA"))

    join_cond = [
        df_new_trip.BUSNUMBER == df_final_trip.BUSNUMBER,
        df_new_trip.LINE == df_final_trip.LINE,
        df_new_trip.TRIPID == df_final_trip.TRIPID,
        df_new_trip.date == df_final_trip.date,
        df_new_trip.ORIENTATION == df_final_trip.ORIENTATION
    ]

    candidates = (
        df_new_trip.alias("news")
        .join(df_final_trip.alias("olds"), on=join_cond, how="left")
        .filter(F.col("olds.prev_time").isNotNull() & F.col("news.next_time").isNotNull())
    )

    candidates = candidates.withColumn(
        "TRIPDURATION",
        F.round((F.unix_timestamp(F.col("olds.timestamp")) - F.unix_timestamp(F.col("news.timestamp"))) / 60)
    )

    candidates = candidates.filter(
        (F.col("TRIPDURATION") > 10) &
        (F.col("TRIPDURATION") < 180) &
        (F.col("olds.AVGVELOCITY") > 0.2) &
        (F.col("olds.AVGVELOCITY") < 100)
    ).select(
        F.col("news.BUSNUMBER").alias("BUSNUMBER"),
        F.col("news.LINE").alias("LINE"),
        F.col("news.timestamp").alias("STARTTIME"),
        F.col("olds.timestamp").alias("ENDTIME"),
        F.col("news.date").alias("date"),
        F.col("TRIPDURATION"),
        F.col("olds.AVGDELAY"),
        F.col("olds.AVGVELOCITY"),
        F.col("olds.ORIENTATION"),
        F.col("news.row_number").alias("STARTSTOP"),
        F.col("olds.row_number").alias("ENDSTOP")
    )

    return candidates


@dlt.table(name="gold_trips_final", comment="Tabla GOLD final con trips filtrados por FIRST/LAST parada y features listos.")
@dlt.expect_or_drop("valid_trip_duration", "TRIPDURATION IS NOT NULL AND TRIPDURATION > 0")
def gold_trips_final():
    candidates = dlt.read("gold_trips_candidates")
    routes = dlt.read("silver_routes_meta")

    final_candidates = (
        candidates
        .join(routes, on=["LINE", "ORIENTATION"], how="left")
        .filter(
            (F.col("STARTSTOP").isin(F.col("FIRSTPARADA"), F.col("FIRSTPARADA") + 1, F.col("FIRSTPARADA") + 2)) &
            (F.col("ENDSTOP").isin(F.col("LASTPARADA"), F.col("LASTPARADA") - 1, F.col("LASTPARADA") - 2))
        )
    )

    FINAL = final_candidates.withColumn(
        "TIMEOFDAY",
        F.when(F.hour("STARTTIME").between(4, 11), "morning")
         .when(F.hour("STARTTIME").between(12, 17), "afternoon")
         .when(F.hour("STARTTIME").between(18, 23), "evening")
         .otherwise("night")
    ).withColumn(
        "RUSHHOUR",
        F.when((F.hour("STARTTIME").between(6, 9)) | (F.hour("STARTTIME").between(16, 19)), F.lit(True)).otherwise(F.lit(False))
    ).select(
        "LINE",
        F.col("date").alias("DATE"),
        "TIMEOFDAY",
        "RUSHHOUR",
        "ORIENTATION",
        F.col("TRIPDURATION").cast("int"),
        F.col("AVGDELAY").cast("int"),
        "AVGVELOCITY"
    )

    dlt.expect("avgvelocity_reasonable", "AVGVELOCITY IS NULL OR (AVGVELOCITY > 0.01 AND AVGVELOCITY < 100)")
    return FINAL


@dlt.table(name="gold_route_segment_speed", comment="Velocidad media por segmento (LINE, ORIENTATION, START_STOP -> END_STOP)")
def gold_route_segment_speed():
    enriched = dlt.read("silver_gps_enriched")

    # ventana por viaje para obtener next row_number en la secuencia
    w = Window.partitionBy("BUSNUMBER", "TRIPID", "date", "RUTA", "ORIENTATION").orderBy("timestamp")
    seg = (enriched
           .withColumn("next_row", F.lead("row_number").over(w))
           .withColumn("segment_start", F.col("row_number"))
           .withColumn("segment_end", F.col("next_row"))
           .filter(F.col("segment_start").isNotNull() & F.col("segment_end").isNotNull() & (F.col("segment_end") == F.col("segment_start") + 1))
           # cada fila corresponde al punto de inicio del segmento; agregamos velocidades/distancias observadas
          )

    agg = (seg.groupBy("RUTA", "ORIENTATION", "segment_start", "segment_end", "date")
           .agg(
               F.count("*").alias("OBS"),
               F.round(F.avg("VELOCITY"), 2).alias("AVG_SPEED_M_S"),
               F.round(F.avg("haversine_dist"), 1).alias("AVG_DIST_M"),
               F.round(F.expr("percentile_approx(VELOCITY, 0.5)"), 2).alias("MEDIAN_SPEED_M_S")
           )
          ).withColumnRenamed("RUTA", "LINE")

    return agg