# ============================================================
library(DBI)
library(duckdb)
library(arrow)
library(dplyr)
library(bench)
library(ggplot2)
library(lubridate)
library(sf)

parquet_url  <- "https://data-emf.creaf.cat/public/parquet/stations_data_historical/meteo_stations_2000_2024.parquet"
parquet_file <- "meteo_stations_2000_2024.parquet"

# ============================================================
# SQL CORREGIDO con los nombres reales del esquema
# ============================================================
# Nota: 'dates' es VARCHAR → usamos EXTRACT(YEAR FROM dates::DATE)
# o simplemente filtramos year = 2020 que ya viene como columna INTEGER

duckdb_sql <- "
  SELECT
    AVG(MeanTemperature)      AS mean_temperature,
    AVG(MeanRelativeHumidity) AS mean_relative_humidity,
    SUM(Precipitation)        AS total_precipitation
  FROM
    read_parquet(?)
  WHERE
    station_province = 'OURENSE'
    AND year = 2020
"
# Nota: comprueba si el valor es 'OURENSE' o 'Ourense' con:
# SELECT DISTINCT station_province FROM read_parquet(?) WHERE station_province ILIKE '%uren%'

# ============================================================
# FUNCIÓN 1 – DuckDB REMOTO (sin descarga, lee directo la URL)
# ============================================================
duckdb_remote <- function() {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  dbExecute(con, "INSTALL httpfs; LOAD httpfs;")
  dbGetQuery(con, duckdb_sql, params = list(parquet_url))
}

# ============================================================
# FUNCIÓN 2 – DuckDB LOCAL (descarga incluida en el tiempo medido)
# ============================================================
duckdb_local <- function() {
  download.file(url = parquet_url, destfile = parquet_file,
                mode = "wb", quiet = TRUE)   # descarga siempre → tiempo incluido
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  dbGetQuery(con, duckdb_sql, params = list(parquet_file))
}

# ============================================================
# FUNCIÓN 3 – Arrow LOCAL (descarga incluida en el tiempo medido)
# ============================================================
arrow_local <- function() {
  download.file(url = parquet_url, destfile = parquet_file,
                mode = "wb", quiet = TRUE)   # descarga siempre → tiempo incluido
  
  arrow::read_parquet(parquet_file) %>%
    filter(station_province == "OURENSE",
           year == 2020) %>%
    summarise(
      mean_temperature       = mean(MeanTemperature,      na.rm = TRUE),
      mean_relative_humidity = mean(MeanRelativeHumidity, na.rm = TRUE),
      total_precipitation    = sum(Precipitation,         na.rm = TRUE)
    ) %>%
    collect()
}

sf_local <- function() {
  download.file(url = parquet_url, destfile = parquet_file,
                mode = "wb", quiet = TRUE)   # descarga siempre → tiempo incluido
  # sf::st_read lee el parquet; si tiene columna de geometría la devuelve como sf,
  # si no, la devuelve como data.frame – ambas sirven para nuestro summarise.
  sf::st_read(parquet_file, quiet = TRUE) %>%
    filter(station_province == "OURENSE",
           year == 2020) %>%
    summarise(
      mean_temperature       = mean(MeanTemperature,      na.rm = TRUE),
      mean_relative_humidity = mean(MeanRelativeHumidity, na.rm = TRUE),
      total_precipitation    = sum(Precipitation,         na.rm = TRUE)
    ) %>%
    # Convertir a data.frame normal para que el resultado tenga el mismo tipo
    # que las otras funciones (opcional, pero mantiene consistencia)
    as.data.frame()
}



# ============================================================
# VERIFICACIÓN RÁPIDA antes del benchmark (opcional pero recomendable)
# ============================================================
# Comprueba el valor exacto de provincia antes de lanzar el benchmark completo:
con_check <- dbConnect(duckdb::duckdb())
dbExecute(con_check, "INSTALL httpfs; LOAD httpfs;")
provincias_ourense <- dbGetQuery(con_check,
                                 "SELECT DISTINCT station_province 
   FROM read_parquet(?) 
   WHERE station_province ILIKE '%uren%'",
                                 params = list(parquet_url)
)
print(provincias_ourense)   # confirma si es 'OURENSE', 'Ourense', 'ourense'…
dbDisconnect(con_check, shutdown = TRUE)

# Si el valor es distinto a 'OURENSE', actualiza duckdb_sql y la función arrow_local

# ============================================================
# BENCHMARK  (iterations = 3 porque cada iteración descarga 311 MB)
# ============================================================
bm <- bench::mark(
  duckdb_remote = duckdb_remote(),
  duckdb_local  = duckdb_local(),
  arrow_local   = arrow_local(),
  sf_local      = sf_local(),   # <‑‑ NUEVA COMPARACIÓN
  iterations = 3,
  check = FALSE
)

# ============================================================
# RESULTADOS
# ============================================================
print(bm[, c("expression", "min", "median", "mem_alloc", "n_itr")])

# Determinar el método más rápido (tiempo mediano)
fastest_method <- bm$expression[which.min(bm$median)]
# Comentario indicativo
cat("\nMétodo más rápido:", as.character(fastest_method), "\n")

autoplot(bm) +
  labs(
    title    = "Benchmark: DuckDB remoto vs DuckDB local vs Arrow local",
    subtitle = "Temperatura, HR y precipitación – Ourense 2020",
    x        = "Tiempo de ejecución",
    y        = "Método"
  ) +
  theme_minimal(base_size = 13)