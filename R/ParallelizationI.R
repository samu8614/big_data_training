# ============================================================
# LIBRERÍAS
# ============================================================
library(DBI)
library(duckdb)
library(arrow)
library(dplyr)
library(bench)
library(ggplot2)
library(lubridate)
library(parallel)
library(future.apply)

# --------------------------------------------------------------
# Parámetros del benchmark (ajusta si tu máquina tiene poca RAM)
# --------------------------------------------------------------
parquet_url  <- "https://data-emf.creaf.cat/public/parquet/stations_data_historical/meteo_stations_2000_2024.parquet"
parquet_file <- "meteo_stations_2000_2024.parquet"

# --------------------------------------------------------------
# SQL (igual que en el ejercicio anterior)
# --------------------------------------------------------------
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

# --------------------------------------------------------------
# FUNCIONES DE CONSULTA (igual que antes)
# --------------------------------------------------------------
duckdb_remote <- function() {
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  dbExecute(con, "INSTALL httpfs; LOAD httpfs;")
  dbGetQuery(con, duckdb_sql, params = list(parquet_url))
}

duckdb_local <- function() {
  download.file(url = parquet_url, destfile = parquet_file,
                mode = "wb", quiet = TRUE)
  con <- dbConnect(duckdb::duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  dbGetQuery(con, duckdb_sql, params = list(parquet_file))
}

arrow_local <- function() {
  download.file(url = parquet_url, destfile = parquet_file,
                mode = "wb", quiet = TRUE)
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

# --------------------------------------------------------------
# VERIFICACIÓN RÁPIDA (opcional)
# --------------------------------------------------------------
con_check <- dbConnect(duckdb::duckdb())
dbExecute(con_check, "INSTALL httpfs; LOAD httpfs;")
provincias_ourense <- dbGetQuery(con_check,
                                 "SELECT DISTINCT station_province FROM read_parquet(?) 
   WHERE station_province ILIKE '%uren%'",
                                 params = list(parquet_url)
)
print(provincias_ourense)
dbDisconnect(con_check, shutdown = TRUE)

# --------------------------------------------------------------
# CARGA DE TRABAJO PARALELA (más ligera)
# --------------------------------------------------------------
set.seed(123)
n_vec   <- 30          # número de vectores (reducido para evitar uso excesivo de RAM)
len_vec <- 5e5         # longitud de cada vector (0.5  millones → ~4  MB por vector)
work_data <- lapply(seq_len(n_vec), function(i) rnorm(len_vec))
work_fn   <- function(x) mean(x)

# ---- FUNCIÓN SECUENCIAL ------------------------------------
run_seq <- function() {
  lapply(work_data, work_fn)
}

# ---- FUNCIÓN PARALELA con parallel::parLapply ---------------
run_par <- function() {
  cl <- makeCluster(detectCores() - 1)   # deja un núcleo libre
  on.exit(stopCluster(cl), add = TRUE)
  parLapply(cl, work_data, work_fn)
}

# ---- FUNCIÓN PARALELA con future.apply ----------------------
run_future <- function() {
  plan(multisession, workers = availableCores() - 1)
  on.exit(plan(sequential), add = TRUE)
  future_lapply(work_data, work_fn)
}

# ============================================================
# BENCHMARK (iterations = 2 → suficiente para ver la tendencia)
# ============================================================
bm <- bench::mark(
  duckdb_remote = duckdb_remote(),
  duckdb_local  = duckdb_local(),
  arrow_local   = arrow_local(),
  sequential    = run_seq(),
  parallel_mc   = run_par(),
  future_lapply = run_future(),
  iterations = 2,
  check = FALSE
)

# ============================================================
# RESULTADOS NUMÉRICOS
# ============================================================
print(bm[, c("expression", "min", "median", "mem_alloc", "n_itr")])

# --------------------------------------------------------------
# GUARDAR GRÁFICO EN outputs/
# --------------------------------------------------------------
if (!dir.exists("outputs")) dir.create("outputs")
ggsave(
  filename = file.path("outputs", "parallelizationI.png"),
  plot = last_plot(),
  width = 8, height = 5, dpi = 150
)

# ============================================================
# RESPUESTAS A LAS PREGUNTAS DEL EJERCICIO (Parallelization I)
# ============================================================
# Pregunta 1: ¿Cuál método fue el más rápido y por qué?
# Respuesta: <escribir aquí tu respuesta basada en el output de bm>
#
# Pregunta 2: ¿Qué observaste respecto al overhead de crear el cluster
# o de iniciar los workers de future.apply frente al costo del cálculo?
# Respuesta: <escribir aquí tu respuesta>
#
# Pregunta 3: Si aumentaras el tamaño de cada vector (len_vec), ¿cómo
# esperas que cambie la relación entre los métodos? Justifica.
# Respuesta: <escribir aquí tu respuesta>
# ============================================================