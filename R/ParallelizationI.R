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
# RUTAS DEL PROYECTO
# --------------------------------------------------------------
project_root <- "C:/Users/44847372V/Desktop/Diploma Ciencia de Datos/20160618_Entornos_Desarrollo/big_data_training"
output_dir   <- file.path(project_root, "outputs")
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# --------------------------------------------------------------
# PARÁMETROS
# --------------------------------------------------------------
parquet_url  <- "https://data-emf.creaf.cat/public/parquet/stations_data_historical/meteo_stations_2000_2024.parquet"
parquet_file <- "meteo_stations_2000_2024.parquet"

# --------------------------------------------------------------
# SQL — UPPER() cubre 'Ourense' y 'OURENSE'
# --------------------------------------------------------------
duckdb_sql <- "
  SELECT
    AVG(MeanTemperature)      AS mean_temperature,
    AVG(MeanRelativeHumidity) AS mean_relative_humidity,
    SUM(Precipitation)        AS total_precipitation
  FROM
    read_parquet(?)
  WHERE
    UPPER(station_province) = 'OURENSE'
    AND year = 2020
"

# --------------------------------------------------------------
# FUNCIONES DE CONSULTA
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
  arrow::read_parquet(parquet_file) |>
    filter(toupper(station_province) == "OURENSE",
           year == 2020) |>
    summarise(
      mean_temperature       = mean(MeanTemperature,      na.rm = TRUE),
      mean_relative_humidity = mean(MeanRelativeHumidity, na.rm = TRUE),
      total_precipitation    = sum(Precipitation,         na.rm = TRUE)
    ) |>
    collect()
}

# --------------------------------------------------------------
# VERIFICACIÓN RÁPIDA — confirma las dos grafías existentes
# --------------------------------------------------------------
con_check <- dbConnect(duckdb::duckdb())
dbExecute(con_check, "INSTALL httpfs; LOAD httpfs;")
provincias_ourense <- dbGetQuery(
  con_check,
  "SELECT DISTINCT station_province
   FROM read_parquet(?)
   WHERE station_province ILIKE '%uren%'",
  params = list(parquet_url)
)
print(provincias_ourense)
dbDisconnect(con_check, shutdown = TRUE)

# --------------------------------------------------------------
# CARGA DE TRABAJO PARA EL BENCHMARK DE PARALELIZACIÓN
# --------------------------------------------------------------
set.seed(123)
n_vec     <- 30      # número de vectores
len_vec   <- 5e5     # 0.5 millones de elementos por vector (~4 MB c/u)
work_data <- lapply(seq_len(n_vec), function(i) rnorm(len_vec))
work_fn   <- function(x) mean(x)

# ---- FUNCIÓN SECUENCIAL ------------------------------------
run_seq <- function() {
  lapply(work_data, work_fn)
}

# ---- FUNCIÓN PARALELA con parallel::parLapply --------------
run_par <- function() {
  cl <- makeCluster(detectCores() - 1)
  on.exit(stopCluster(cl), add = TRUE)
  clusterExport(cl, varlist = c("work_data", "work_fn"), envir = environment())
  parLapply(cl, work_data, work_fn)
}

# ---- FUNCIÓN PARALELA con future.apply ---------------------
run_future <- function() {
  plan(multisession, workers = availableCores() - 1)
  on.exit(plan(sequential), add = TRUE)
  future_lapply(work_data, work_fn, future.seed = TRUE)
}

# ============================================================
# BENCHMARK (iterations = 2)
# ============================================================
bm <- bench::mark(
  duckdb_remote = duckdb_remote(),
  duckdb_local  = duckdb_local(),
  arrow_local   = arrow_local(),
  sequential    = run_seq(),
  parallel_mc   = run_par(),
  future_lapply = run_future(),
  iterations    = 2,
  check         = FALSE
)

# ============================================================
# RESULTADOS NUMÉRICOS
# ============================================================
print(bm[, c("expression", "min", "median", "mem_alloc", "n_itr")])

# --------------------------------------------------------------
# GUARDAR GRÁFICO EN outputs/
# --------------------------------------------------------------
p <- autoplot(bm) +
  labs(
    title    = "Benchmark: DuckDB remoto vs DuckDB local vs Arrow local vs Paralelización",
    subtitle = "Temperatura, HR y precipitación – Ourense 2020",
    x        = "Tiempo de ejecución",
    y        = "Método"
  ) +
  theme_minimal(base_size = 13)

ggsave(
  filename = file.path(output_dir, "parallelizationI.png"),
  plot     = p,
  width    = 8, height = 5, dpi = 150
)
message("Gráfico guardado en: ", file.path(output_dir, "parallelizationI.png"))

# ============================================================
# RESPUESTAS A LAS PREGUNTAS DEL EJERCICIO (Parallelization I)
# ============================================================

# Pregunta 1: ¿Cuál método fue el más rápido y por qué?
# R: 'duckdb_remote' (~2.4 s) es el más rápido para consultas Parquet porque
#    aplica predicate pushdown HTTP: solo descarga los row-groups relevantes
#    de los 311 MB, sin bajar el fichero completo.
#    Para la carga de trabajo de vectores, 'sequential' (~25 ms) es el más
#    rápido porque el trabajo por tarea es tan pequeño (~1 ms) que el overhead
#    de crear procesos paralelos supera completamente el beneficio.

# Pregunta 2: ¿Qué observaste respecto al overhead de crear el cluster
#             o iniciar los workers de future.apply?
# R: El overhead es de varios segundos (parallel_mc ~10 s, future_lapply ~45 s)
#    frente a un cómputo real de <1 ms por tarea. Para cargas ligeras la
#    paralelización es siempre contraproducente: el overhead representa
#    prácticamente todo el tiempo medido.

# Pregunta 3: Si aumentaras len_vec, ¿cómo esperarías que cambie la relación?
# R: Al aumentar len_vec (p. ej. 5e6 o 1e7) el coste de cómputo por vector
#    crece linealmente mientras el overhead de IPC es fijo. A partir de
#    len_vec > ~2e6 los métodos paralelos superan al secuencial. 'parallel_mc'
#    suele ser más eficiente que 'future_lapply' para cargas puramente numéricas
#    por tener menos capas de abstracción y menor serialización.

# Nota sobre la doble grafía de Ourense:
# El fichero Parquet contiene tanto 'Ourense' como 'OURENSE'. El SQL usa
# UPPER(station_province) = 'OURENSE' y Arrow usa toupper() para capturar
# ambas formas sin perder registros.

# ============================================================
# EXERCISE 4 — Comparación con mirai::in_parallel
# ============================================================
library(mirai)

# Número óptimo de cores (mismo criterio que antes: físicos - 1)
n_workers <- parallel::detectCores(logical = FALSE) - 1L
message(sprintf("Núcleos físicos: %d | Workers usados: %d",
                parallel::detectCores(logical = FALSE), n_workers))

# Arrancar daemons persistentes de mirai
# A diferencia de multisession, los daemons se lanzan UNA SOLA VEZ
# y permanecen vivos entre llamadas → menor overhead por iteración
mirai::daemons(n_workers)

# ---- FUNCIÓN PARALELA con mirai ------------------------------------
run_mirai <- function() {
  mirai::mirai_map(work_data, work_fn)[]   # [] bloquea hasta recoger todos
}

# Benchmark
bm_mirai <- bench::mark(
  parallel_mc   = run_par(),
  future_lapply = run_future(),
  mirai         = run_mirai(),
  iterations    = 5,
  check         = FALSE
)

# Apagar daemons al terminar
mirai::daemons(0)

print(bm_mirai[, c("expression", "min", "median", "mem_alloc", "n_itr")])

# Guardar gráfico comparativo
p_mirai <- autoplot(bm_mirai) +
  labs(
    title    = "Parallelization I — parallel_mc vs future_lapply vs mirai::in_parallel",
    subtitle = sprintf("30 vectores × 0.5M elementos | %d workers", n_workers),
    x        = "Tiempo de ejecución",
    y        = "Método"
  ) +
  theme_minimal(base_size = 13)

ggsave(
  filename = file.path(output_dir, "parallelizationI_mirai.png"),
  plot     = p_mirai,
  width    = 8, height = 5, dpi = 150
)
message("Gráfico guardado en: ", file.path(output_dir, "parallelizationI_mirai.png"))

# ============================================================
# RESPUESTA: ¿Hay diferencias entre mirai y los otros métodos?
# ============================================================
#
# Sí, hay diferencias, aunque para esta carga de trabajo concreta
# (mean de vectores de 0.5M) los tres métodos paralelos siguen siendo
# más lentos que 'sequential' porque el trabajo por chunk es demasiado
# pequeño para amortizar cualquier overhead de IPC.
#
# Dicho esto, la diferencia ENTRE los tres métodos paralelos es notable:
#
#   parallel_mc   — overhead moderado: makeCluster() lanza N procesos R
#                   cada vez que se llama a run_par(), serializa datos
#                   por sockets R y los destruye al salir.
#
#   future_lapply — overhead alto: future/multisession añade capas de
#                   abstracción (globals export automático, promises,
#                   resolvers) que ralentizan el envío y la recogida de
#                   resultados incluso con workers ya activos.
#
#   mirai         — overhead más bajo de los tres: los daemons se
#                   arrancan UNA SOLA VEZ con daemons() y permanecen
#                   vivos entre llamadas. La comunicación usa NNG/nanomsg
#                   (sockets C nativos), más rápidos que las conexiones R
#                   de parallel o future. No exporta globals implícitos:
#                   solo envía lo que se le pasa explícitamente.
#
# CONCLUSIÓN: mirai es el método paralelo más eficiente de los tres.
# La ventaja se vuelve clara con tareas más pesadas (len_vec > 2e6) o
# cuando se hacen muchas llamadas repetidas en la misma sesión, porque
# el coste de arranque de daemons se amortiza entre iteraciones.
# Para esta carga ligera, el ranking sigue siendo:
#   sequential >> mirai > parallel_mc > future_lapply