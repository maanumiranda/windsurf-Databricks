# Apache Spark Learning Project

A comprehensive project for learning and practicing Apache Spark with both Python (PySpark) and SQL examples.


# 📚 Módulo 2: Apache Spark – Ingesta de Datos con Lakeflow Connect

**Universidad Latina de Costa Rica**  
**Técnico en Ingeniería de Datos con Databricks**

Este módulo introduce los conceptos fundamentales para el procesamiento de datos a gran escala utilizando **Apache Spark** y las herramientas modernas de **ingesta de datos en Databricks**, incluyendo **Lakeflow Connect**, **Auto Loader** y **Databricks Connect**.

El objetivo del curso es que los estudiantes comprendan cómo diseñar, construir y depurar **pipelines de datos modernos** dentro del ecosistema Databricks.

---

# 📖 Contenidos del Módulo

## Apache Spark

### Fundamentos de programación en sistemas distribuidos
- Paralelismo
- Tolerancia a fallos
- Distribución de datos

### Introducción a Apache Spark
- Conceptos fundamentales
- Procesamiento distribuido
- Casos de uso en Big Data

### Arquitectura de ejecución de Apache Spark
- Driver
- Executors
- Cluster Manager
- DAG Scheduler

### Introducción a DataFrames y SQL en Apache Spark
- Concepto de DataFrame
- Transformaciones vs acciones
- Uso de Spark SQL
- Manipulación de datos estructurados



### Operaciones ETL básicas con la API de DataFrames
- Lectura de datos
- Transformaciones
- Limpieza de datos
- Escritura de resultados

---

# Ingesta de Datos con Lakeflow Connect

### Concepto de ingesta moderna en Databricks
- Arquitecturas modernas de datos
- Ingesta batch vs streaming

### Lakeflow Connect: visión general
- Qué es Lakeflow Connect
- Componentes principales
- Casos de uso

### Conectores estándar
- Conexión con bases de datos
- Integración con sistemas externos

### Conectores administrados
- Gestión automática de conexiones
- Simplificación de pipelines de datos

### Arquitectura de ingesta en Databricks
- Flujo de datos
- Integración con Delta Lake
- Pipelines de ingesta

### Alternativas de ingesta
- APIs
- Archivos
- Bases de datos
- Streaming

---

# Auto Loader

### Qué es Auto Loader
Auto Loader es una herramienta de Databricks que permite la **ingesta incremental y automática de archivos** desde almacenamiento en la nube.

### Cuándo usar Auto Loader
- Ingesta continua de datos
- Procesamiento incremental
- Manejo eficiente de grandes volúmenes de archivos

### Fuentes válidas de datos
- Cloud storage
- Archivos JSON
- CSV
- Parquet
- Avro

### Sintaxis `cloudFiles`
Uso de la opción `cloudFiles` para definir fuentes de ingesta.

### Configuración básica
- Directorio de entrada
- Formato de datos
- Checkpoints

### Procesamiento incremental
- Detección automática de nuevos archivos
- Escalabilidad

### Casos de uso
- Procesamiento batch
- Procesamiento streaming

### Buenas prácticas de ingesta continua
- Uso de checkpoints
- Manejo de esquemas
- Control de errores

---

# Databricks Connect

### Qué es Databricks Connect
Herramienta que permite **desarrollar aplicaciones Spark localmente** y ejecutarlas en clusters de Databricks.

### Uso con VS Code / desarrollo local
- Configuración del entorno
- Integración con el IDE

### Integración con Spark remoto
- Conexión con clusters
- Ejecución remota de código

### Debugging y desarrollo
- Pruebas locales
- Iteración rápida

---

# Debugging y Desarrollo de Pipelines

### Debugging en notebooks
- Identificación de errores
- Logs de ejecución

### Errores comunes en pipelines
- Problemas de esquema
- Fallos de conexión
- Manejo incorrecto de transformaciones

### Testing básico de pipelines de datos
- Validación de datos
- Pruebas de transformación

### Buenas prácticas de desarrollo en Data Engineering
- Modularización de código
- Control de versiones
- Observabilidad
- Documentación de pipelines

---

# 🎯 Objetivos de Aprendizaje

Al finalizar este módulo, el estudiante podrá:

- Comprender la **arquitectura de Apache Spark**
- Construir **pipelines ETL utilizando DataFrames**
- Implementar **ingesta de datos moderna en Databricks**
- Utilizar **Auto Loader para ingesta incremental**
- Desarrollar y depurar pipelines con **Databricks Connect**

---

# 🛠 Tecnologías Utilizadas

- Apache Spark
- Databricks
- Delta Lake
- Lakeflow Connect
- Auto Loader
- Databricks Connect
