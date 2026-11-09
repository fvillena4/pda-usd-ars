# ETL del Tipo de Cambio ARS/USD - Trabajo Práctico

Este proyecto implementa un proceso ETL (Extract, Transform, Load) que obtiene los tipos de cambio ARS/USD desde una API externa, transforma los datos y los carga enAmazon Redshift. El proyecto se ejecuta mediante Apache Airflow y está configurado para correr en un entorno Docker.

## Características del Proyecto

- **Extracción**: Obtiene los tipos de cambio de una API pública de cotizaciones del dólar en Argentina.
- **Transformación**: Procesa los datos, renombra columnas, y calcula el promedio entre precios de compra y venta.
- **Carga**: Inserta los datos en una tabla de Redshift, almacenando la información de manera incremental.

## Requisitos Previos

- Tener instalados Docker y Docker Compose.
- Credenciales de Amazon Redshift.

## Configuración del Entorno

### Paso 1: Clonar el Repositorio

Primero, clona este repositorio para tener acceso a todos los archivos necesarios:

```bash
git clone https://github.com/fvillena4/pda-usd-ars.git
cd pda-usd-ars
```

### Paso 2: Agregar las credenciales en `.env`

Puedes usar el archivo `.env` como referencia:

```bash
cp .env.example .env
```

Luego edita el archivo `.env` y completa los valores de las siguientes variables de entorno:

```env
REDSHIFT_HOST=<tu_redshift_host>
REDSHIFT_PORT=5439
REDSHIFT_DB=<tu_redshift_db>
REDSHIFT_USER=<tu_redshift_usuario>
REDSHIFT_PASSWORD=<tu_redshift_contraseña>
SCHEMA_NAME=<tu_schema_name>
TABLE_NAME=<tu_table_name>
```

## Ejecutar el ETL con Docker

El proyecto está configurado para ejecutarse usando Docker y Docker Compose. Sigue estos pasos para iniciar el entorno:

### Paso 1: Construir y Levantar los Contenedores

Usa Docker Compose para construir la imagen y levantar los contenedores necesarios para Apache Airflow:

```bash
docker-compose up --build
```

Este comando hará lo siguiente:

- Construir las imágenes de Docker necesarias (Airflow, base de datos, etc.).
- Levantar los contenedores, incluyendo el servidor web de Airflow y el scheduler.

### Paso 2: Acceder a la Interfaz de Airflow

Una vez que los contenedores estén en funcionamiento, puedes acceder a la interfaz de Apache Airflow en tu navegador en la dirección:

```
http://localhost:8080
```

Usa las credenciales predeterminadas para iniciar sesión:

- **Usuario**: `airflow`
- **Contraseña**: `airflow`

### Paso 3: Ejecutar el DAG

En Airflow, ejecuta el DAG `redshift_etl_dag`.

## Ejecutar Tests

Este proyecto incluye un conjunto de tests unitarios que se pueden ejecutar usando pytest. Se ejecutan al principio del ETL.

Esto ejecutará los tests definidos en la carpeta `tests` para verificar el correcto funcionamiento del ETL.

## Estructura del Proyecto

- `.github/workflows/ci-cd.yml`: Configuración del pipeline de CI/CD para ejecutar tests automáticos con GitHub Actions.
- `dags/redshift_etl_dag.py`: Contiene el DAG de Airflow que define el proceso ETL.
- `tests/test_etl.py`: Tests unitarios para validar las funciones del ETL.
- `Dockerfile`: Archivo que define la imagen de Docker para el entorno de Airflow.
- `docker-compose.yaml`: Define los servicios para levantar Apache Airflow con Docker.
- `requirements.txt`: Lista de dependencias necesarias para el proyecto.
- `.env.`: Archivo de ejemplo para definir las variables de entorno necesarias.