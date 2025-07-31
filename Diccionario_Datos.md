---

## 📘 Diccionario de Datos

| Tabla              | Descripción                                                      |
|--------------------|------------------------------------------------------------------|
| `dim_ventas`       | Detalles individuales de cada venta realizada                    |
| `dim_productos`    | Información descriptiva de los productos                         |
| `dim_clientes`     | Datos de los clientes (nombre, país, etc.)                       |
| `dim_territorio`   | Información sobre territorios (país, región, continente)         |
| `dim_fecha`        | Dimensión de fechas con jerarquías como año, mes, día            |
| `df_hechos`        | Tabla de hechos que consolida las métricas principales de venta  |

| Columna             | Tabla asociada       | Descripción                                        |
|---------------------|----------------------|----------------------------------------------------|
| `ProductoID`         | Varias               | Identificador único de producto                    |
| `FechaID`            | `dim_fecha`, `df_hechos` | Fecha como clave para análisis temporal      |
| `Cantidad_pedido`    | `df_hechos`          | Unidades vendidas                                 |
| `Linea_total`        | `df_hechos`          | Monto total por línea de pedido                   |
| `TerritorioID`       | `dim_territorio`, `df_hechos` | Identificador del territorio               |

---

## 📗 Glosario TI

- **Dataset**: Conjunto de datos estructurados, generalmente en forma de tabla.
- **ETL**: Proceso de Extracción, Transformación y Carga de datos desde distintas fuentes hacia un repositorio.
- **Data Warehouse (DW)**: Almacén centralizado donde se integran y organizan datos para su análisis.
- **Dimensión**: Tabla con atributos descriptivos utilizados para analizar los hechos (ej. cliente, producto, tiempo).
- **Hechos**: Tabla con métricas numéricas de negocio que se quieren analizar (ej. monto, cantidad).
- **Spark**: Motor de procesamiento distribuido utilizado para trabajar con grandes volúmenes de datos.
- **Delta Lake**: Formato de almacenamiento transaccional que permite versiones y actualizaciones eficientes en entornos Spark.
- **Bronze / Silver / Gold**: Niveles de limpieza en una arquitectura medallion. Bronze: datos crudos, Silver: datos limpios, Gold: datos listos para análisis o consumo.

---
