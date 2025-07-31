# Adventure Works - ETL con PySpark y Arquitectura Medallion

Este proyecto implementa un flujo ETL usando PySpark basado en la base de datos AdventureWorks2019. Se sigue la arquitectura Medallion (Bronze, Silver, Gold) para simular un pipeline de datos con fines de análisis de ventas.

Estructura del repositorio:
- `data/`: Archivos CSV extraídos desde la base de datos.
- `ambientes/`: Almacén de datos delta.
- `src/`: Notebooks organizados por fases: Extracción, Transformación, Carga.

Tecnologías utilizadas:
- PySpark
- Jupyter Notebook
- Git & GitHub
- Arquitectura Medallion (Bronze, Silver, Gold)

Instrucciones para la ejecución:
1. Clona este repositorio
2. Asegúrate de tener PySpark instalado
3. Abre los notebooks en la carpeta `src/` y sigue el orden:
   - `extraccion.ipynb`
   - `transformacion.ipynb`
   - `carga.ipynb`

Desarrollado por Giovani Curo  
[LinkedIn](https://www.linkedin.com/in/giovani-curo-medina-5b456717a/)  


Notebook 1: Extracción de Datos (Capa Bronze)
Este notebook representa la capa Bronze de la arquitectura Medallion. Se encarga de extraer los datos desde archivos .csv provenientes de la base AdventureWorks2019 y cargarlos en formato Delta Lake para asegurar trazabilidad y eficiencia en etapas posteriores.

Pasos principales:
Se inicia una sesión Spark configurada para trabajar con Delta Lake.

Se cargan 5 datasets planos (Customer, Product, SalesOrderDetail, SalesOrderHeader, SalesTerritory).

A cada dataframe se le añaden:
fecha_carga: timestamp del momento de lectura.
archivo_origen: nombre del archivo fuente.

Con la finalidad de tener control de versiones sobre auditorías

Los datos se escriben en formato Delta en la ruta ../sql/dw_bronze/, uno por archivo.

Datasets procesados:

Customer.csv
Product.csv
SalesOrderDetail.csv
SalesOrderHeader.csv
SalesTerritory.csv

Salida:
Los datos extraídos se almacenan como tablas Delta en carpetas independientes dentro de la capa dw_bronze.

Notebook 2: Transformación de Datos ( Capa Silver)
En esta etapa se desarrolla la capa Silver, encargada de limpiar, estandarizar y enriquecer los datos extraídos desde la capa Bronze. También se genera un conjunto de tablas dimensionales que serán utilizadas para análisis posteriores en la capa Gold.

Limpieza y Estandarización:
Eliminación de duplicados y registros nulos clave (CustomerID, ProductID, SalesOrderID, TerritoryID).
Eliminación de campos de auditoría (fecha_carga, archivo_origen).
Conversión de tipos de datos (por ejemplo, fechas en formato timestamp).
Traducción y renombrado de columnas del inglés al español para facilitar la lectura y el entendimiento.

Construcción de tablas dimensionales:
A partir de joins y proyecciones sobre los datos limpios, se crean:
dim_ventas: tabla de hechos de ventas que integra pedidos y detalles de productos.
dim_productos: catálogo de productos con atributos clave.
dim_clientes: referencia de clientes y sus territorios.
dim_territorio: información de zonas geográficas y continentes.
dim_fecha: tabla de fechas generada con rangos desde 2011 hasta 2015, incluyendo atributos como día, mes, trimestre y si es fin de semana.

Salida:
Los dataframes resultantes se almacenan en formato Delta Lake en la carpeta ../sql/dw_silver/ bajo los siguientes nombres:
dim_ventas
dim_productos
dim_clientes
dim_territorio
dim_fecha

Notebook 3: Vista de Reportes y Análisis (Capa Gold)
En esta última etapa del pipeline ETL se construye la tabla de hechos consolidada y se realizan consultas analíticas clave utilizando las dimensiones procesadas en la capa Silver.

Construcción de la tabla de hechos
Se genera una tabla de hechos llamada df_hechos, que contiene:
ProductoID, PedidoID, ClienteID, TerritorioID
FechaID: extraída y transformada desde la fecha de pedido (FechaPedido)
Cantidad_pedido
Linea_total: valor monetario total por línea de pedido
Los datos son almacenados en formato Delta en la ruta ../sql/dw_gold/df_hechos.

Consultas y análisis realizados
Se ejecutan diversas consultas analíticas sobre la tabla de hechos y dimensiones:
Suma de ventas por continente: total de ventas agrupado por la columna Continente.
Cantidad vendida por producto: total de unidades vendidas por nombre del producto.
Top 10 productos más vendidos: productos con mayor cantidad total pedida.
Ventas agrupadas por continente y producto: para entender la distribución geográfica de las ventas por ítem.
Ventas por fecha y continente: útil para análisis de tendencias temporales y estacionales.