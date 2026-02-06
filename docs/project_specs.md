
# Project Specs: Sales Intelligence Engine (v7.5)

## 1. Arquitectura de Salida (Power BI Optimized)

El motor de procesamiento genera una estructura de datos normalizada para su ingesta directa en herramientas de visualización, dividida en las siguientes entidades:

- **DATA_BI**: Tabla de hechos con la facturación neta consolidada.
- **DATA_CANALES**: Dimensión de canales de venta con métricas de performance y tasas de retorno.
- **DATA_CATEGORIAS**: Dimensión jerárquica por categoría de producto.

## 2. Protocolo de Limpieza y Estandarización

Para asegurar la integridad del modelo, el script aplica las siguientes reglas de transformación:

- **Estructura Atómica:** Las tablas de salida no contienen metadatos, filas de resumen ni celdas vacías. Solo registros puros con encabezados estandarizados.
- **Normalización de IDs:** Todos los campos clave (`Articulo`, `Canal`, `Categoria`) se procesan en mayúsculas, eliminando espacios redundantes y caracteres especiales para evitar errores de relación en el modelo de datos.

## 3. Protocolo de Reconciliación (Audit Ready)

El proceso incluye una etapa de validación contable para asegurar que:

- Los totales consolidados en las tablas de hechos coincidan con los reportes de origen.
- Se mantenga la trazabilidad de los datos desde la extracción hasta la carga final (ETL).

## 4. Integridad de Escritura

- Se implementan métodos de escritura atómica para prevenir la corrupción de archivos durante el proceso de exportación y asegurar la disponibilidad de los datos para el reporte ejecutivo.
