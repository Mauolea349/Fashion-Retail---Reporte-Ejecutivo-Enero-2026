-

# Project Specs: Mamy Blue - Sales Intelligence Engine (v7.5)

## 1. Arquitectura de Salida (Power BI Optimized)

- El script debe generar tres (3) pestañas adicionales dedicadas exclusivamente a la ingesta de datos:
  - `DATA_BI`: Facturación neta consolidada (Hechos).
  - `DATA_CANALES`: Desglose de canales con KPIs de devolución (Dimensión).
  - `DATA_CATEGORIAS`: Desglose por categoría (Dimensión).

## 2. Protocolo de Limpieza de Datos

- **Sin Metadatos:** Estas pestañas NO deben contener títulos, filas vacías, ni textos descriptivos ("Oportunidades", "Insights"). Solo encabezados y registros.
- **Normalización Forzada:** Todos los campos de texto usados como IDs (`Articulo`, `Canal`, `Categoria`) deben estar en MAYÚSCULAS y sin espacios adicionales.

## 3. Certificación CISA & Reconciliación

- Se mantiene el sello "CISA CERTIFIED".
- El script debe asegurar que los totales en las pestañas `DATA_...` coincidan exactamente con las pestañas gerenciales.

## 4. Escritura Atómica

- Mantener la seguridad de escritura para evitar corrupción de archivos.
