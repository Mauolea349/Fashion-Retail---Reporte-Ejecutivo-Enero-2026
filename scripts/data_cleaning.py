"""
Data Cleaning Pipeline - ETL + Pareto Unificado
================================================
Script unificado que ejecuta el flujo completo:
1. EXTRACCION: Lee todos los CSV de data/raw/ usando glob
2. LIMPIEZA: Quita nulos, corrige tipos, consolida
3. TRANSFORMACION: Calcula Analisis Pareto (ABC)
4. CARGA: Exporta ventas_final_pareto.csv a data/processed/

Uso:
    python scripts/data_cleaning.py
"""

import glob
import logging
import unicodedata
from pathlib import Path
from typing import Tuple

import pandas as pd
import numpy as np

# =============================================================================
# RUTAS (relativas a la raiz del proyecto - portables entre computadoras)
# =============================================================================
# Path(__file__) obtiene la ubicacion del script actual (scripts/data_cleaning.py)
# .parent.parent sube dos niveles: scripts/ -> raiz del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent

# Carpetas de datos
RAW_DIR = BASE_DIR / 'data' / 'raw'          # Entrada: CSVs crudos
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'  # Salida: CSV procesado
OUTPUT_FILE = PROCESSED_DIR / 'ventas_final_procesadas.csv'

# Carpeta de logs
LOG_DIR = BASE_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# Configuracion de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'data_cleaning.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# FASE 2: NORMALIZACION DE COLUMNAS (SOLUCION DEFINITIVA)
# =============================================================================

def remove_accents(text: str) -> str:
    """
    Elimina tildes y caracteres especiales usando unicodedata.
    Convierte: á->a, é->e, í->i, ó->o, ú->u, ñ->n
    """
    if not isinstance(text, str):
        return str(text)
    # NFD descompone el caracter en base + acento
    # Luego filtramos solo los caracteres base (no combining)
    nfkd = unicodedata.normalize('NFKD', text)
    return ''.join(c for c in nfkd if not unicodedata.combining(c))


def super_normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    SOLUCION DEFINITIVA: Normaliza columnas con FUZZY DETECTION.

    Operaciones:
    1. Elimina espacios en blanco
    2. Elimina tildes usando unicodedata (á->a, í->i, etc.)
    3. FUZZY DETECTION: Busca columnas que contengan 'art' o 'prod' -> 'Articulo'
    4. Mapea nombres comunes a formato estandar

    Args:
        df: DataFrame con columnas a normalizar

    Returns:
        DataFrame con columnas normalizadas
    """
    logger.info("\n" + "="*60)
    logger.info("FASE 2: NORMALIZACION DE COLUMNAS (Fuzzy Detection)")
    logger.info("="*60)

    # DEBUG: Mostrar columnas originales (CRITICO para debugging)
    print(f"\n[DEBUG] ========== NORMALIZACION ==========")
    print(f"[DEBUG] Columnas ORIGINALES (raw): {df.columns.tolist()}")
    logger.info(f"  Columnas originales: {list(df.columns)}")

    # Paso 1: Limpiar cada nombre de columna
    new_columns = []
    for col in df.columns:
        # Strip + remove accents
        clean_col = remove_accents(str(col).strip())
        new_columns.append(clean_col)
        print(f"  [DEBUG] '{col}' -> '{clean_col}'")

    df.columns = new_columns

    # Paso 2: FUZZY DETECTION - Buscar columnas similares
    col_mapping = {}
    articulo_found = False

    for col in df.columns:
        col_lower = col.lower()
        col_clean = remove_accents(col_lower)

        # FUZZY: Buscar 'art' o 'prod' para Articulo
        if not articulo_found:
            if 'art' in col_clean or 'prod' in col_clean or 'codigo' in col_clean or 'sku' in col_clean:
                col_mapping[col] = 'Articulo'
                articulo_found = True
                print(f"  [FUZZY] '{col}' -> 'Articulo' (match: art/prod/codigo/sku)")
                continue

        # Descripcion
        if 'desc' in col_clean or 'nombre' in col_clean:
            col_mapping[col] = 'Descripcion'
            print(f"  [FUZZY] '{col}' -> 'Descripcion'")
        # Cantidad
        elif 'cant' in col_clean and 'total' not in col_clean:
            col_mapping[col] = 'Cantidad'
            print(f"  [FUZZY] '{col}' -> 'Cantidad'")
        # Precio unitario
        elif col_clean == 'precio' or col_clean == 'precio unitario' or col_clean == 'preciou':
            col_mapping[col] = 'Precio'
            print(f"  [FUZZY] '{col}' -> 'Precio'")
        # Total Precio
        elif ('total' in col_clean and 'prec' in col_clean) or col_clean == 'total' or col_clean == 'importe':
            col_mapping[col] = 'Total_precio'
            print(f"  [FUZZY] '{col}' -> 'Total_precio'")

    # Aplicar mapeo
    if col_mapping:
        df = df.rename(columns=col_mapping)
        logger.info(f"  Mapeo aplicado: {col_mapping}")

    # DEBUG: Mostrar columnas finales
    print(f"\n[DEBUG] Columnas FINALES: {df.columns.tolist()}")
    logger.info(f"  Columnas normalizadas: {list(df.columns)}")

    # Verificacion critica con mensaje detallado
    if 'Articulo' not in df.columns:
        logger.error("="*60)
        logger.error("  ERROR CRITICO: Columna 'Articulo' NO encontrada!")
        logger.error(f"  Columnas disponibles: {list(df.columns)}")
        logger.error("  Sugerencia: Verifica que los CSV tengan una columna")
        logger.error("  que contenga 'Articulo', 'Art', 'Producto', 'Codigo' o 'SKU'")
        logger.error("="*60)
        raise KeyError("La columna 'Articulo' no existe. Ver DEBUG arriba.")

    logger.info("  OK: Columna 'Articulo' verificada correctamente")

    return df


# =============================================================================
# FASE 1: EXTRACCION (con deteccion inteligente de headers)
# =============================================================================

def has_valid_columns(df: pd.DataFrame) -> bool:
    """
    Verifica si el DataFrame tiene columnas que parezcan validas.
    Busca columnas que contengan 'art', 'prod', 'desc', 'prec', 'cant'.
    """
    if df.empty or len(df.columns) < 3:
        return False

    cols_lower = [remove_accents(str(c).lower()) for c in df.columns]
    keywords = ['art', 'prod', 'desc', 'prec', 'cant', 'total']

    matches = sum(1 for col in cols_lower for kw in keywords if kw in col)
    return matches >= 2  # Al menos 2 columnas validas


def smart_read_csv(filepath: Path) -> pd.DataFrame:
    """
    Lee un CSV intentando diferentes valores de header (0, 1, 2, 3).
    Retorna el DataFrame cuando encuentra columnas validas.
    """
    encodings = ['utf-8', 'latin-1', 'cp1252']
    headers_to_try = [0, 1, 2, 3, None]

    for encoding in encodings:
        for header in headers_to_try:
            try:
                df = pd.read_csv(filepath, header=header, encoding=encoding)

                # DEBUG: Mostrar que se esta probando
                print(f"  [DEBUG] Probando header={header}, encoding={encoding}")
                print(f"  [DEBUG] Columnas detectadas: {df.columns.tolist()}")

                if has_valid_columns(df):
                    logger.info(f"    OK: header={header}, encoding={encoding}")
                    return df

            except Exception as e:
                continue

    # Si nada funciono, usar header=2 por defecto
    logger.warning(f"  No se encontro header valido, usando header=2")
    return pd.read_csv(filepath, header=2, encoding='utf-8')


def extract_all_files() -> pd.DataFrame:
    """
    Extrae todos los archivos CSV de data/raw/ usando glob.
    - Detecta automaticamente el header correcto
    - Agrega columna 'Sucursal' basada en el nombre del archivo

    Returns:
        DataFrame consolidado con todos los archivos
    """
    logger.info("\n" + "="*60)
    logger.info("FASE 1: EXTRACCION (Smart Header Detection)")
    logger.info("="*60)

    # Buscar todos los CSV usando glob
    pattern = str(RAW_DIR / '*.csv')
    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(f"No se encontraron archivos CSV en {RAW_DIR}")

    logger.info(f"Archivos encontrados: {len(files)}")

    dataframes = []

    for filepath in sorted(files):
        filepath = Path(filepath)
        sucursal_name = filepath.stem.upper()

        logger.info(f"\n  Procesando: {filepath.name}")
        print(f"\n[DEBUG] ========== ARCHIVO: {filepath.name} ==========")

        try:
            # Lectura inteligente con deteccion de header
            df = smart_read_csv(filepath)

            # DEBUG: Columnas detectadas en este archivo
            print(f"[DEBUG] Columnas FINALES del archivo: {df.columns.tolist()}")

            # Agregar columna de origen
            df['Sucursal'] = sucursal_name

            logger.info(f"    {sucursal_name}: {len(df)} filas, {len(df.columns)} columnas")
            dataframes.append(df)

        except Exception as e:
            logger.error(f"  ERROR en {filepath.name}: {e}")
            print(f"[DEBUG] ERROR: {e}")

    if not dataframes:
        raise ValueError("No se pudo cargar ningun archivo")

    # Consolidar
    master_df = pd.concat(dataframes, ignore_index=True)
    logger.info(f"\nTotal extraido: {len(master_df)} filas")

    # DEBUG: Columnas consolidadas
    print(f"\n[DEBUG] ========== CONSOLIDADO ==========")
    print(f"[DEBUG] Columnas despues de pd.concat: {master_df.columns.tolist()}")

    return master_df


# =============================================================================
# FASE 3: LIMPIEZA (con deteccion flexible de columnas)
# =============================================================================

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza columnas criticas y asegura la existencia de Venta_Neta.
    Usa deteccion flexible para encontrar la columna de Total.
    """
    logger.info("\n" + "="*60)
    logger.info("FASE 3: LIMPIEZA (Deteccion Flexible)")
    logger.info("="*60)

    filas_iniciales = len(df)
    logger.info(f"Columnas disponibles: {list(df.columns)}")

    # 1. Mapeo flexible para la columna de Total
    # Buscamos variaciones comunes que resultan de la normalizacion
    posibles_totales = ['Total_precio', 'Total precio', 'Precio total', 'Total_venta', 'Total', 'Importe']
    col_total_encontrada = next((c for c in posibles_totales if c in df.columns), None)

    if col_total_encontrada:
        df['Venta_Neta'] = pd.to_numeric(df[col_total_encontrada], errors='coerce').fillna(0)
        logger.info(f"  OK Columna de total detectada como: '{col_total_encontrada}'")
    else:
        # Si no la encuentra, intentamos buscar una columna que contenga 'Total'
        col_fuzzy = next((c for c in df.columns if 'total' in c.lower()), None)
        if col_fuzzy:
            df['Venta_Neta'] = pd.to_numeric(df[col_fuzzy], errors='coerce').fillna(0)
            logger.info(f"  WARN Usando columna similar para total: '{col_fuzzy}'")
        else:
            # Fallback de seguridad para evitar que el script se detenga
            df['Venta_Neta'] = 0
            logger.error("  ERROR No se encontro columna de Total. Se inicializo en 0.")

    # 2. Asegurar otras columnas necesarias
    if 'Cantidad' in df.columns:
        df['Cantidad'] = pd.to_numeric(df['Cantidad'], errors='coerce').fillna(0)
        logger.info(f"  OK Columna 'Cantidad' convertida a numerico")
    else:
        df['Cantidad'] = 0
        logger.warning(f"  WARN Columna 'Cantidad' no encontrada, inicializada en 0")

    if 'Precio' in df.columns:
        df['Precio'] = pd.to_numeric(df['Precio'], errors='coerce').fillna(0)
        logger.info(f"  OK Columna 'Precio' convertida a numerico")

    # 3. Limpiar Articulo
    if 'Articulo' in df.columns:
        df['Articulo'] = df['Articulo'].astype(str).str.strip().str.upper()
        mask_valido = (df['Articulo'] != '') & (df['Articulo'] != 'NAN') & (df['Articulo'] != 'NONE')
        df = df[mask_valido].copy()
        logger.info(f"  Filas con Articulo vacio eliminadas: {filas_iniciales - len(df)}")

    # =========================================================================
    # CRITICO: Eliminar filas de TOTALES/SUBTOTALES (distorsionan el Pareto)
    # =========================================================================
    filas_antes_filtro = len(df)

    # Patrones a eliminar en Articulo y Descripcion
    patrones_totales = ['TOTAL', 'GRAND TOTAL', 'SUBTOTAL', 'GRAN TOTAL', 'TOTAL GENERAL']

    # Crear mascara para filas que NO sean totales
    mask_no_total = pd.Series(True, index=df.index)

    # Filtrar en columna Articulo
    if 'Articulo' in df.columns:
        for patron in patrones_totales:
            mask_no_total &= ~df['Articulo'].str.contains(patron, case=False, na=False)

    # Filtrar en columna Descripcion
    if 'Descripcion' in df.columns:
        df['Descripcion'] = df['Descripcion'].astype(str).str.strip().str.upper()
        for patron in patrones_totales:
            mask_no_total &= ~df['Descripcion'].str.contains(patron, case=False, na=False)

    # Aplicar filtro
    df = df[mask_no_total].copy()

    filas_totales_eliminadas = filas_antes_filtro - len(df)
    if filas_totales_eliminadas > 0:
        logger.warning(f"  FILTRO PARETO: {filas_totales_eliminadas} filas de TOTAL/SUBTOTAL eliminadas")
        logger.info(f"  (Estas filas distorsionaban el calculo de Pareto)")
    else:
        logger.info(f"  OK No se encontraron filas de TOTAL/SUBTOTAL")

    # 4. Asegurar columna Descripcion
    if 'Descripcion' not in df.columns:
        df['Descripcion'] = 'SIN DESCRIPCION'
        logger.warning(f"  WARN Columna 'Descripcion' no encontrada, usando valor por defecto")

    logger.info(f"\nTotal limpio: {len(df)} filas")
    logger.info(f"Columnas finales: {list(df.columns)}")

    return df


# =============================================================================
# FASE 4: TRANSFORMACION - ANALISIS PARETO (ABC) con Consistencia Total
# =============================================================================

def calculate_pareto(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el analisis de Pareto (ABC) con CONSISTENCIA TOTAL.

    PRINCIPIO CLAVE: Ambas tablas (fact_ventas y dim_articulos) se generan
    desde el MISMO dataset para garantizar que las sumas coincidan.

    PASO 1 - CREAR FACT_VENTAS:
        - Agrupa por Articulo + Sucursal
        - SIN filtrar ventas negativas (para mantener consistencia)

    PASO 2 - CREAR DIM_ARTICULOS DESDE FACT_VENTAS:
        - Agrupa fact_ventas por Articulo
        - Calcula totales, Ranking y Clasificacion ABC
        - Asi: SUM(fact_ventas.Venta_Neta) == SUM(dim_articulos.Venta_Total)

    Args:
        df: DataFrame limpio con columnas Articulo, Sucursal, Venta_Neta, etc.

    Returns:
        Diccionario con 'fact_ventas' y 'dim_articulos' consistentes
    """
    logger.info("\n" + "="*60)
    logger.info("FASE 4: TRANSFORMACION - PARETO CON CONSISTENCIA TOTAL")
    logger.info("="*60)

    cols_disponibles = df.columns.tolist()
    logger.info(f"  Columnas disponibles: {cols_disponibles}")

    # Verificar columnas requeridas
    for col in ['Articulo', 'Venta_Neta', 'Sucursal']:
        if col not in cols_disponibles:
            raise KeyError(f"La columna '{col}' es requerida")

    # ==========================================================================
    # PASO 1: CREAR FACT_VENTAS (tabla de hechos con Bruta/Devolucion/Neta)
    # ==========================================================================
    logger.info("\n  PASO 1: Crear fact_ventas (tabla de hechos)...")

    # Crear columnas auxiliares para Bruta y Devolucion
    df['_Venta_Bruta'] = df['Venta_Neta'].apply(lambda x: x if x > 0 else 0)
    df['_Venta_Devolucion'] = df['Venta_Neta'].apply(lambda x: abs(x) if x < 0 else 0)

    # Agregacion por Articulo + Sucursal
    agg_fact = {
        'Venta_Neta': 'sum',
        '_Venta_Bruta': 'sum',
        '_Venta_Devolucion': 'sum'
    }
    if 'Cantidad' in cols_disponibles:
        agg_fact['Cantidad'] = 'sum'

    fact_ventas = df.groupby(['Articulo', 'Sucursal']).agg(agg_fact).reset_index()

    # Renombrar columnas
    fact_ventas = fact_ventas.rename(columns={
        '_Venta_Bruta': 'Venta_Bruta',
        '_Venta_Devolucion': 'Venta_Devolucion'
    })

    # Redondear valores
    fact_ventas['Venta_Neta'] = fact_ventas['Venta_Neta'].round(2)
    fact_ventas['Venta_Bruta'] = fact_ventas['Venta_Bruta'].round(2)
    fact_ventas['Venta_Devolucion'] = fact_ventas['Venta_Devolucion'].round(2)
    if 'Cantidad' in fact_ventas.columns:
        fact_ventas['Cantidad'] = fact_ventas['Cantidad'].round(0)

    # Limpiar columnas auxiliares del df original
    df.drop(columns=['_Venta_Bruta', '_Venta_Devolucion'], inplace=True)

    # Estadisticas
    total_bruta = fact_ventas['Venta_Bruta'].sum()
    total_devolucion = fact_ventas['Venta_Devolucion'].sum()
    total_neta = fact_ventas['Venta_Neta'].sum()
    tasa_devolucion = (total_devolucion / total_bruta * 100) if total_bruta > 0 else 0

    logger.info(f"    Filas: {len(fact_ventas)}")
    logger.info(f"    Sucursales: {fact_ventas['Sucursal'].nunique()}")
    logger.info(f"    Venta Bruta:      ${total_bruta:,.2f}")
    logger.info(f"    Devoluciones:     ${total_devolucion:,.2f}")
    logger.info(f"    Venta Neta:       ${total_neta:,.2f}")
    logger.info(f"    Tasa Devolucion:  {tasa_devolucion:.2f}%")

    # ==========================================================================
    # PASO 2: CREAR DIM_ARTICULOS DESDE FACT_VENTAS (garantiza consistencia)
    # ==========================================================================
    logger.info("\n  PASO 2: Crear dim_articulos DESDE fact_ventas...")

    # Agregar Descripcion al fact_ventas temporalmente para el merge
    if 'Descripcion' in cols_disponibles:
        desc_map = df.groupby('Articulo')['Descripcion'].first().to_dict()

    # Agrupar fact_ventas por Articulo para obtener totales (incluyendo Bruta/Devolucion)
    dim_articulos = fact_ventas.groupby('Articulo').agg({
        'Venta_Neta': 'sum',
        'Venta_Bruta': 'sum',
        'Venta_Devolucion': 'sum'
    }).reset_index()

    # Renombrar columnas para dim_articulos
    dim_articulos = dim_articulos.rename(columns={
        'Venta_Neta': 'Venta_Neta_Total',
        'Venta_Bruta': 'Venta_Bruta_Total',
        'Venta_Devolucion': 'Venta_Devolucion_Total'
    })

    # Calcular Tasa de Devolucion por articulo (como decimal para Excel)
    dim_articulos['Tasa_Devolucion'] = (
        dim_articulos['Venta_Devolucion_Total'] / dim_articulos['Venta_Bruta_Total']
    ).fillna(0).round(4)

    # Agregar Descripcion
    if 'Descripcion' in cols_disponibles:
        dim_articulos['Descripcion'] = dim_articulos['Articulo'].map(desc_map)

    # Filtrar solo articulos con venta NETA positiva para el RANKING
    dim_pareto = dim_articulos[dim_articulos['Venta_Neta_Total'] > 0].copy()

    if len(dim_pareto) == 0:
        raise ValueError("No hay articulos con venta positiva")

    # Ordenar por venta descendente
    dim_pareto = dim_pareto.sort_values('Venta_Neta_Total', ascending=False).reset_index(drop=True)

    # Calcular metricas Pareto (DECIMALES para Excel/Power BI)
    total_positivo = dim_pareto['Venta_Neta_Total'].sum()
    dim_pareto['Porcentaje_Articulo_Global'] = (dim_pareto['Venta_Neta_Total'] / total_positivo).round(4)
    dim_pareto['Porcentaje_Acumulado'] = dim_pareto['Porcentaje_Articulo_Global'].cumsum().round(4)

    # Clasificacion ABC (umbrales en decimal)
    dim_pareto['Clasificacion_ABC'] = 'C'
    dim_pareto.loc[dim_pareto['Porcentaje_Acumulado'] <= 0.80, 'Clasificacion_ABC'] = 'A'
    dim_pareto.loc[
        (dim_pareto['Porcentaje_Acumulado'] > 0.80) &
        (dim_pareto['Porcentaje_Acumulado'] <= 0.95),
        'Clasificacion_ABC'
    ] = 'B'

    # Ranking
    dim_pareto['Ranking'] = range(1, len(dim_pareto) + 1)

    logger.info(f"    Articulos con venta positiva: {len(dim_pareto)}")
    logger.info(f"    Facturacion (solo positivos): ${total_positivo:,.2f}")

    # Estadisticas ABC
    clase_a = dim_pareto[dim_pareto['Clasificacion_ABC'] == 'A']
    clase_b = dim_pareto[dim_pareto['Clasificacion_ABC'] == 'B']
    clase_c = dim_pareto[dim_pareto['Clasificacion_ABC'] == 'C']

    logger.info(f"\n  CLASIFICACION ABC GLOBAL:")
    logger.info(f"  {'='*50}")
    logger.info(f"  Clase A (80%):  {len(clase_a):>4} articulos ({len(clase_a)/len(dim_pareto)*100:.1f}%)")
    logger.info(f"  Clase B (95%):  {len(clase_b):>4} articulos ({len(clase_b)/len(dim_pareto)*100:.1f}%)")
    logger.info(f"  Clase C (100%): {len(clase_c):>4} articulos ({len(clase_c)/len(dim_pareto)*100:.1f}%)")
    logger.info(f"  {'='*50}")

    # Top 5
    logger.info(f"\n  TOP 5 ARTICULOS:")
    for _, row in dim_pareto.head(5).iterrows():
        articulo = str(row['Articulo'])[:15]
        desc = str(row.get('Descripcion', ''))[:20]
        venta = row['Venta_Neta_Total']
        pct = row['Porcentaje_Articulo_Global'] * 100
        abc = row['Clasificacion_ABC']
        logger.info(f"    #{int(row['Ranking'])}: {articulo} {desc} - ${venta:,.0f} ({pct:.2f}%) [{abc}]")

    # ==========================================================================
    # PASO 3: VERIFICACION DE CONSISTENCIA
    # ==========================================================================
    logger.info("\n  PASO 3: Verificacion de consistencia...")

    total_dim = dim_pareto['Venta_Neta_Total'].sum()
    # Solo comparar con fact_ventas de articulos en dim_pareto
    articulos_en_dim = set(dim_pareto['Articulo'].unique())
    fact_filtrado = fact_ventas[fact_ventas['Articulo'].isin(articulos_en_dim)]
    total_fact_filtrado = fact_filtrado['Venta_Neta'].sum()

    diferencia = abs(total_dim - total_fact_filtrado)
    logger.info(f"    SUM(dim_articulos.Venta_Neta_Total): ${total_dim:,.2f}")
    logger.info(f"    SUM(fact_ventas.Venta_Neta):         ${total_fact_filtrado:,.2f}")
    logger.info(f"    Diferencia: ${diferencia:,.2f}")

    if diferencia < 1:  # Tolerancia de $1 por redondeo
        logger.info(f"    CONSISTENCIA: OK")
    else:
        logger.warning(f"    CONSISTENCIA: DIFERENCIA DETECTADA!")

    # ==========================================================================
    # PREPARAR SALIDA FINAL
    # ==========================================================================
    logger.info("\n  MODELO ESTRELLA (Star Schema):")

    # Ordenar columnas de dim_articulos
    cols_dim = ['Articulo', 'Descripcion', 'Ranking', 'Clasificacion_ABC',
                'Venta_Neta_Total', 'Venta_Bruta_Total', 'Venta_Devolucion_Total',
                'Tasa_Devolucion', 'Porcentaje_Articulo_Global', 'Porcentaje_Acumulado']
    cols_dim = [c for c in cols_dim if c in dim_pareto.columns]
    dim_articulos_final = dim_pareto[cols_dim].copy()

    logger.info(f"    dim_articulos: {len(dim_articulos_final)} filas")

    # Filtrar fact_ventas solo para articulos en dim_articulos (integridad referencial)
    fact_ventas_final = fact_filtrado.copy()

    logger.info(f"    fact_ventas: {len(fact_ventas_final)} filas")
    logger.info(f"    Integridad referencial: OK")

    return {
        'fact_ventas': fact_ventas_final,
        'dim_articulos': dim_articulos_final
    }


# =============================================================================
# FASE 5: CARGA - MODELO ESTRELLA (2 archivos)
# =============================================================================

# Rutas para modelo estrella
FACT_FILE = PROCESSED_DIR / 'fact_ventas.csv'
DIM_FILE = PROCESSED_DIR / 'dim_articulos.csv'


def load_star_schema(tablas: dict) -> dict:
    """
    Exporta el modelo estrella a 2 archivos CSV.

    Args:
        tablas: Diccionario con 'fact_ventas' y 'dim_articulos'

    Returns:
        Diccionario con paths a los archivos generados
    """
    logger.info("\n" + "="*60)
    logger.info("FASE 5: CARGA - MODELO ESTRELLA (Star Schema)")
    logger.info("="*60)

    # Asegurar que el directorio existe
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # ==========================================================================
    # FORMATO EXCEL-SPANISH (Argentina)
    # - Separador de columnas: punto y coma (;)
    # - Separador decimal: coma (,)
    # ==========================================================================
    CSV_SEP = ';'
    CSV_DECIMAL = ','

    logger.info(f"\n  Formato CSV: Excel-Spanish (sep='{CSV_SEP}', decimal='{CSV_DECIMAL}')")

    paths = {}

    # 1. Exportar DIMENSION: dim_articulos
    dim_articulos = tablas['dim_articulos']
    dim_articulos.to_csv(DIM_FILE, index=False, encoding='utf-8', sep=CSV_SEP, decimal=CSV_DECIMAL)
    paths['dim_articulos'] = DIM_FILE

    logger.info(f"\n  DIMENSION: dim_articulos.csv")
    logger.info(f"    Filas: {len(dim_articulos)}")
    logger.info(f"    Columnas: {list(dim_articulos.columns)}")
    logger.info(f"    Clave primaria: Articulo")

    # 2. Exportar DIMENSION: dim_sucursales (nueva)
    fact_ventas = tablas['fact_ventas']
    sucursales_unicas = fact_ventas['Sucursal'].unique()
    dim_sucursales = pd.DataFrame({
        'Sucursal': sucursales_unicas,
        'Tipo': ['ONLINE' if 'ONLINE' in s else 'FISICA' for s in sucursales_unicas]
    })
    dim_sucursales = dim_sucursales.sort_values('Sucursal').reset_index(drop=True)

    DIM_SUC_FILE = PROCESSED_DIR / 'dim_sucursales.csv'
    dim_sucursales.to_csv(DIM_SUC_FILE, index=False, encoding='utf-8', sep=CSV_SEP, decimal=CSV_DECIMAL)
    paths['dim_sucursales'] = DIM_SUC_FILE

    logger.info(f"\n  DIMENSION: dim_sucursales.csv")
    logger.info(f"    Filas: {len(dim_sucursales)}")
    logger.info(f"    Columnas: {list(dim_sucursales.columns)}")
    logger.info(f"    Clave primaria: Sucursal")

    # 3. Exportar HECHOS: fact_ventas
    fact_ventas.to_csv(FACT_FILE, index=False, encoding='utf-8', sep=CSV_SEP, decimal=CSV_DECIMAL)
    paths['fact_ventas'] = FACT_FILE

    logger.info(f"\n  HECHOS: fact_ventas.csv")
    logger.info(f"    Filas: {len(fact_ventas)}")
    logger.info(f"    Columnas: {list(fact_ventas.columns)}")
    logger.info(f"    Claves foraneas:")
    logger.info(f"      - Articulo -> dim_articulos")
    logger.info(f"      - Sucursal -> dim_sucursales")

    # Resumen del modelo
    logger.info(f"\n  RELACIONES EN POWER BI:")
    logger.info(f"  fact_ventas[Articulo] ---> dim_articulos[Articulo]")
    logger.info(f"  fact_ventas[Sucursal] ---> dim_sucursales[Sucursal]")
    logger.info(f"  (Muchos a Uno, direccion: Ambas)")

    return paths


# =============================================================================
# PIPELINE PRINCIPAL
# =============================================================================

def run_pipeline() -> Tuple[dict, dict]:
    """
    Ejecuta el pipeline completo ETL + Pareto con modelo estrella.

    Returns:
        Tuple con (diccionario de DataFrames, diccionario de Paths)
    """
    logger.info("\n" + "#"*60)
    logger.info("DATA CLEANING PIPELINE - MODELO ESTRELLA")
    logger.info("#"*60)

    # 1. Extraccion
    raw_df = extract_all_files()

    # 2. Normalizacion de columnas (ANTES de cualquier procesamiento)
    raw_df = super_normalize_columns(raw_df)

    # 3. Limpieza
    clean_df = clean_data(raw_df)

    # 4. Transformacion (Pareto + Modelo Estrella)
    tablas = calculate_pareto(clean_df)

    # 5. Carga (2 archivos CSV)
    paths = load_star_schema(tablas)

    logger.info("\n" + "#"*60)
    logger.info("PIPELINE COMPLETADO EXITOSAMENTE")
    logger.info(f"Archivos generados (formato Excel-Spanish):")
    logger.info(f"  - {paths['dim_articulos'].name} (Dimension Articulos)")
    logger.info(f"  - {paths['dim_sucursales'].name} (Dimension Sucursales)")
    logger.info(f"  - {paths['fact_ventas'].name} (Tabla de Hechos)")
    logger.info("#"*60)

    return tablas, paths


if __name__ == '__main__':
    tablas, paths = run_pipeline()
    print(f"\nArchivos listos para Power BI (formato Excel-Spanish):")
    print(f"  1. {paths['dim_articulos']}")
    print(f"  2. {paths['dim_sucursales']}")
    print(f"  3. {paths['fact_ventas']}")
