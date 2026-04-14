"""
Módulo de limpieza básica reutilizable para las bases del proyecto
Cierre de Brechas - PNIE 2017-2028.

Aplica los siguientes pasos genéricos a cualquier DataFrame:
  1. Estandarización de nombres de columnas (minúsculas, sin tildes,
     sin espacios, sin saltos de línea).
  2. Limpieza de strings (strip, espacios múltiples, valores nulos textuales).
  3. Normalización de IDs (cod_local, cod_mod, cui, anexo) como string
     preservando ceros a la izquierda.
  4. Normalización Sí/No a 0/1.
  5. Reporte de duplicados (exactos y por clave).
  6. Ficha de calidad por variable (% completitud, unicidad, tipo).

No aplica criterios de negocio (esos se aplican en scripts posteriores).
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

# ──────────────────────────────────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────────────────────────────────

# Valores que, aunque vengan como texto, deben tratarse como nulos.
NULOS_TEXTO = {
    "", "-", "--", "---",
    "N/A", "NA", "n/a", "na", "N.A.",
    "S/N", "s/n", "SIN DATO", "SIN INFORMACION", "SIN INFORMACIÓN",
    "ND", "nd", "#N/A", "#¡REF!", "#REF!",
    "null", "NULL", "None", "NONE", "nan", "NaN",
}

# Mapeo de respuestas Sí/No
MAPA_SINO = {
    "SI": 1, "SÍ": 1, "S": 1, "1": 1, "TRUE": 1, "X": 1, "YES": 1, "Y": 1,
    "NO": 0, "N": 0, "0": 0, "FALSE": 0,
}

# Regex para extraer códigos modulares del formato "[A2 - 0257048] [E1 - 1735752]"
# típico de las hojas UGM - Mantenimiento / Accesibilidad.
RE_COD_MOD_LISTA = re.compile(r"\[\s*([A-Z]\d?)\s*-\s*(\d+)\s*\]")


# ──────────────────────────────────────────────────────────────────────────
# Capa 1 - Columnas
# ──────────────────────────────────────────────────────────────────────────

def normaliza_col(nombre: str) -> str:
    """Convierte un nombre de columna a snake_case ASCII."""
    s = unicodedata.normalize("NFKD", str(nombre))
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip()
    s = re.sub(r"[\n\r\t]+", " ", s)
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


# Mapa de nombres que se convierten a la convención estándar del proyecto
ALIAS_COL = {
    "codigo_local": "cod_local",
    "codigo_de_local": "cod_local",
    "codigo_del_local": "cod_local",
    "codigo_del_local_educativo": "cod_local",
    "codlocal": "cod_local",
    "cod_loc": "cod_local",
    "codigo_modular": "cod_mod",
    "codigo_mod": "cod_mod",
    "codmod": "cod_mod",
    "cod_modular": "cod_mod",
    "codigo_unico_de_inversion": "cui",
    "codigo_unico_de_inversion_cui": "cui",
    "codigo_unificado_de_inversion_c": "cui",
    "codigo_unificado_de_inversion_cui": "cui",
    "cod_unico_inversion": "cui",
    "fur": "cui",
}


def estandariza_columnas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [normaliza_col(c) for c in df.columns]
    # aplica alias hacia la convención estándar del proyecto
    df.columns = [ALIAS_COL.get(c, c) for c in df.columns]
    # si quedaron nombres duplicados, añadir sufijo numérico
    cols, vistos = [], {}
    for c in df.columns:
        if c in vistos:
            vistos[c] += 1
            cols.append(f"{c}_{vistos[c]}")
        else:
            vistos[c] = 0
            cols.append(c)
    df.columns = cols
    return df


# ──────────────────────────────────────────────────────────────────────────
# Capa 2 y 3 - Tipificación y strings
# ──────────────────────────────────────────────────────────────────────────

def limpia_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip, colapsa espacios múltiples y convierte nulos textuales a NA."""
    df = df.copy()
    for c in df.select_dtypes(include="object").columns:
        s = df[c].astype("string").str.strip()
        s = s.str.replace(r"\s+", " ", regex=True)
        s = s.where(~s.isin(NULOS_TEXTO), other=pd.NA)
        df[c] = s
    return df


def normaliza_id(serie: pd.Series, ancho: int | None = None) -> pd.Series:
    """
    Convierte una columna de ID a string sin decimales flotantes
    y preserva ceros a la izquierda si se provee ancho.
    """
    s = serie.copy()
    # castear desde float / int / str sin perder ceros
    if pd.api.types.is_numeric_dtype(s):
        s = s.astype("Int64").astype("string")
    else:
        s = s.astype("string").str.strip()
        s = s.str.replace(r"\.0$", "", regex=True)
    s = s.str.upper()
    s = s.where(~s.isin(NULOS_TEXTO), other=pd.NA)
    if ancho is not None:
        mask = s.notna()
        s.loc[mask] = s.loc[mask].str.zfill(ancho)
    return s


def normaliza_ids(
    df: pd.DataFrame,
    mapa: dict[str, int | None] | None = None,
) -> pd.DataFrame:
    """
    Normaliza columnas clave con el ancho correspondiente.

    Convención aplicada al proyecto PNIE:
        cod_local: 6 dígitos (puede haber 5-7 en la práctica)
        cod_mod:   7 dígitos
        cui:       7 dígitos
        anexo:     1 dígito (0/1/2...)
    """
    if mapa is None:
        mapa = {"cod_local": 6, "cod_mod": 7, "cui": 7, "anexo": None}
    df = df.copy()
    for col, ancho in mapa.items():
        if col in df.columns:
            df[col] = normaliza_id(df[col], ancho=ancho)
    return df


def normaliza_sino(serie: pd.Series) -> pd.Series:
    s = serie.astype("string").str.strip().str.upper()
    return s.map(MAPA_SINO).astype("Int8")


# ──────────────────────────────────────────────────────────────────────────
# Capa 4 - Duplicados
# ──────────────────────────────────────────────────────────────────────────

def reporta_duplicados(df: pd.DataFrame, claves: list[str]) -> dict:
    claves_existentes = [c for c in claves if c in df.columns]
    dup_exactos = int(df.duplicated().sum())
    dup_clave = (
        int(df.duplicated(subset=claves_existentes, keep=False).sum())
        if claves_existentes else 0
    )
    return {
        "filas": len(df),
        "duplicados_exactos": dup_exactos,
        "duplicados_por_clave": dup_clave,
        "claves_usadas": claves_existentes,
    }


def marca_duplicados(df: pd.DataFrame, claves: list[str]) -> pd.DataFrame:
    """Agrega columna flag_dup_clave sin eliminar filas."""
    df = df.copy()
    claves_existentes = [c for c in claves if c in df.columns]
    if claves_existentes:
        df["flag_dup_clave"] = df.duplicated(
            subset=claves_existentes, keep=False
        ).astype("Int8")
    else:
        df["flag_dup_clave"] = 0
    return df


# ──────────────────────────────────────────────────────────────────────────
# Capa 5 - Utilidad: explotar lista de códigos modulares
# ──────────────────────────────────────────────────────────────────────────

def explota_cod_mod_lista(
    df: pd.DataFrame,
    col_origen: str,
    col_cod_mod: str = "cod_mod",
    col_prefijo: str = "pref_modalidad",
) -> pd.DataFrame:
    """
    Convierte una fila con '[A2 - 0257048] [E1 - 1735752]' en dos filas,
    una por cada código modular extraído.
    Crea además la columna `col_prefijo` con el código de modalidad (A2, E1...).
    """
    df = df.copy()
    def parse(cell):
        if pd.isna(cell):
            return []
        return RE_COD_MOD_LISTA.findall(str(cell))

    df["_parsed"] = df[col_origen].map(parse)
    df = df.explode("_parsed", ignore_index=True)
    df[col_prefijo] = df["_parsed"].map(lambda x: x[0] if isinstance(x, tuple) else pd.NA)
    df[col_cod_mod] = df["_parsed"].map(lambda x: x[1] if isinstance(x, tuple) else pd.NA)
    df = df.drop(columns="_parsed")
    return df


# ──────────────────────────────────────────────────────────────────────────
# Capa 7 - Ficha de calidad
# ──────────────────────────────────────────────────────────────────────────

def ficha_calidad(df: pd.DataFrame, nombre: str) -> pd.DataFrame:
    filas = []
    n = len(df)
    for c in df.columns:
        s = df[c]
        filas.append({
            "base": nombre,
            "variable": c,
            "tipo": str(s.dtype),
            "n_no_nulos": int(s.notna().sum()),
            "pct_completitud": round(s.notna().mean() * 100, 2) if n else 0,
            "n_unicos": int(s.nunique(dropna=True)),
        })
    return pd.DataFrame(filas)


# ──────────────────────────────────────────────────────────────────────────
# Pipeline estándar
# ──────────────────────────────────────────────────────────────────────────

def limpieza_estandar(
    df: pd.DataFrame,
    llaves: list[str] | None = None,
    drop_exactos: bool = True,
) -> tuple[pd.DataFrame, dict]:
    """
    Pipeline base: estandariza columnas, limpia strings, normaliza IDs,
    elimina duplicados exactos, marca duplicados por clave y retorna
    (df_limpio, reporte_dict).
    """
    if llaves is None:
        llaves = ["cod_local", "cod_mod", "cui"]

    df = estandariza_columnas(df)
    df = limpia_strings(df)
    df = normaliza_ids(df)

    n0 = len(df)
    if drop_exactos:
        df = df.drop_duplicates().reset_index(drop=True)
    n1 = len(df)

    df = marca_duplicados(df, llaves)
    rep = reporta_duplicados(df, llaves)
    rep["duplicados_exactos_eliminados"] = n0 - n1
    return df, rep


def guarda(df: pd.DataFrame, ruta: Path, formato: str = "parquet") -> None:
    ruta = Path(ruta)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    if formato == "parquet":
        df.to_parquet(ruta.with_suffix(".parquet"), index=False)
    elif formato == "csv":
        df.to_csv(ruta.with_suffix(".csv"), index=False, encoding="utf-8-sig")
    elif formato == "xlsx":
        df.to_excel(ruta.with_suffix(".xlsx"), index=False)
    else:
        raise ValueError(f"Formato no soportado: {formato}")
