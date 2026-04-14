"""
01_limpiar_pronied.py
─────────────────────
Consolida las bases PRONIED en 4 sub-bases listas para cruzar con
las demás entidades (GR/GL, ANIN, PEIP, UE118, FONCODES).

Entrada:
    Archivos xlsx en la raíz del proyecto:
        - UGEO-ACTUALIZACION_ARCHIVO_10.04.26_VF.xlsx
        - 2026.03.17_UGSC.xlsx
        - 2026.03.18 UGME.xlsx
        - 2026.03.19 UGM.xlsx
        - 2026.03.30 Zonales_UZ.xlsx
        - 2026.03.31 UGEO-UGME-UGM.xlsx

Salida (en 03_output/):
    - PRONIED_INVERSIONES.parquet        (UGEO + UGSC)
    - PRONIED_MODULAR_EQUIP.parquet      (UGME - 3 hojas)
    - PRONIED_MANTENIMIENTO.parquet      (UGM - todas las hojas)
    - PRONIED_INSPECCIONES_AT.parquet    (UZ + OTROS_AT)
    - calidad_PRONIED_<base>.csv         (ficha de calidad por base)
    - reporte_duplicados_PRONIED.csv     (resumen de duplicados)

Llaves garantizadas en todas las sub-bases (cuando existen en origen):
    cui, cod_local, cod_mod, anexo

No aplica aún criterios de negocio: solo limpieza técnica y consolidación.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Permite importar utils/ cuando se ejecuta el script directamente
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils.limpieza_base import (
    estandariza_columnas,
    explota_cod_mod_lista,
    ficha_calidad,
    guarda,
    limpieza_estandar,
    normaliza_ids,
)

# ──────────────────────────────────────────────────────────────────────────
# Rutas
# ──────────────────────────────────────────────────────────────────────────
PROY = Path(__file__).resolve().parent.parent
RAW = PROY
OUT = PROY / "03_output"
TMP = PROY / "04_temporal"
for p in (OUT, TMP):
    p.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────────────────────────────────────────────────
# Configuración de cada hoja (archivo, hoja, fila de encabezado base 0)
# ──────────────────────────────────────────────────────────────────────────
# Para UGM (Mantenimiento / Accesibilidad) el encabezado real está en la
# fila 3 (índice 3) porque fila 2 trae un encabezado "de agrupación".
HOJAS = {
    # INVERSIONES
    "UGEO":       ("UGEO-ACTUALIZACION_ARCHIVO_10.04.26_VF.xlsx", "UGEO", 2),
    "UGSC_AT":    ("2026.03.17_UGSC.xlsx", "ASITEC-SIAT", 2),
    "UGSC_SEG":   ("2026.03.17_UGSC.xlsx", "SEGUIMIENTO DE PI FINANCIADOS ", 2),
    # MODULAR / EQUIPAMIENTO
    "UGME_SM":    ("2026.03.18 UGME.xlsx", "SISTEMAS MODULARES", 2),
    "UGME_MB":    ("2026.03.18 UGME.xlsx", "MOBILIARIO Y EQUIPAMIENTO", 2),
    "UGME_PC":    ("2026.03.18 UGME.xlsx", "PLAN DE CONSERVACIÓN MODULAR", 2),
    # MANTENIMIENTO
    "UGM_ACOND":  ("2026.03.19 UGM.xlsx", "ACONDICIONAMIENTO", 2),
    "UGM_MT25":   ("2026.03.19 UGM.xlsx", "MANTENIMIENTO 2025", 3),
    "UGM_MT26":   ("2026.03.19 UGM.xlsx", "MANTENIMIENTO 2026", 3),
    # Hojas de accesibilidad: se completan dinámicamente al descubrir las hojas
    # INSPECCIONES / ASESORAMIENTO
    "UZ_INSP":    ("2026.03.30 Zonales_UZ.xlsx", "INSPECCIONES", 2),
    "UZ_ASE":     ("2026.03.30 Zonales_UZ.xlsx", "ASESORAMIENTO", 2),
    "OTROS_AT":   ("2026.03.31 UGEO-UGME-UGM.xlsx", "OTROS_ASESORAMIENTO_AT", 2),
}


def lee_hoja(archivo: str, hoja: str, header: int) -> pd.DataFrame:
    ruta = RAW / archivo
    df = pd.read_excel(ruta, sheet_name=hoja, header=header)
    df = df.dropna(how="all")
    # elimina filas que son el mismo encabezado repetido
    df = df[df.iloc[:, 0].astype(str).str.lower() != "n°"]
    return df


# ──────────────────────────────────────────────────────────────────────────
# 1. PRONIED_INVERSIONES  (UGEO + UGSC)
# ──────────────────────────────────────────────────────────────────────────

def construye_inversiones() -> pd.DataFrame:
    partes = []

    # UGEO
    arc, sh, h = HOJAS["UGEO"]
    ugeo = lee_hoja(arc, sh, h)
    ugeo = estandariza_columnas(ugeo)
    ugeo = ugeo.rename(columns={
        "codigos_modulares_intervenidos_dividir_por": "cod_mod_lista",
        "monto_de_inversion_s": "monto",
        "tipo_de_inversion_pi_regular_ioarr_u_oxi": "tipo_inversion",
        "fase_de_la_obra": "fase",
        "etapa_de_la_obra": "etapa",
        "fecha_de_inicio_de_la_obra_o_estimada": "fecha_inicio",
        "fecha_de_culminacion_de_obra_o_estimada": "fecha_culminacion",
        "fecha_de_recepcion_de_obra_o_estimada": "fecha_recepcion",
        "fecha_de_entrega_de_obra_o_estimada": "fecha_entrega",
        "fecha_de_inauguracion_o_estimada": "fecha_inauguracion",
        "i_e": "nom_local",
    })
    ugeo["unidad_pronied"] = "UGEO"
    ugeo["fuente_archivo"] = arc
    ugeo["fuente_hoja"] = sh
    partes.append(ugeo)

    # UGSC - ASITEC-SIAT (AT para elaboración/aprobación de ET)
    arc, sh, h = HOJAS["UGSC_AT"]
    ugsc_at = lee_hoja(arc, sh, h)
    ugsc_at = estandariza_columnas(ugsc_at)
    ugsc_at = ugsc_at.rename(columns={
        "monto_de_inversion_s": "monto",
        "cantidad_de_locales_educativos_intervenidos_por_el_pi": "n_locales",
        "fecha_de_aprobacion_del_expediente_tecnico_et_estimado": "fecha_aprob_et",
        "fecha_de_caducidad_del_et": "fecha_caducidad_et",
    })
    ugsc_at["unidad_pronied"] = "UGSC"
    ugsc_at["tipo_inversion"] = "PI - ASISTENCIA TECNICA ET"
    ugsc_at["fuente_archivo"] = arc
    ugsc_at["fuente_hoja"] = sh
    partes.append(ugsc_at)

    # UGSC - SEGUIMIENTO PI FINANCIADOS (GR/GL financiados por GN)
    arc, sh, h = HOJAS["UGSC_SEG"]
    ugsc_seg = lee_hoja(arc, sh, h)
    ugsc_seg = estandariza_columnas(ugsc_seg)
    ugsc_seg = ugsc_seg.rename(columns={
        "monto_de_inversion_s": "monto",
        "transferencia_total_s": "monto_transferido",
        "cantidad_de_locales_educativos_intervenidos_por_el_pi": "n_locales",
        "ano_de_la_primera_transferencia": "ano_trans_ini",
        "ano_de_la_ultima_transferencia": "ano_trans_fin",
    })
    ugsc_seg["unidad_pronied"] = "UGSC"
    ugsc_seg["tipo_inversion"] = "PI - SEGUIMIENTO FINANCIAMIENTO GN"
    ugsc_seg["fuente_archivo"] = arc
    ugsc_seg["fuente_hoja"] = sh
    partes.append(ugsc_seg)

    df = pd.concat(partes, ignore_index=True, sort=False)
    df = normaliza_ids(df)  # normaliza cui, cod_local, cod_mod, anexo si existen
    df["cod_mod"] = df.get("cod_mod", pd.NA)
    df["anexo"] = df.get("anexo", pd.NA)
    return df


# ──────────────────────────────────────────────────────────────────────────
# 2. PRONIED_MODULAR_EQUIP  (UGME x3)
# ──────────────────────────────────────────────────────────────────────────

def construye_modular() -> pd.DataFrame:
    partes = []
    etiquetas = {
        "UGME_SM": "SISTEMAS MODULARES",
        "UGME_MB": "MOBILIARIO Y EQUIPAMIENTO",
        "UGME_PC": "PLAN CONSERVACION MODULAR",
    }
    for k, tipo in etiquetas.items():
        arc, sh, h = HOJAS[k]
        d = lee_hoja(arc, sh, h)
        d = estandariza_columnas(d)
        d["unidad_pronied"] = "UGME"
        d["tipo_intervencion"] = tipo
        d["fuente_archivo"] = arc
        d["fuente_hoja"] = sh
        partes.append(d)
    df = pd.concat(partes, ignore_index=True, sort=False)
    df = normaliza_ids(df)
    df["anexo"] = df.get("anexo", pd.NA)
    return df


# ──────────────────────────────────────────────────────────────────────────
# 3. PRONIED_MANTENIMIENTO  (UGM - Acondicionamiento + Mantenimiento + Accesibilidad)
# ──────────────────────────────────────────────────────────────────────────

def construye_mantenimiento() -> pd.DataFrame:
    arc_ugm = "2026.03.19 UGM.xlsx"
    xl = pd.ExcelFile(RAW / arc_ugm)

    partes = []

    # ACONDICIONAMIENTO (por obra, cod_local sin lista)
    acond = lee_hoja(arc_ugm, "ACONDICIONAMIENTO", 2)
    acond = estandariza_columnas(acond)
    acond = acond.rename(columns={
        "monto_contractual_s": "monto",
        "tipo_de_intervencion_mantenimiento_correctivo_confort_termico_o_residencias_estudiantiles": "tipo_intervencion",
        "fecha_estimada_de_entrega": "fecha_entrega",
        "local_escolar": "nom_local",
        "fase_del_proceso": "fase",
        "etapa_del_proceso": "etapa",
    })
    acond["periodo"] = acond.get("ano", pd.NA)
    acond["modalidad"] = "ACONDICIONAMIENTO"
    acond["unidad_pronied"] = "UGM"
    acond["fuente_archivo"] = arc_ugm
    acond["fuente_hoja"] = "ACONDICIONAMIENTO"
    partes.append(acond)

    # MANTENIMIENTO 2025 y 2026 (requieren explode de cod_mod)
    for sh, periodo in [("MANTENIMIENTO 2025", "2025-1"), ("MANTENIMIENTO 2026", "2026-1")]:
        d = lee_hoja(arc_ugm, sh, 3)
        d = estandariza_columnas(d)
        col_origen = "codigos_modulares_del_local_educativo"
        if col_origen in d.columns:
            d = explota_cod_mod_lista(d, col_origen=col_origen)
        d = d.rename(columns={
            "monto_asignado_total_s": "monto_asignado",
            "monto_transferido_s": "monto_transferido",
            "monto_asignado_para_mantenimiento_s": "monto_mant",
            "monto_asignado_para_rutas_solidarias_s": "monto_rutas",
            "monto_total_de_la_fam": "monto_fam",
            "monto_total_de_la_dg": "monto_dg",
            "estado_de_la_ficha_de_acciones_de_mantenimiento_fam": "estado_fam",
            "estado_de_la_declaracion_de_gastos_dg": "estado_dg",
        })
        d["periodo"] = periodo
        d["modalidad"] = "MANTENIMIENTO_SUBVENCION"
        d["unidad_pronied"] = "UGM"
        d["fuente_archivo"] = arc_ugm
        d["fuente_hoja"] = sh
        partes.append(d)

    # ACCESIBILIDAD - recorrer todas las hojas 'ACCESIBILIDAD *'
    for sh in xl.sheet_names:
        if sh.startswith("ACCESIBILIDAD"):
            d = lee_hoja(arc_ugm, sh, 3)
            d = estandariza_columnas(d)
            col_origen = "codigos_modulares_del_local_educativo"
            if col_origen in d.columns:
                d = explota_cod_mod_lista(d, col_origen=col_origen)
            d = d.rename(columns={
                "monto_asignado_total_s": "monto_asignado",
                "monto_transferido_s": "monto_transferido",
                "monto_total_de_la_faa": "monto_faa",
                "monto_total_de_la_dg": "monto_dg",
                "estado_de_la_ficha_de_acciones_de_acondicionamiento_faa": "estado_faa",
                "estado_de_la_declaracion_de_gastos_dg": "estado_dg",
            })
            # el nombre "ACCESIBILIDAD 2025-2" ya incluye periodo
            d["periodo"] = sh.replace("ACCESIBILIDAD ", "").strip()
            d["modalidad"] = "ACCESIBILIDAD_SUBVENCION"
            d["unidad_pronied"] = "UGM"
            d["fuente_archivo"] = arc_ugm
            d["fuente_hoja"] = sh
            partes.append(d)

    df = pd.concat(partes, ignore_index=True, sort=False)
    df = normaliza_ids(df)
    df["anexo"] = df.get("anexo", pd.NA)
    return df


# ──────────────────────────────────────────────────────────────────────────
# 4. PRONIED_INSPECCIONES_AT  (UZ + OTROS_AT)
# ──────────────────────────────────────────────────────────────────────────

def construye_inspecciones_at() -> pd.DataFrame:
    partes = []

    # UZ - INSPECCIONES (tiene cod_local)
    arc, sh, h = HOJAS["UZ_INSP"]
    d = lee_hoja(arc, sh, h)
    d = estandariza_columnas(d)
    d = d.rename(columns={
        "codigo_local": "cod_local",
        "nombre_de_la_unidad_zonal": "unidad_zonal",
        "fecha_de_inspeccion": "fecha",
    })
    d["tipo_actividad"] = "INSPECCION"
    d["unidad_pronied"] = "UZ"
    d["fuente_archivo"] = arc
    d["fuente_hoja"] = sh
    partes.append(d)

    # UZ - ASESORAMIENTO (sin cod_local)
    arc, sh, h = HOJAS["UZ_ASE"]
    d = lee_hoja(arc, sh, h)
    d = estandariza_columnas(d)
    d["tipo_actividad"] = "ASESORAMIENTO_UZ"
    d["unidad_pronied"] = "UZ"
    d["fuente_archivo"] = arc
    d["fuente_hoja"] = sh
    partes.append(d)

    # OTROS_AT (UGEO-UGME-UGM)
    arc, sh, h = HOJAS["OTROS_AT"]
    d = lee_hoja(arc, sh, h)
    d = estandariza_columnas(d)
    d["tipo_actividad"] = "ASESORAMIENTO_OTROS"
    d["unidad_pronied"] = "OTROS"
    d["fuente_archivo"] = arc
    d["fuente_hoja"] = sh
    partes.append(d)

    df = pd.concat(partes, ignore_index=True, sort=False)
    df = normaliza_ids(df)
    for c in ("cui", "cod_mod", "anexo"):
        if c not in df.columns:
            df[c] = pd.NA
    return df


# ──────────────────────────────────────────────────────────────────────────
# Orquestador
# ──────────────────────────────────────────────────────────────────────────

def main():
    print(f"\n[INICIO] Limpieza PRONIED - salida en {OUT}\n")
    resumenes = []

    bases = {
        "PRONIED_INVERSIONES":   construye_inversiones(),
        "PRONIED_MODULAR_EQUIP": construye_modular(),
        "PRONIED_MANTENIMIENTO": construye_mantenimiento(),
        "PRONIED_INSPECCIONES_AT": construye_inspecciones_at(),
    }

    for nombre, df in bases.items():
        # Llaves a usar según base
        if "INSPECCIONES" in nombre:
            llaves = ["cod_local"]
        elif "INVERSIONES" in nombre:
            llaves = ["cui", "cod_local"]
        else:
            llaves = ["cod_local", "cod_mod"]

        df_limpio, rep = limpieza_estandar(df, llaves=llaves, drop_exactos=True)
        print(f"[{nombre}]  filas={rep['filas']:>6}  "
              f"dup_exactos_eliminados={rep['duplicados_exactos_eliminados']:>5}  "
              f"dup_por_clave={rep['duplicados_por_clave']:>5}  "
              f"claves={rep['claves_usadas']}")

        guarda(df_limpio, OUT / nombre, formato="parquet")
        ficha = ficha_calidad(df_limpio, nombre=nombre)
        ficha.to_csv(OUT / f"calidad_{nombre}.csv", index=False, encoding="utf-8-sig")

        rep["base"] = nombre
        resumenes.append(rep)

    pd.DataFrame(resumenes).to_csv(
        OUT / "reporte_duplicados_PRONIED.csv",
        index=False, encoding="utf-8-sig",
    )
    print(f"\n[FIN] Reporte de duplicados: {OUT / 'reporte_duplicados_PRONIED.csv'}")


if __name__ == "__main__":
    main()
