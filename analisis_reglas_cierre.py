"""
================================================================================
  ANÁLISIS DE LA BASE DE INVERSIONES Y FUIE PARA SUSTENTAR REGLAS DE DECISIÓN
  PNIE 2017-2028  |  Algoritmo PNIE 2017 + código NB07  +  propuesta DIPLAN11
--------------------------------------------------------------------------------
  Autor          : DIPLAN-DIGEIE / MINEDU
  Inputs         : 2026.04.13 Base de Inversiones.xlsx
                   Vinculaciones_compartido.xlsx        (CUI <-> cod_local)
                   FUIE24 procesado (variables NB03)    (LE_Brecha.dta o csv)
                   Cálculo de la brecha y orden de prioridad ... .xlsx
  Subcomponentes : GI1 (ct_st_d, ct_sp_d, ct_ri, ct_ic, ct_cp)
                   GI2 (ct_aad, ct_cad)
                   GI3 (ct_me, ct_ce, ct_ene)
                   GI4 (ct_ae, ct_acc, ct_amp, ct_rc, ct_sp_r, ct_ic_r)
                   GI5 (ct_st)         |   SAFIL (b_safil)
  Lógica         : 1) Reglas vigentes (Algoritmo 2017 / NB07).
                   2) Overlay propuesta DIPLAN11 (P1, P2, P3, P8, PB, PC, P9).
================================================================================
"""

from __future__ import annotations
import os
import re
from pathlib import Path
import numpy as np
import pandas as pd

# ------------------------------------------------------------------
# 0. Configuración
# ------------------------------------------------------------------
RAIZ        = Path(".")
F_INV       = RAIZ / "2026.04.13 Base de Inversiones.xlsx"
F_VINC      = RAIZ / "Vinculaciones_compartido.xlsx"
F_BRECHA    = RAIZ / "Cálculo de la brecha y orden de prioridad (al 29 de diciembre de 2025).xlsx"
F_FUIE      = RAIZ / "LE_Brecha_FUIE24.csv"      # exportado desde 8. CalcBrFUIE24
DIR_OUT     = RAIZ / "out_reglas"
DIR_OUT.mkdir(exist_ok=True)

# Umbrales (Algoritmo PNIE 2017 / NB07 - dic 2025)
UMB_AVANCE_CIERRE  = 0.85          # avance físico ≥ 85% activa cierre (NB04)
UMB_RATIO_SUST     = 0.70          # ratio Areasust/Areatech ≥ 70% → sust. total
UMB_DIPLAN_PARCIAL = 0.70          # P8 - área intervenida ≥ 70% para cierre total
UMB_DIPLAN_PB      = 0.30          # PB - monto_PI / costo_brecha < 30%
UMB_DIPLAN_PC_INF  = 0.30          # PC - rango "en proceso de cierre"
UMB_DIPLAN_PC_SUP  = 0.85          # PC - límite superior (sin F9)

# Plazos máximos por nivel de gobierno (P1)
PLAZO_GN  = 3
PLAZO_GRL = 5

# ------------------------------------------------------------------
# 1. Lectura de fuentes
# ------------------------------------------------------------------
def leer_inversiones(path: Path = F_INV) -> pd.DataFrame:
    """Lee el reporte del Banco de Inversiones (header en fila 5)."""
    df = pd.read_excel(path, sheet_name="Data", header=4)
    df.columns = [c.strip() for c in df.columns]
    # tipados clave
    for c in ("AVANCE_FISICO_F12B", "AVANCE_FISICO_F9",
              "COSTO_ACTUALIZADO_BI", "COSTO_INV_TOTAL_BI",
              "MONTO_PIM_BI", "DEV_ACUM_ANO_ACTUAL"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # plazo de ejecución (años) ≈ FEC_FIN_F8 - FEC_INI_F8
    if {"FEC_INI_F8", "FEC_FIN_F8"}.issubset(df.columns):
        df["FEC_INI_F8"] = pd.to_datetime(df["FEC_INI_F8"], errors="coerce")
        df["FEC_FIN_F8"] = pd.to_datetime(df["FEC_FIN_F8"], errors="coerce")
        df["plazo_anios"] = (df["FEC_FIN_F8"] - df["FEC_INI_F8"]).dt.days / 365.25
    # nivel de gobierno deducido de SECTOR/PLIEGO/UEP
    df["nivel_gob"] = np.where(
        df["NIVEL"].astype(str).str.upper().str.startswith("GN"), "GN",
        np.where(df["NIVEL"].astype(str).str.upper().str.startswith("GR"), "GR",
        np.where(df["NIVEL"].astype(str).str.upper().str.startswith("GL"), "GL", "NA"))
    )
    df["CODIGO_UNICO"] = df["CODIGO_UNICO"].astype(str).str.strip()
    return df


def leer_vinculaciones(path: Path = F_VINC) -> pd.DataFrame:
    """Carga el cruce CUI <-> cod_local (NB04 - Vinculaciones_compartido)."""
    df = pd.read_excel(path, sheet_name="Vinculaciones")
    norm = (lambda s: re.sub(r"[^a-z0-9]+", "_",
            s.strip().lower()
             .replace("á", "a").replace("é", "e").replace("í", "i")
             .replace("ó", "o").replace("ú", "u").replace("ñ", "n")))
    df.columns = [norm(c) for c in df.columns]
    rename = {}
    for col in df.columns:
        if col == "cui":                          rename[col] = "CODIGO_UNICO"
        elif col in ("codigo_local", "cod_local", "codlocal"):
            rename[col] = "cod_local"
        elif col in ("codigo_modular", "cod_modular", "cod_mod"):
            rename[col] = "cod_mod"
    df = df.rename(columns=rename)
    df["CODIGO_UNICO"] = df["CODIGO_UNICO"].astype(str).str.strip()
    df["cod_local"]   = df["cod_local"].astype(str).str.zfill(6)
    return df[["CODIGO_UNICO", "cod_local"]].drop_duplicates()


def leer_fuie(path: Path = F_FUIE) -> pd.DataFrame:
    """
    Carga las variables FUIE24 a nivel cod_local generadas por
    `8. CalcBrFUIE24_250730.do`.  Variables esperadas:
        cod_local, areatech, areadem, areasust, areari, arearc,
        ratiodem, zona_sismica (Ame 1-4), zona_bio, sistest,
        ct_st_d, ct_sp_d, ct_ri, ct_ic, ct_cp, ct_aad, ct_cad,
        ct_me, ct_ce, ct_ene, ct_ae, ct_acc, ct_amp, ct_rc,
        ct_sp_r, ct_ic_r, ct_st, b_safil,
        gestion_zona (U/R), gue (1/0), saneado_margesi (1/0)
    """
    if not path.exists():
        # placeholder: scaffold para que el script corra aunque el .csv no exista
        return pd.DataFrame(columns=[
            "cod_local", "areatech", "areadem", "areasust", "ratiodem",
            "zona_sismica", "zona_bio", "sistest", "gestion_zona", "gue",
            "saneado_margesi",
            "ct_st_d", "ct_sp_d", "ct_ri", "ct_ic", "ct_cp",
            "ct_aad", "ct_cad", "ct_me", "ct_ce", "ct_ene",
            "ct_ae", "ct_acc", "ct_amp", "ct_rc",
            "ct_sp_r", "ct_ic_r", "ct_st", "b_safil",
        ])
    df = pd.read_csv(path, dtype={"cod_local": str})
    df["cod_local"] = df["cod_local"].astype(str).str.zfill(6)
    return df


# ------------------------------------------------------------------
# 2. Tipificación de la intervención (T1, T2, T3a, T3b, T4a, T4b, T5, T6)
# ------------------------------------------------------------------
def clasificar_tipo_intervencion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Asigna T1..T6 según DES_TIPO_FORMATO + TIPO_IOARR + DES_BRECHA + UEI.
        T1 : PI Integral (formato 7 / 8 / 12B con obra integral)
        T2 : IOARR
        T3a: Módulo definitivo (ANIN, módulos prefabricados)
        T3b: Plan Selva / PEIP-EB (estado transitorio)
        T4a: Mantenimiento dotación equipos (UGME)
        T4b: Mantenimiento correctivo (PRONIED programa Mto.)
        T5 : Habilitante (saneamiento, accesibilidad puntual)
        T6 : Emergencia (D.U. / contingente)
    """
    fmt   = df["DES_TIPO_FORMATO"].astype(str).str.upper()
    ioarr = df["TIPO_IOARR"].astype(str).str.upper()
    nom   = df["NOMBRE_INVERSION"].astype(str).str.upper()
    uei   = df["UEI"].astype(str).str.upper()

    cond = [
        fmt.str.contains("IOARR") | ioarr.str.len().gt(3),
        nom.str.contains("PLAN SELVA|PEIP-EB|PEIP EB"),
        nom.str.contains("EMERGENC|D\\.U|DECRETO DE URGENCIA|CONTINGENTE"),
        nom.str.contains("MANT(ENIMIENT)?O CORRECTIV|MANTENIMIENTO CORRECTIVO"),
        uei.str.contains("UGME") | nom.str.contains("MOBILIARIO|EQUIPAMIENTO"),
        nom.str.contains("M[OÓ]DULO|ANIN|PREFABRICAD"),
        nom.str.contains("SANEAMIENTO F[IÍ]SICO|HABILIT|ACCESIBILIDAD"),
    ]
    etiqueta = ["T2", "T3b", "T6", "T4b", "T4a", "T3a", "T5"]
    df["tipo_int"] = np.select(cond, etiqueta, default="T1")
    return df


# ------------------------------------------------------------------
# 3. Reglas vigentes (Algoritmo 2017 + NB07)
# ------------------------------------------------------------------
def aplicar_reglas_vigentes(d: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve un DataFrame con las banderas GIx_y por CUI/cod_local que
    activan ct_XX_post = 0 según el algoritmo vigente.
    """
    # avance "elegible" para cierre
    avance = d[["AVANCE_FISICO_F12B", "AVANCE_FISICO_F9"]].max(axis=1)
    eleg   = (avance >= UMB_AVANCE_CIERRE) | (d["CERRADO_F9"].astype(str) == "SI")

    # ---------- GI1 ----------
    d["GI1_1"] = ((d["tipo_int"].isin(["T1", "T3b"])) &
                  (d["ratiodem"].fillna(0) >= UMB_RATIO_SUST) & eleg).astype(int)
    d["GI1_2"] = ((d["tipo_int"].isin(["T1", "T2"])) &
                  (d["ratiodem"].fillna(0) <  UMB_RATIO_SUST) &
                  (d["zona_sismica"].fillna(0).isin([3, 4])) & eleg).astype(int)
    d["GI1_3"] = (d["tipo_int"].isin(["T6", "T3b"])).astype(int)              # estado transitorio
    d["GI1_4"] = ((d["tipo_int"].isin(["T1", "T2"])) &
                  (d["gestion_zona"] == "U") & eleg).astype(int)              # cerco perimétrico
    # ---------- GI2 ----------
    d["GI2_1"] = ((d["tipo_int"].isin(["T1", "T2", "T3b"])) & eleg).astype(int)   # acceso ag/sn
    d["GI2_2"] = ((d["tipo_int"].isin(["T1", "T2"])) & eleg).astype(int)          # calidad ag/sn
    # ---------- GI3 ----------
    d["GI3_1"] = ((d["tipo_int"].isin(["T1", "T2"])) & eleg).astype(int)          # calidad energía
    d["GI3_2"] = ((d["tipo_int"].isin(["T1", "T3b"])) & eleg).astype(int)         # mobiliario
    d["GI3_3"] = (d["tipo_int"] == "T4b").astype(int)                              # mto. correctivo OE4
    # ---------- GI4 ----------
    d["GI4_1"] = ((d["tipo_int"].isin(["T1", "T2"])) &
                  (d["zona_sismica"].fillna(0).isin([3, 4])) & eleg).astype(int)  # reposición sp
    d["GI4_2"] = ((d["tipo_int"].isin(["T1", "T2"])) & eleg).astype(int)          # acceso eléctrico
    d["GI4_3"] = ((d["tipo_int"].isin(["T1", "T2"])) & eleg).astype(int)          # accesibilidad
    d["GI4_4"] = ((d["tipo_int"].isin(["T1", "T2"])) & eleg).astype(int)          # ampliación de áreas
    # ---------- GI5 (sustitución total) ----------
    d["GI5_1"] = ((d["tipo_int"].isin(["T1", "T3b"])) &
                  (d["ratiodem"].fillna(0) >= UMB_RATIO_SUST) & eleg).astype(int)
    # ---------- SAFIL ----------
    d["SAFIL_1"] = (d.get("saneado_margesi", 0).fillna(0) == 1).astype(int)
    # arrastre de GI5 → cierra GI1, GI2, GI3, GI4
    arrastre = d["GI5_1"] == 1
    for g in ("GI1_1", "GI1_2", "GI1_4",
              "GI2_1", "GI2_2",
              "GI3_1", "GI3_2",
              "GI4_1", "GI4_2", "GI4_3", "GI4_4"):
        d.loc[arrastre, g] = 1
    return d


def aplicar_costos_post(d: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula ct_XX_post (cero si la bandera correspondiente está activa).
    Excepción documentada: GI1_3 (intervención contingente) NO pone
    ct_ic_post = 0  →  la brecha persiste hasta obra definitiva.
    """
    mapa = {
        "ct_st_d": "GI1_1",
        "ct_sp_d": "GI1_2",
        "ct_ri":   "GI1_2",
        "ct_cp":   "GI1_4",
        "ct_aad":  "GI2_1",
        "ct_cad":  "GI2_2",
        "ct_me":   "GI3_2",
        "ct_ce":   "GI3_1",
        "ct_ene":  "GI3_3",      # solo OE4
        "ct_ae":   "GI4_2",
        "ct_acc":  "GI4_3",
        "ct_amp":  "GI4_4",
        "ct_rc":   "GI1_2",
        "ct_sp_r": "GI4_1",
        "ct_ic_r": "GI1_3",
        "ct_st":   "GI5_1",
        "b_safil": "SAFIL_1",
    }
    for c, flag in mapa.items():
        if c in d.columns:
            d[f"{c}_post"] = np.where(d[flag] == 1, 0.0, d[c])
    # NB07 explícito: la intervención contingente NO cierra ct_ic
    if "ct_ic" in d.columns:
        d["ct_ic_post"] = d["ct_ic"]
    return d


# ------------------------------------------------------------------
# 4. Overlay DIPLAN11  (P1, P2, P3, P8, PB, PC, P9)
# ------------------------------------------------------------------
def aplicar_diplan11(d: pd.DataFrame, fuie_serie: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Reglas propuestas que MODIFICAN las vigentes.
        P1 : plazo > 3a (GN) o > 5a (GR/GL)            → requiere FUIE
        P2 : ≥ 2 CUI con mismo tipo en mismo local     → solo el de mayor avance
        P3 : FUIE_t adecuado y FUIE_{t-1} ausente      → cierre por FUIE directo
        P8 : área intervenida < 70% de la necesidad    → cierre parcial proporcional
        PB : monto_PI / costo_brecha < 30%             → no activa cierre total
        PC : 30% ≤ avance < 85% sin F9                 → "en proceso de cierre"
        P9 : 2 años consecutivos FUIE crítico post-cierre → cierre se revierte
    `fuie_serie` (opcional) debe contener cod_local, anio, condicion (string)
    """
    avance = d[["AVANCE_FISICO_F12B", "AVANCE_FISICO_F9"]].max(axis=1).fillna(0)

    # P1 ------------------------------------------------------------
    plazo_max = np.where(d["nivel_gob"].eq("GN"), PLAZO_GN, PLAZO_GRL)
    d["alerta_P1"] = ((d.get("plazo_anios", 0).fillna(0) > plazo_max)).astype(int)

    # P2 - duplicidad por (cod_local, tipo_int) -----------------------
    if "cod_local" in d.columns:
        grp = d.groupby(["cod_local", "tipo_int"], dropna=False)
        d["n_cui_local_tipo"] = grp["CODIGO_UNICO"].transform("nunique")
        d["max_avance_grp"]   = grp["AVANCE_FISICO_F12B"].transform("max")
        d["alerta_P2"] = ((d["n_cui_local_tipo"] > 1) &
                          (d["AVANCE_FISICO_F12B"] < d["max_avance_grp"])).astype(int)
    else:
        d["alerta_P2"] = 0

    # P3 - cierre por FUIE directo (sin CUI educativo) ---------------
    d["cierre_P3"] = 0
    if fuie_serie is not None and {"cod_local", "anio", "condicion"}.issubset(fuie_serie.columns):
        fs = (fuie_serie
              .assign(condicion=lambda x: x["condicion"].astype(str).str.upper())
              .pivot_table(index="cod_local", columns="anio",
                           values="condicion", aggfunc="last"))
        if fs.shape[1] >= 2:
            ult, prev = fs.columns[-1], fs.columns[-2]
            mask = (fs[ult] == "ADECUADO") & (fs[prev] == "AUSENTE")
            d.loc[d["cod_local"].isin(fs.index[mask]), "cierre_P3"] = 1

    # P8 - cierre parcial proporcional --------------------------------
    if {"area_intervenida", "area_necesidad"}.issubset(d.columns):
        ratio_inter = d["area_intervenida"] / d["area_necesidad"].replace(0, np.nan)
        d["pct_cierre_P8"] = ratio_inter.clip(0, 1).fillna(0)
        d["alerta_P8"] = (ratio_inter < UMB_DIPLAN_PARCIAL).astype(int)
    else:
        d["pct_cierre_P8"] = np.nan
        d["alerta_P8"]    = 0

    # PB - coherencia presupuestaria ---------------------------------
    if "costo_brecha_subcomp" in d.columns:
        ratio_pb = d["COSTO_INV_TOTAL_BI"] / d["costo_brecha_subcomp"].replace(0, np.nan)
        d["alerta_PB"] = (ratio_pb < UMB_DIPLAN_PB).astype(int)
    else:
        d["alerta_PB"] = 0

    # PC - estado "en proceso de cierre" -----------------------------
    sin_f9 = d["TIENE_F9"].astype(str).str.upper().ne("SI")
    d["estado_PC"] = np.where(
        (avance >= UMB_DIPLAN_PC_INF) & (avance < UMB_DIPLAN_PC_SUP) & sin_f9,
        "EN_PROCESO_DE_CIERRE", "")

    # P9 - reversión por dos FUIE consecutivos críticos --------------
    d["alerta_P9"] = 0
    if fuie_serie is not None and {"cod_local", "anio", "condicion"}.issubset(fuie_serie.columns):
        fs = (fuie_serie
              .assign(condicion=lambda x: x["condicion"].astype(str).str.upper())
              .pivot_table(index="cod_local", columns="anio",
                           values="condicion", aggfunc="last"))
        if fs.shape[1] >= 2:
            ult, prev = fs.columns[-1], fs.columns[-2]
            mask = (fs[ult] == "CRITICO") & (fs[prev] == "CRITICO")
            d.loc[d["cod_local"].isin(fs.index[mask]), "alerta_P9"] = 1

    # Reconciliación: si PB o (avance < 0.85 y sin F9) → no cerrar
    cerrar = (avance >= UMB_AVANCE_CIERRE) | (d["CERRADO_F9"].astype(str) == "SI")
    d["cierre_efectivo"] = (cerrar & (d["alerta_PB"] == 0)).astype(int)
    d.loc[d["alerta_P9"] == 1, "cierre_efectivo"] = 0
    return d


# ------------------------------------------------------------------
# 5. Reportes diagnósticos
# ------------------------------------------------------------------
def reporte_por_subcomponente(d: pd.DataFrame) -> pd.DataFrame:
    """% de CUI con cierre por subcomponente y montos asociados."""
    cols = ["GI1_1", "GI1_2", "GI1_3", "GI1_4",
            "GI2_1", "GI2_2",
            "GI3_1", "GI3_2", "GI3_3",
            "GI4_1", "GI4_2", "GI4_3", "GI4_4",
            "GI5_1", "SAFIL_1"]
    res = []
    n = len(d)
    for c in cols:
        if c not in d.columns:
            continue
        n_cierre = int(d[c].sum())
        monto    = d.loc[d[c] == 1, "COSTO_ACTUALIZADO_BI"].sum()
        res.append({"subcomp": c,
                    "n_cui": n_cierre,
                    "pct_cui": round(100 * n_cierre / max(n, 1), 2),
                    "monto_asociado": round(float(monto), 2)})
    return pd.DataFrame(res)


def reporte_alertas_diplan(d: pd.DataFrame) -> pd.DataFrame:
    cols = ["alerta_P1", "alerta_P2", "alerta_P8",
            "alerta_PB", "alerta_P9", "cierre_P3"]
    res = []
    for c in cols:
        if c not in d.columns:
            continue
        n = int(d[c].sum())
        res.append({"regla": c, "n_cui": n,
                    "pct_cui": round(100 * n / max(len(d), 1), 2)})
    res.append({"regla": "estado_PC=EN_PROCESO",
                "n_cui": int((d["estado_PC"] == "EN_PROCESO_DE_CIERRE").sum()),
                "pct_cui": round(100 * (d["estado_PC"] == "EN_PROCESO_DE_CIERRE").mean(), 2)})
    return pd.DataFrame(res)


# ------------------------------------------------------------------
# 6. Pipeline
# ------------------------------------------------------------------
def main() -> None:
    print(">> Leyendo Banco de Inversiones ...")
    inv  = leer_inversiones()
    print(f"   filas: {len(inv):,}  columnas: {inv.shape[1]}")

    print(">> Leyendo Vinculaciones CUI <-> cod_local ...")
    vinc = leer_vinculaciones()

    print(">> Leyendo FUIE24 (LE_Brecha_FUIE24) ...")
    fuie = leer_fuie()

    print(">> Cruce CUI-cod_local-FUIE ...")
    base = (inv
            .merge(vinc, on="CODIGO_UNICO", how="left")
            .merge(fuie, on="cod_local",   how="left", suffixes=("", "_fuie")))

    print(">> Clasificando tipo de intervención ...")
    base = clasificar_tipo_intervencion(base)

    print(">> Aplicando reglas vigentes (Algoritmo 2017 + NB07) ...")
    base = aplicar_reglas_vigentes(base)
    base = aplicar_costos_post(base)

    print(">> Aplicando overlay DIPLAN11 ...")
    base = aplicar_diplan11(base, fuie_serie=None)

    print(">> Generando reportes ...")
    rep_sub = reporte_por_subcomponente(base)
    rep_d11 = reporte_alertas_diplan(base)

    out_xls = DIR_OUT / "reglas_cierre_diagnostico.xlsx"
    with pd.ExcelWriter(out_xls, engine="openpyxl") as w:
        base.to_excel(w, sheet_name="cui_evaluado", index=False)
        rep_sub.to_excel(w, sheet_name="rep_subcomponente", index=False)
        rep_d11.to_excel(w, sheet_name="rep_diplan11", index=False)
    print(f"   -> {out_xls}")

    print("\n--- RESUMEN POR SUBCOMPONENTE ---")
    print(rep_sub.to_string(index=False))
    print("\n--- ALERTAS DIPLAN11 ---")
    print(rep_d11.to_string(index=False))


if __name__ == "__main__":
    main()
