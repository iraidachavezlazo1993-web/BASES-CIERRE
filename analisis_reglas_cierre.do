/*============================================================================
   ANÁLISIS DE LA BASE DE INVERSIONES Y FUIE PARA SUSTENTAR REGLAS DE DECISIÓN
   PNIE 2017-2028   |   Algoritmo PNIE 2017 + código NB07 + propuesta DIPLAN11
-------------------------------------------------------------------------------
   Autor   : DIPLAN-DIGEIE / MINEDU
   Inputs  : 2026.04.13 Base de Inversiones.xlsx
             Vinculaciones_compartido.xlsx                (CUI <-> cod_local)
             LE_Brecha_FUIE24.dta   (8. CalcBrFUIE24_250730.do)
             FUIE_serie.dta         (serie cod_local x anio x condicion)
   Outputs : out_reglas\reglas_cierre_diagnostico.xlsx
             out_reglas\cui_evaluado.dta
   Reglas  : GI1 ct_st_d ct_sp_d ct_ri ct_ic ct_cp
             GI2 ct_aad  ct_cad
             GI3 ct_me   ct_ce  ct_ene
             GI4 ct_ae   ct_acc ct_amp ct_rc ct_sp_r ct_ic_r
             GI5 ct_st            SAFIL b_safil
   Overlay : DIPLAN11 -> P1, P2, P3, P8, PB, PC, P9
============================================================================*/

clear all
set more off
set varabbrev off
set type double

* ----------------------------------------------------------------------------
* 0. Configuración
* ----------------------------------------------------------------------------
global Main   "C:\BASES-CIERRE"
global Raw    "$Main"
global Input  "$Main\input"
global Out    "$Main\out_reglas"
cap mkdir "$Out"
cd "$Main"

* Umbrales (Algoritmo PNIE 2017 / NB07 dic-2025)
local UMB_AVANCE   = 0.85
local UMB_RATIO    = 0.70
local UMB_P8       = 0.70
local UMB_PB       = 0.30
local UMB_PC_INF   = 0.30
local UMB_PC_SUP   = 0.85
local PLAZO_GN     = 3
local PLAZO_GRL    = 5

* ----------------------------------------------------------------------------
* 1. Lectura del Banco de Inversiones (header en fila 5 -> cellrange A5)
* ----------------------------------------------------------------------------
import excel "2026.04.13 Base de Inversiones.xlsx", ///
        sheet("Data") cellrange(A5) firstrow case(lower) clear

rename codigo_unico CODIGO_UNICO
rename costo_actualizado_bi  COSTO_ACT_BI
rename costo_inv_total_bi    COSTO_INV_BI
rename avance_fisico_f12b    AVANCE_F12B
rename avance_fisico_f9      AVANCE_F9
rename tiene_f9              TIENE_F9
rename cerrado_f9            CERRADO_F9
rename des_tipo_formato      FORMATO
rename tipo_ioarr            T_IOARR
rename nombre_inversion      NOMINV
rename uei                   UEI

destring AVANCE_F12B AVANCE_F9 COSTO_ACT_BI COSTO_INV_BI, replace force

* Plazo de ejecución estimado (años) ≈ FEC_FIN_F8 - FEC_INI_F8
gen double plazo_anios = (fec_fin_f8 - fec_ini_f8) / 365.25

* Nivel de gobierno (GN / GR / GL) según campo NIVEL
gen str4 nivel_gob = "NA"
replace  nivel_gob = "GN" if regexm(upper(nivel), "^GN")
replace  nivel_gob = "GR" if regexm(upper(nivel), "^GR")
replace  nivel_gob = "GL" if regexm(upper(nivel), "^GL")

tempfile inv
save `inv'

* ----------------------------------------------------------------------------
* 2. Vinculaciones CUI <-> cod_local
* ----------------------------------------------------------------------------
import excel "Vinculaciones_compartido.xlsx", firstrow case(lower) clear
cap rename cui  CODIGO_UNICO
cap rename codigo_unico CODIGO_UNICO
cap rename codigo_local cod_local
cap rename codlocal     cod_local
keep CODIGO_UNICO cod_local
duplicates drop CODIGO_UNICO cod_local, force
tempfile vinc
save `vinc'

* ----------------------------------------------------------------------------
* 3. FUIE24 procesado por LE (output de 8. CalcBrFUIE24_250730.do)
*    Variables esperadas: cod_local areatech ratiodem zona_sismica zona_bio
*    sistest gestion_zona gue saneado_margesi
*    ct_st_d ct_sp_d ct_ri ct_ic ct_cp ct_aad ct_cad ct_me ct_ce ct_ene
*    ct_ae ct_acc ct_amp ct_rc ct_sp_r ct_ic_r ct_st b_safil
* ----------------------------------------------------------------------------
cap confirm file "$Input\LE_Brecha_FUIE24.dta"
if !_rc {
    use "$Input\LE_Brecha_FUIE24.dta", clear
    tempfile fuie
    save `fuie'
}
else {
    di as err "Falta $Input\LE_Brecha_FUIE24.dta — se continúa sin variables FUIE"
    clear
    gen str10 cod_local = ""
    tempfile fuie
    save `fuie', emptyok
}

* ----------------------------------------------------------------------------
* 4. Construcción de la base CUI-cod_local-FUIE
* ----------------------------------------------------------------------------
use `inv', clear
merge m:1 CODIGO_UNICO using `vinc', keep(master match) nogen
merge m:1 cod_local   using `fuie', keep(master match) nogen

* ----------------------------------------------------------------------------
* 5. Tipificación del tipo de intervención (T1 ... T6)
* ----------------------------------------------------------------------------
gen str3 tipo_int = "T1"
replace  tipo_int = "T2"  if regexm(upper(FORMATO),"IOARR") | length(T_IOARR) > 3
replace  tipo_int = "T3b" if regexm(upper(NOMINV),"PLAN SELVA|PEIP-EB|PEIP EB")
replace  tipo_int = "T6"  if regexm(upper(NOMINV),"EMERGENC|D\\.U|DECRETO DE URGENCIA|CONTINGENTE")
replace  tipo_int = "T4b" if regexm(upper(NOMINV),"MANTENIMIENTO CORRECTIV")
replace  tipo_int = "T4a" if regexm(upper(UEI),"UGME") | regexm(upper(NOMINV),"MOBILIARIO|EQUIPAMIENTO")
replace  tipo_int = "T3a" if regexm(upper(NOMINV),"M[OÓ]DULO|ANIN|PREFABRICAD")
replace  tipo_int = "T5"  if regexm(upper(NOMINV),"SANEAMIENTO F[IÍ]SICO|HABILIT|ACCESIBILIDAD")

* avance "elegible" para cierre
egen double avance_eleg = rowmax(AVANCE_F12B AVANCE_F9)
gen byte    elegible    = (avance_eleg >= `UMB_AVANCE') | upper(CERRADO_F9) == "SI"

* ----------------------------------------------------------------------------
* 6. Reglas vigentes — Algoritmo 2017 + NB07
* ----------------------------------------------------------------------------
* GI1
gen byte GI1_1 = inlist(tipo_int,"T1","T3b") & ratiodem >= `UMB_RATIO' & elegible == 1
gen byte GI1_2 = inlist(tipo_int,"T1","T2")  & ratiodem <  `UMB_RATIO' & ///
                 inlist(zona_sismica,3,4)    & elegible == 1
gen byte GI1_3 = inlist(tipo_int,"T6","T3b")
gen byte GI1_4 = inlist(tipo_int,"T1","T2")  & gestion_zona == "U" & elegible == 1
* GI2
gen byte GI2_1 = inlist(tipo_int,"T1","T2","T3b") & elegible == 1
gen byte GI2_2 = inlist(tipo_int,"T1","T2")       & elegible == 1
* GI3
gen byte GI3_1 = inlist(tipo_int,"T1","T2")  & elegible == 1
gen byte GI3_2 = inlist(tipo_int,"T1","T3b") & elegible == 1
gen byte GI3_3 = tipo_int == "T4b"
* GI4
gen byte GI4_1 = inlist(tipo_int,"T1","T2") & inlist(zona_sismica,3,4) & elegible == 1
gen byte GI4_2 = inlist(tipo_int,"T1","T2") & elegible == 1
gen byte GI4_3 = inlist(tipo_int,"T1","T2") & elegible == 1
gen byte GI4_4 = inlist(tipo_int,"T1","T2") & elegible == 1
* GI5 (sustitución total -> arrastre)
gen byte GI5_1 = inlist(tipo_int,"T1","T3b") & ratiodem >= `UMB_RATIO' & elegible == 1
* SAFIL
cap confirm variable saneado_margesi
if _rc gen byte saneado_margesi = 0
gen byte SAFIL_1 = saneado_margesi == 1

* Arrastre GI5 -> cierra GI1, GI2, GI3, GI4
foreach g of varlist GI1_1 GI1_2 GI1_4 GI2_1 GI2_2 GI3_1 GI3_2 ///
                     GI4_1 GI4_2 GI4_3 GI4_4 {
    replace `g' = 1 if GI5_1 == 1
}

* ----------------------------------------------------------------------------
* 7. ct_XX_post (cero si la bandera está activa)
*    Excepción: GI1_3 NO pone ct_ic_post = 0  (NB07 explícito)
* ----------------------------------------------------------------------------
local mapeo "ct_st_d=GI1_1 ct_sp_d=GI1_2 ct_ri=GI1_2 ct_cp=GI1_4 " ///
            "ct_aad=GI2_1 ct_cad=GI2_2 ct_me=GI3_2 ct_ce=GI3_1 " ///
            "ct_ene=GI3_3 ct_ae=GI4_2 ct_acc=GI4_3 ct_amp=GI4_4 " ///
            "ct_rc=GI1_2 ct_sp_r=GI4_1 ct_ic_r=GI1_3 ct_st=GI5_1 b_safil=SAFIL_1"
foreach kv of local mapeo {
    local var = substr("`kv'", 1, strpos("`kv'", "=") - 1)
    local flag = substr("`kv'", strpos("`kv'", "=") + 1, .)
    cap confirm variable `var'
    if !_rc gen double `var'_post = cond(`flag' == 1, 0, `var')
}
* La intervención contingente NO cierra ct_ic
cap confirm variable ct_ic
if !_rc gen double ct_ic_post = ct_ic

* ----------------------------------------------------------------------------
* 8. Overlay DIPLAN11
* ----------------------------------------------------------------------------
* P1 - plazo > 3a (GN) o > 5a (GR/GL)
gen byte alerta_P1 = (nivel_gob == "GN" & plazo_anios > `PLAZO_GN') | ///
                     (inlist(nivel_gob,"GR","GL") & plazo_anios > `PLAZO_GRL')

* P2 - duplicidad por (cod_local, tipo_int) -> solo el de mayor avance
bysort cod_local tipo_int (AVANCE_F12B): gen byte n_local_tipo = _N
bysort cod_local tipo_int (AVANCE_F12B): gen double max_avance = AVANCE_F12B[_N]
gen byte alerta_P2 = (n_local_tipo > 1) & (AVANCE_F12B < max_avance)

* P3 - cierre por FUIE directo (requiere FUIE_serie.dta con cod_local anio condicion)
gen byte cierre_P3 = 0
cap confirm file "$Input\FUIE_serie.dta"
if !_rc {
    preserve
        use "$Input\FUIE_serie.dta", clear
        keep cod_local anio condicion
        replace condicion = upper(condicion)
        bysort cod_local (anio): gen str20 cond_t   = condicion[_N]
        bysort cod_local (anio): gen str20 cond_t_1 = condicion[_N-1]
        keep cod_local cond_t cond_t_1
        duplicates drop cod_local, force
        tempfile fserie
        save `fserie'
    restore
    merge m:1 cod_local using `fserie', keep(master match) nogen
    replace cierre_P3 = 1 if cond_t == "ADECUADO" & cond_t_1 == "AUSENTE"
    gen byte alerta_P9 = cond_t == "CRITICO" & cond_t_1 == "CRITICO"
}
else gen byte alerta_P9 = 0

* P8 - cierre parcial proporcional
cap confirm variable area_intervenida
if !_rc & ("area_necesidad" == "area_necesidad") {
    cap confirm variable area_necesidad
    if !_rc {
        gen double pct_cierre_P8 = min(area_intervenida / area_necesidad, 1)
        gen byte   alerta_P8     = pct_cierre_P8 < `UMB_P8'
    }
    else {
        gen double pct_cierre_P8 = .
        gen byte   alerta_P8     = 0
    }
}
else {
    gen double pct_cierre_P8 = .
    gen byte   alerta_P8     = 0
}

* PB - coherencia presupuestaria
cap confirm variable costo_brecha_subcomp
if !_rc {
    gen double ratio_PB = COSTO_INV_BI / costo_brecha_subcomp
    gen byte   alerta_PB = ratio_PB < `UMB_PB' & costo_brecha_subcomp > 0
}
else gen byte alerta_PB = 0

* PC - estado "en proceso de cierre"
gen str25 estado_PC = ""
replace   estado_PC = "EN_PROCESO_DE_CIERRE" if ///
          avance_eleg >= `UMB_PC_INF' & avance_eleg < `UMB_PC_SUP' & ///
          upper(TIENE_F9) != "SI"

* Cierre efectivo
gen byte cierre_efectivo = (elegible == 1) & (alerta_PB == 0) & (alerta_P9 == 0)

* ----------------------------------------------------------------------------
* 9. Reportes diagnósticos
* ----------------------------------------------------------------------------
preserve
    keep CODIGO_UNICO cod_local tipo_int nivel_gob avance_eleg ///
         GI1_1 GI1_2 GI1_3 GI1_4 GI2_1 GI2_2 GI3_1 GI3_2 GI3_3 ///
         GI4_1 GI4_2 GI4_3 GI4_4 GI5_1 SAFIL_1 ///
         ct_*_post ///
         alerta_P1 alerta_P2 alerta_P8 alerta_PB alerta_P9 ///
         cierre_P3 estado_PC cierre_efectivo
    save "$Out\cui_evaluado.dta", replace
    export excel using "$Out\reglas_cierre_diagnostico.xlsx", ///
        sheet("cui_evaluado", replace) firstrow(variables)
restore

* Resumen por subcomponente
preserve
    keep CODIGO_UNICO COSTO_ACT_BI GI1_1 GI1_2 GI1_3 GI1_4 ///
         GI2_1 GI2_2 GI3_1 GI3_2 GI3_3 GI4_1 GI4_2 GI4_3 GI4_4 GI5_1 SAFIL_1
    foreach g of varlist GI1_* GI2_* GI3_* GI4_* GI5_* SAFIL_* {
        gen double monto_`g' = cond(`g' == 1, COSTO_ACT_BI, 0)
    }
    collapse (sum) GI1_* GI2_* GI3_* GI4_* GI5_* SAFIL_* monto_*
    xpose, clear varname
    rename _varname subcomp
    rename v1 valor
    export excel using "$Out\reglas_cierre_diagnostico.xlsx", ///
        sheet("rep_subcomponente", replace) firstrow(variables)
restore

* Resumen alertas DIPLAN11
preserve
    gen one = 1
    collapse (sum) alerta_P1 alerta_P2 alerta_P8 alerta_PB alerta_P9 ///
                   cierre_P3 (sum) one
    foreach v of varlist alerta_P1 alerta_P2 alerta_P8 alerta_PB ///
                         alerta_P9 cierre_P3 {
        gen double pct_`v' = 100 * `v' / one
    }
    export excel using "$Out\reglas_cierre_diagnostico.xlsx", ///
        sheet("rep_diplan11", replace) firstrow(variables)
restore

di as txt _newline ///
   "===> Resultado guardado en $Out\reglas_cierre_diagnostico.xlsx"
di as txt "     Base CUI evaluada en $Out\cui_evaluado.dta"
