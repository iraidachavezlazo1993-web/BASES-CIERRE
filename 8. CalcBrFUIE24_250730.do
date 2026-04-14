	/*______________________________________________________________
	|	                                                      		|
	|	MINEDU - Cálculo de Brecha de Infraestructura Educativa		|
	|	Formato: FUIE - Censo Educativo 2024 						|
	|	Actualizado: 30/07/2025		             					|
	|______________________________________________________________*/

	/*_____________________________________________________________________
	|                                                                      |
	|                               PRÓLOGO                                |
	|_____________________________________________________________________*/
	
	clear 	all
	
	global	Raw			= 	"C:\CalcBrPr2507\raw"				// Carpeta Raw (2025-07)
	global	Raw2		= 	"C:\CalcBrPr2412\raw"				// Carpeta Raw (2024-12)
	global 	Input  		=   "C:\CalcBrPr2507\input" 			// Carpeta Input (Actual)
	global 	Final  		=   "C:\CalcBrPr2507\"					// Carpeta Final
	scalar 	pf_cb		=	0									// Prefabricado permanente no requiere intervención estructural. 1 = SI, 0 = NO.
	cd 		"$Final"

	set 	more off 
	set 	varabbrev off
	set 	type double
	set 	seed 339487731
	set 	excelxlsxlargefile on
	
	/*_____________________________________________________________________
	|                                                                      |
	|            			CÁLCULO DE ÁREAS				   			   |
	|_____________________________________________________________________*/
		
	use 	"$Raw\fuie24\local_sec106", clear
	destring codlocal, gen(cod_local)
	rename 	id_edif cod_edif
	
	duplicates tag cod_local cod_edif p106_3, gen(_aux)
	duplicates tag cod_local cod_edif p106_3 id_terr, gen(_aux2)
	drop	if _aux != 0 & _aux2 == 0
	drop 	_*													// Eliminar observaciones con mismo código local, edif y piso pero distinto ID terreno. Usar info de sec400 para estas edificaciones.
	
	gen 	ac1v_p106 = 0
	merge  m:1 cod_local using "$Input\LE_SiFUIE24", keepusing(si_f24) keep(1 3) nogen
	replace ac1v_p106 = 1 if si_f24 == "Articulación con regiones 2024-11"
	
	gen		ac1_p106_3 = p106_3 < 1 | p106_3 > 6

	forvalues i = 1/9 {
			replace cod_edif = "E0`i'" if cod_edif == "E`i'" | cod_edif == "ED`i'" | cod_edif == "EO`i'" | 	cod_edif == "`i'"
	}
	replace cod_edif = "E01" if cod_edif == "0" | cod_edif == "SI" | cod_edif == "si" | cod_edif == "x" | cod_edif == "X" | cod_edif == "ABC"
	replace cod_edif = "E10" if cod_edif == "10"
	replace cod_edif = "E19" if cod_edif == "e19"
	
	* Base de observaciones de pisos sin códigos de edificación.
	*------------------------------------------------------------
	preserve
		drop if cod_edif != "---"
		keep cod_local cod_edif p106_3 p106_4 ac1_p106_3
		save "$Input\fuie24\Pisos_AC_SinCodEdif_FUIE24.dta", replace
	restore
	drop	if cod_edif == "---"										// Revisar cómo corregir esto con UE en info oficial.
	
	gen 	ac1_p106_4 = p106_4 < 10 | p106_4 > 2500 
	
	gen 	ac1_p106_4_sif24 = p106_4 < 1 | p106_4 == .
	replace ac1_p106_4 = ac1_p106_4_sif24 if ac1v_p106 == 1
	
	egen 	piso_min = min(p106_3), by(cod_local cod_edif)
	gen 	areatechp1 = p106_4 if p106_3 == piso_min
	gen		areatech = p106_4
	
	replace areatechp1 = 10 	if areatechp1 < 10 & ac1v_p106 != 1
	replace areatechp1 = 1 		if areatechp1 < 1 & ac1v_p106 == 1
	replace areatechp1 = 2500 	if areatechp1 > 2500 & areatechp1 != . & ac1v_p106 != 1

	replace areatech = 10 	if areatech < 10 & ac1v_p106 != 1
	replace areatech = 1 	if areatech < 1 & ac1v_p106 == 1
	replace areatech = 2500 if areatech > 2500 & areatech != . & ac1v_p106 != 1

	collapse (count) p106_3 (sum) areatech ac1_* (max) areatechp1, by(cod_local cod_edif)
	rename p106_3 numpisos
	
	replace numpisos = 1 if numpisos < 1 | numpisos == .
	replace areatech = areatech * (6/numpisos) if numpisos > 6 & numpisos != .
	replace numpisos = 6 if numpisos > 6 & numpisos != .

	gen		dif_prom_techp1 = areatech/numpisos-areatechp1
	replace areatech = areatechp1 * numpisos if dif_prom_techp1 > 1.1 * areatechp1
	replace areatechp1 = round(areatechp1,0.01)
	replace areatech = round(areatech,0.01) 
	
	compress
	save 	"$Input\fuie24\Edif_Area_FUIE24.dta", replace
	
	use 	"$Raw\fuie24\local_sec400", clear
	capture confirm scalar pf_cb
    if 		(!_rc == 0) scalar pf_cb = 0					// Prefabricado permanente no requiere intervención estructural. 1 = SI, 0 = NO.
	
	* Preparar variables iniciales
	*------------------------------
	destring p401_5_1 p401_8_1 p401_14_43 p401_17_43, replace force
	replace p401_5_1 = int(p401_5_1)
	replace p401_8_1 = int(p401_8_1)
	replace p401_14_43 = int(p401_14_43)
	replace p401_17_43 = int(p401_17_43)
	
	destring codlocal, gen(cod_local)
	rename 	id_edif cod_edif
	
	* Casos particulares.
	*----------------------
	replace cod_edif = "E01" if cod_local == 15049 & p401_7 == 70		// Revisar en info oficial FUIE24.
	replace cod_edif = "E02" if cod_local == 15049 & p401_7 == 30
	replace cod_edif = "E01" if cod_local == 739023 & numero == "1"
	
	forvalues i = 1/9 {
			replace cod_edif = "E0`i'" if cod_edif == "E`i'" | cod_edif == "ED`i'" | cod_edif == "EO`i'" | 	cod_edif == "`i'"
	}	
	
	* Obtención de variables indicador de información validada.
	*-----------------------------------------------------------
	*merge 	m:1 cod_local using "$Input\LE_Valid_FUIE24", keepusing(ac1v_p105 ac1v_p106 ac1v_p401_7) keep(1 3) nogen
	gen 	ac1v_p105 = 0
	gen 	ac1v_p106 = 0
	gen  	ac1v_p401_7 = 0
	merge  m:1 cod_local using "$Input\LE_SiFUIE24", keepusing(si_f24) keep(1 3) nogen
	replace ac1v_p105 = 1 if si_f24 != "" & si_f24 != "Análisis de consistencia de datos" & si_f24 != "Articulación con regiones 2022-2023 (Cambio AT <= 20%)"
	replace ac1v_p106 = 1 if si_f24 != "" & si_f24 != "Análisis de consistencia de datos" & si_f24 != "Articulación con regiones 2022-2023 (Cambio AT <= 20%)"
	replace ac1v_p401_7 = 1 if si_f24 != "" & si_f24 != "Análisis de consistencia de datos" & si_f24 != "Articulación con regiones 2022-2023 (Cambio AT <= 20%)"

	gen		ac1v_p401_5_1 = (si_f24 == "Articulación con regiones 2023-11")
	gen		ac1v_p401_17_43 = (si_f24 == "Articulación con regiones 2023-11")
	
	* Ajuste de valores previo a análisis de consistencia de datos (ajustes que no involucran análisis de consistencia)
	*--------------------------------------------------------------
	foreach v of varlist p401_16 p401_17_1 p401_17_22 p401_17_32 p401_17_42 p401_18_1 p401_18_2 {
		replace `v' = "01" if `v' == "1"
		replace `v' = "02" if `v' == "2"
		replace `v' = "03" if `v' == "3"
		replace `v' = "04" if `v' == "4"
		replace `v' = "05" if `v' == "5"
	}
	
	* Ajuste de valores y análisis de consistencia de datos (revisión)
	*-----------------------------------------------------------------
	gen		ac1_p401_8_1 = p401_8_1 < 1900 | p401_8_1 > 2024					// Reemplazar por año del Censo Educativo.	
	gen		ac1_p401_8_2 = p401_8_2 != "01" & p401_8_2 != "02" & p401_8_2 != "03" & p401_8_2 != "04" & p401_8_2 != "05" & p401_8_2 != "06" & p401_8_2 != "07"
	replace p401_8_2 = "07" if p401_8_2 != "01" & p401_8_2 != "02" & p401_8_2 != "03" & p401_8_2 != "04" & p401_8_2 != "05" & p401_8_2 != "06" & p401_8_2 != "07"
	replace p401_8_1 = 1900 if p401_8_1 < 1900 | p401_8_1 == .
	replace p401_8_1 = 2024 if p401_8_1 > 2024									// Reemplazar por año del Censo Educativo.	
	
	gen 	ac1_p401_14_43 = p401_14_41 == "1" & (p401_14_43 < 1980 | p401_14_43 > 2024)			// Reemplazar por año del Censo Educativo.	
	replace p401_14_43 = 1980 if p401_14_41 == "1" & (p401_14_43 < 1980 | p401_14_43 == .)
	replace p401_14_43 = 2024 if p401_14_41 == "1" & p401_14_43 > 2024								// Reemplazar por año del Censo Educativo.	
	
	gen		ac1_p401_17_43 = p401_17_41 != 0 & (p401_17_42 == "01" | p401_17_42 == "02" | p401_17_42 == "03") & (p401_17_43 < 1950 | p401_17_43 > 2024)				// Reemplazar por año del Censo Educativo.
	gen		ac1_p401_17_43_sif24 = p401_17_41 != 0 & (p401_17_42 == "01" | p401_17_42 == "02" | p401_17_42 == "03") & (p401_17_43 > 2024)
	replace	ac1_p401_17_43 = ac1_p401_17_43_sif24 if ac1v_p401_17_43 == 1	
	
	replace p401_17_43 = 1950 if (p401_17_42 == "01" | p401_17_42 == "02" | p401_17_42 == "03") & (p401_17_43 < 1950 | p401_17_43 == .) & ac1v_p401_17_43 != 1
	replace p401_17_43 = 1950 if (p401_17_42 == "01" | p401_17_42 == "02" | p401_17_42 == "03") & (p401_17_43 == .) & ac1v_p401_17_43 == 1 	
	replace p401_17_43 = 2024 if (p401_17_42 == "01" | p401_17_42 == "02" | p401_17_42 == "03") & p401_17_43 > 2024													// Reemplazar por año del Censo Educativo.
	
	gen 	ac1_p401_4 = p401_4 != "1" & p401_4 != "2"
	gen 	ac1_p401_4a = p401_4 == "2" & (p401_10 == "07" | p401_10 == "08")
	
	gen		ac1_p401_5_1 = p401_4 == "1" & (p401_5_1 < 1950 | p401_5_1 > 2024)												// Reemplazar por año del Censo Educativo.
	gen		ac1_p401_5_1_sif24 = p401_4 == "1" & (p401_5_1 > 2024)															// Reemplazar por año del Censo Educativo.
	replace	ac1_p401_5_1 = ac1_p401_5_1_sif24 if ac1v_p401_5_1 == 1	
	gen		ac1_p401_5_2 = p401_4 == "1" & (p401_5_2 != "01" & p401_5_2 != "02" & p401_5_2 != "03")
	
	gen 	ac1_p401_6 = p401_6 < 1 | p401_6 > 6
	gen 	ac1_p401_6a = p401_4 == "1" & p401_6 > 2 & p401_6 <= 6
	
	forvalues i = 1/9 {
		replace p401_10 = "0`i'" if p401_10 == "`i'"
	}
	
	gen		ac1_p401_10 = p401_10 != "01" & p401_10 != "02" & p401_10 != "03" & p401_10 != "04" & ///
						  p401_10 != "05" & p401_10 != "06" & p401_10 != "07" & p401_10 != "08" & ///
						  p401_10 != "09" & p401_10 != "10" 
	gen 	ac1_p401_10a = p401_4 == "1" & (p401_10 != "07" & p401_10 != "08")
	
	gen		ac1_p401_13_1 = p401_13_1 != "1" & p401_13_1 != "2"
	gen		ac1_p401_13_2 = p401_13_2 != "1" & p401_13_2 != "2"
	gen		ac1_p401_13_3 = p401_13_3 != "1" & p401_13_3 != "2"
	
	gen		ac1_p401_14_1 = p401_14_1 != "1" & p401_14_1 != "2"
	gen		ac1_p401_14_21 = p401_14_1 != "1" & p401_14_21 != "1" & p401_14_21 != "2"
	gen		ac1_p401_14_22 = p401_6 != 1 & p401_14_22 != "1" & p401_14_22 != "2"
	gen		ac1_p401_13_41 = p401_14_41 != "1" & p401_14_41 != "2"
	gen		ac1_p401_13_42 = p401_14_41 == "1" & p401_14_42 != "1" & p401_14_42 != "2"
	
	gen		ac1_p401_16 = p401_16 != "01" & p401_16 != "02" & p401_16 != "03" & p401_16 != "04"
	gen		ac1_p401_17_1 =  p401_13_3 == "1" & (p401_17_1 != "01" & p401_17_1 != "02" & p401_17_1 != "03")
	gen		ac1_p401_17_21 = p401_13_3 == "1" & (p401_17_21 < 0 | p401_17_21 > 99)
	gen		ac1_p401_17_22 = p401_13_3 == "1" & p401_17_21 != 0 & (p401_17_22 != "01" & p401_17_22 != "02" & p401_17_22 != "03" & p401_17_22 != "04" & p401_17_22 != "05")
	gen		ac1_p401_17_31 = p401_13_3 == "1" & (p401_17_31 < 0 | p401_17_31 > 99)
	gen		ac1_p401_17_32 = p401_13_3 == "1" & p401_17_31 != 0 & (p401_17_32 != "01" & p401_17_32 != "02" & p401_17_32 != "03" & p401_17_32 != "04" & p401_17_32 != "05")
	gen		ac1_p401_17_41 = p401_13_3 == "1" & (p401_17_41 < 0 | p401_17_41 > 99)
	gen		ac1_p401_17_42 = p401_13_3 == "1" & p401_17_41 != 0 & (p401_17_42 != "01" & p401_17_42 != "02" & p401_17_42 != "03" & p401_17_42 != "04" & p401_17_42 != "05")
	
	gen		ac1_p401_18_1 = p401_18_1 != "01" & p401_18_1 != "02" & p401_18_1 != "03" & p401_18_1 != "04" & p401_18_1 != "05"
	gen		ac1_p401_18_2 = p401_18_2 != "01" & p401_18_2 != "02" & p401_18_2 != "03" & p401_18_2 != "04" & p401_18_2 != "05"
	
	replace p401_10 = "" if ac1_p401_10 == 1											// Si sistema estructural está mal llenado, se deja incompleto.
	replace p401_10 = "08" if p401_4 == "1" & (p401_10 != "07" & p401_10 != "08")		// Si variable de módulos prefabricado = SI pero sistema estructural no es módulos prefabricados, el sistema estructural se cambia a Módulo prefabricado-Otro.
	replace p401_4 = "1" if p401_4 == "2" & (p401_10 == "07" | p401_10 == "08")			// Si variable de módulos prefabricado = NO pero sistema estructural es módulos prefabricados, la variable se cambia a SI.
	replace p401_5_1 = p401_8_1 if p401_4 == "1" & p401_5_1 == .
	replace p401_5_1 = 1950 if p401_4 == "1" & (p401_5_1 < 1950 | p401_5_1 == .) & ac1v_p401_5_1 != 1 
	replace p401_5_1 = 1950 if p401_4 == "1" & (p401_5_1 == .) & ac1v_p401_5_1 == 1 
	replace p401_5_1 = 2024 if p401_4 == "1" & p401_5_1 > 2024 							// Reemplazar por año del Censo Educativo.
	replace p401_5_2 = "02" if p401_4 == "1" & (p401_5_2 == ""  | p401_5_2 == "--")
	replace p401_6 = 2 if p401_4 == "1" & & p401_6 > 2 & p401_6 != .
	
	gen		_numpisos = int(p401_6)
	replace _numpisos = 1 if _numpisos < 1 | _numpisos == .
	replace _numpisos = 2 if p401_4 == "1" & ( _numpisos > 2 & _numpisos != .)
	replace _numpisos = 6 if _numpisos > 6 & _numpisos != .

	gen		ac1_p401_7 = p401_7 < (10 * _numpisos) | p401_7 > (2500 * _numpisos)
	gen 	ac1_p401_7_sif24 = p401_7 < 1 | p401_7 == .
	replace ac1_p401_7 = ac1_p401_7_sif24 if ac1v_p401_7 == 1
	
	gen		_areatechp1 = abs(p401_7)/_numpisos												// Área total de la edificación / Número de pisos.
	replace _areatechp1 = 10 	if _areatechp1 < 10 & ac1v_p401_7 != 1
	replace _areatechp1 = 1 	if _areatechp1 < 1 & ac1v_p401_7 == 1
	replace _areatechp1 = 2500 	if _areatechp1 > 2500 & _areatechp1 != . & ac1v_p401_7 != 1
	replace _areatechp1 = round(_areatechp1,0.01) 
	
	gen 	_areatech = _areatechp1 * _numpisos
	
	* Consolidación con base de áreas techadas por piso.
	*----------------------------------------------------
	merge 	1:1 cod_local cod_edif using "$Input\fuie24\Edif_Area_FUIE24", keepusing(areatech areatechp1 numpisos ac1_*)
	drop 	if _merge == 2
	gen		info_area_sec400 = _merge == 1 | (ac1_p401_7 == 0 & ac1_p106_4 > 0 & ac1_p106_4 != .)
	gen		ac1_cod_edif = _merge == 1
	
	replace numpisos = _numpisos if info_area_sec400 == 1
	replace numpisos = 1 if _merge == 3 & p401_4 == "1" & (numpisos < 1)
	replace numpisos = 2 if _merge == 3 & p401_4 == "1" & (numpisos > 2 & numpisos != .)

	replace areatechp1 = _areatechp1 if info_area_sec400 == 1
	replace areatech = _areatech if info_area_sec400 == 1
	replace areatech = areatechp1 if _merge == 3 & p401_4 == "1" & (numpisos < 1)
	replace areatech = areatechp1 * 2  if _merge == 3 & p401_4 == "1" & (numpisos > 2 & numpisos != .)

	drop 	_*
	
	* Base de edificaciones sin áreas techadas.
	*-------------------------------------------
	preserve
		drop if areatech != .
		keep cod_local cod_edif numpisos areatechp1 ac1_p401_6 ac1_p401_7 ac1_p106_3 ac1_p106_4 ac1_cod_edif
		save "$Input\fuie24\Edif_AC_SinAr_FUIE24.dta", replace
	restore
	drop 	if areatech == .

	* Corrección de áreas techadas según intervalo de área techada total = 20-50,000
	*--------------------------------------------------------------------------------
	egen	areatech_le = sum(areatech), by(cod_local)
	gen		ac1_areatech_le = areatech_le < 20 | areatech_le > 50000
	gen		ac1_areatech_le_sif24 = areatech_le < 10 | areatech_le == .
	replace ac1_areatech_le = ac1_areatech_le_sif24 if ac1v_p106 == 1 | ac1v_p401_7 == 1
	
	replace areatechp1 = areatechp1/areatech_le * 20 	if areatech_le < 20 & ac1v_p106 != 1 & ac1v_p401_7 != 1
	replace areatechp1 = areatechp1/areatech_le * 10 	if areatech_le < 10 & (ac1v_p106 == 1 | ac1v_p401_7 == 1)
	replace areatechp1 = areatechp1/areatech_le * 50000	if areatech_le > 50000 & areatech_le != . & ac1v_p106 != 1 & ac1v_p401_7 != 1
	
	replace areatechp1 = round(areatechp1,0.01) if (areatech_le < 20 | (areatech_le > 50000 & areatech_le != .)) & ac1v_p106 != 1 & ac1v_p401_7 != 1
	replace areatechp1 = round(areatechp1,0.01) if (areatech_le < 10) & (ac1v_p106 == 1 | ac1v_p401_7 == 1)
	replace	areatech = areatechp1 * numpisos 	if (areatech_le < 20 | (areatech_le > 50000 & areatech_le != .)) & ac1v_p106 != 1 & ac1v_p401_7 != 1
	replace areatech = areatechp1 * numpisos 	if (areatech_le < 10) & (ac1v_p106 == 1 | ac1v_p401_7 == 1)
	
	drop	areatech_le
	egen	areatech_le = sum(areatech), by(cod_local)	
	
	* Sistema estructural predominante
	*----------------------------------
	gen 	sistest = 1 if (p401_10 == "01" | p401_10 == "02") & p401_8_1 > 1998 & (p401_8_2 == "01" | p401_8_2 == "02" | p401_8_2 == "03")
	replace sistest = 2 if sistest == . & (p401_10 == "01" | p401_10 == "02") & (p401_8_1 <= 1998 & p401_8_1 >= 1978) & (p401_8_2 == "01" | p401_8_2 == "02" | p401_8_2 == "03")
	replace	sistest = 9 if sistest == . & (p401_10 == "01" | p401_10 == "02") & ((p401_8_1 <= 1977 & (p401_8_2 == "01" | p401_8_2 == "02" | p401_8_2 == "03"))  ///
										| (p401_8_2 == "04" | p401_8_2 == "05" | p401_8_2 == "06" | p401_8_2 == "07"))
	replace	sistest = 5 if sistest == . & p401_10 == "06"
	replace	sistest = 7 if sistest == . & p401_10 == "05"
	replace sistest = 3 if sistest == . & p401_10 == "04"
	replace sistest = 4 if sistest == . & p401_10 == "03"
	replace sistest = 8 if sistest == . & (p401_10 == "09" | p401_10 == "10" | p401_10 == "" | ((p401_10 == "07" | p401_10 == "08") & p401_5_1 <= 1998))
	replace sistest = 10 if sistest == . & (p401_10 == "07" | p401_10 == "08") & p401_5_1 > 1998
	label 	define sistest 1 "780-POST" 2 "780-PRE" 3 "A" 4 "ASC" 5 "EA" 6 "GUE" 7 "M" 8 "P" 9 "PCM" 10 "PROV"					// DEFINIR: si incluir módulos prefabricados permanentes como cierre de brecha.
	label 	values sistest sistest
		
	merge 	m:1 cod_local using "$Input\LE_BaseAdic", keepusing(zona codgeo cen_pob matricula zonaamenaza clima tmin_ugel capital ciudad_inei pob_ciud_cap_inei densidad)
	keep 	if _merge == 3
	drop 	_merge									// Mantener únicamente LL.EE. que tengan información de la FUIE y en BaseAdic = LL.EE. con al menos un servicio educativo (código modular) público activo.

	merge 	m:1 cod_local using "$Input\LE_InfRie"	// LL.EE. con informe de riesgo, se cambia el sistema estructural PNIE a precario (regla interna).
	drop 	if _merge == 2
	gen 	infriesgo = _merge == 3
	replace sistest = 8 if infriesgo == 1
	drop 	_merge
	tab 	sistest infriesgo
	
	* Definir escenarios
	* -------------------
	egen	matdisturb = sum(matricula) if zona == 2, by(codgeo)
	egen	matcp = sum(matricula), by(cen_pob)
	
	gen		escenario = 1 if zona == 2 & (ciudad_inei == "LIMA METROPOLITANA" | ciudad_inei == "AREQUIPA")
	replace	escenario = 2 if escenario == . & zona == 2 & pob_ciud_cap_inei != .
	replace	escenario = 3 if escenario == . & zona == 2 & (ciudad_inei != "" | capital == 2 | tmin_ugel < 60 | (matdisturb > 200 & matdisturb != .)) 
	replace	escenario = 4 if escenario == . & ((zona == 1 & (ciudad_inei != "" | capital == 2 | capital == 3)) | (tmin_ugel < 300 | (matricula >= 100 & matricula != .) | (matcp > 300 & matcp != .))) 
	replace	escenario = 5 if (escenario > 2 & (tmin_ugel > 300 & tmin_ugel != .)) | (escenario == . & (densidad < 100 | matricula < 100))
	replace escenario = 3 if escenario == . & zona == 2		// Escenario con costos más altos de zona urbana.
	replace escenario = 5 if escenario == . & zona == 1		// Escenario con costos más altos de zona rural.
	
	label 	define escenario 1 "Grandes ciudades" 2 "Ciudades intermedias" 3 "Centros urbanos" 4 "Pueblos conectados" 5 "Comunidades dispersas"
	label 	values escenario escenario
	
	* Topografía
	*---------------
	merge 	m:1 codlocal using "$Raw\fuie24\local_lineal_01", keepusing(p105 p106 p109)
	drop 	if _merge == 2
	drop	 _merge
	
	* Análisis de consistencia de datos (revisión)
	*-----------------------------------------------------------------
	gen 	ac1_p105 = p105 < 20 | p105 > 50000
	gen 	ac1_p106 = p106 < 20 | p106 > 50000 
	* gen 	ac1_p106a1 = round(p106_qe + p106_qd*0.01) > round(p105_qe + p105_qd*0.01)			// CORREGIR
	* gen 	ac1_p106a2 = round(p106_qe + p106_qd*0.01) != round(areatech_le)					// CORREGIR
	
	gen 	ac1_p105_sif24 = p105 < 1 | p105 == .
	gen 	ac1_p106_sif24 = p106 < 1 | p106 == .
	
	replace ac1_p105 = ac1_p105_sif24 if ac1v_p105 == 1
	replace ac1_p106 = ac1_p106_sif24 if ac1v_p106 == 1
	
	gen 	ac1_p109 = p109 != "1" & p109 != "2" & p109 != "3" & p109 != "4"
	
	destring p109, gen(topografia)
	label	define topo 1 "Llano / Plano" 2 "Inclinado" 3 "Muy inclinado / Desnivelado" 4 "Accidentado / Irregular / Quebrada"
	label 	values topografia topo
	replace topografia = 4 if topografia == .				// Caso más conservador. Si no tiene registro de topografía, se registra la topografía más accidentada.
	gen		pendiente = 1 if topografia == 1 | topografia == 2
	replace pendiente = 2 if topografia == 3 | topografia == 4
	label	define pendiente 1 "Sin pendiente" 2 "Con pendiente"
	label 	values pendiente pendiente
	
	* Cálculo de áreas según tipo de intervención
	*---------------------------------------------	
	gen 	areadem = areatech if (sistest == 3 | sistest == 4 | sistest == 5 | sistest == 7 | sistest == 8 | ///
			(pf_cb == 0 & sistest == 10) | (pf_cb == 1 & sistest == 10 & p401_5_2 != "03") | (sistest == 9 & zona == 1)) & zonaamenaza != 1						// Cálculo de área de demolición.
	gen		areari = areatech if (sistest == 2 | (sistest == 9 & zona == 2)) & zonaamenaza != 1																	// Cálculo de área de reforzamiento incremental.
	gen 	arearc = areatech if (sistest == 6) & zonaamenaza != 1																								// Cálculo de área de reforzamiento convencional.
	gen		areaic2 = areatech if (sistest != 1 & (pf_cb == 0 | (pf_cb == 1 & (sistest != 10 | (sistest == 10 & p401_5_2 != "03"))))) & zonaamenaza == 1		// Cálculo de área de intervención contingente en ZA baja.
	gen		areasi = areatech if sistest == 1 | (pf_cb == 1 & sistest == 10 & p401_5_2 == "03")
	
	egen 	_aux = rowtotal(areadem areari arearc areaic2 areasi), missing
	count 	if _aux > areatech													
	count 	if _aux < areatech																							//	Debe resultar = 0. Verificar si hay inconsistencias.
	drop 	_aux
	
	egen	areadem_le = sum(areadem), by(cod_local)
	gen		ratiodem = areadem_le / areatech_le
	
	gen		areasust = areadem if ratiodem >= 0.7 | (ratiodem < 0.7 & (zonaamenaza == 3 | zonaamenaza == 4))			// Cálculo de área de sustitución; se separa área demolición en ZA = 2 a intervención contingente.
	gen		areaic1 = areadem if ratiodem < 0.7 & zonaamenaza == 2														// Cálculo de área de intervención contingente en ZA media.
	
	egen 	_aux = rowtotal(areasust areaic1), missing
	count 	if _aux > areadem													
	count 	if _aux < areadem & areadem != .																			//	Debe resultar = 0. Verificar si hay inconsistencias.
	drop 	_aux
	
	* Cálculo de áreas para calidad de energía
	*------------------------------------------
	replace p401_17_22 = "04" if p401_17_22 != "01" & p401_17_22 != "02" & p401_17_22 != "03" & p401_17_22 != "04" & p401_17_22 != "05"
	replace p401_17_32 = "04" if p401_17_32 != "01" & p401_17_32 != "02" & p401_17_32 != "03" & p401_17_32 != "04" & p401_17_32 != "05" 
	replace p401_17_42 = "04" if p401_17_42 != "01" & p401_17_42 != "02" & p401_17_42 != "03" & p401_17_42 != "04" & p401_17_42 != "05" 
	
	gen		area_ce1 = areatech if ratiodem < 0.7 & areasust == . & p401_13_3 != "1"													// Requiere instalaciones eléctricas interiores.
	gen		area_ce2 = areatech if ratiodem < 0.7 & areasust == . & p401_13_3 == "1" & p401_17_1 != "01"								// Requiere canalizar circuitos elécticos.
	gen		area_ce3 = areatech if ratiodem < 0.7 & areasust == . & p401_13_3 == "1" & 	///
			(p401_17_22 == "03" | p401_17_22 == "04" | p401_17_32 == "03" | p401_17_32 == "04")											// Requiere nuevo tablero o gabinete.
	gen		area_ce4 = areatech if ratiodem < 0.7 & areasust == . & p401_13_3 == "1" & (p401_17_42 == "04")								// Requiere sistema de puesta a tierra.
	gen		area_ce5 = areatech if ratiodem < 0.7 & areasust == . & p401_13_3 == "1" & (p401_17_42 == "03" | ///
			((p401_17_42 == "01" | p401_17_42 == "02") & p401_17_43 < 2023))															// Requiere nuevo sistema de puesta a tierra. 
																																		// ACTUALIZAR: año de mantenimiento, 1 año menos que el actual.								
	
	* Cálculo de metros lineales de canaletas aéreas y bajadas pluviales 
	*--------------------------------------------------------------------
	replace p401_18_1 = "04" if p401_18_1 != "01" & p401_18_1 != "02" & p401_18_1 != "03" & p401_18_1 != "04" & p401_18_1 != "05"
	replace p401_18_2 = "04" if p401_18_2 != "01" & p401_18_2 != "02" & p401_18_2 != "03" & p401_18_2 != "04" & p401_18_2 != "05" 
	
	gen		ca_reh = 2 * sqrt(areatechp1) if clima >= 2 & clima <= 9 & p401_18_1 == "02" 												// Rehabilitación de canaletas aéreas.
	gen		ca_sus = 2 * sqrt(areatechp1) if clima >= 2 & clima <= 9 & p401_18_1 == "03"												// Sustitución de canaletas aéreas.
	gen		ca_imp = 2 * sqrt(areatechp1) if clima >= 2 & clima <= 9 & p401_18_1 == "04"												// Implementación de canaletas aéreas.
	
	gen		bp_reh = 2 * 2.8 * numpisos if clima >= 2 & clima <= 9 & p401_18_2 == "02"													// Rehabilitación de bajadas pluviales.
	gen		bp_sus = 2 * 2.8 * numpisos if clima >= 2 & clima <= 9 & p401_18_2 == "03"													// Sustitución de bajadas pluviales.
	gen		bp_imp = 2 * 2.8 * numpisos if clima >= 2 & clima <= 9 & p401_18_2 == "04"													// Implementación de bajadas pluviales.
	
	compress
	save 	"$Input\fuie24\Edif_Infra_FUIE24.dta", replace
	
	* Colapso de base a locales y cálculo de indicadores
	*----------------------------------------------------
	use 	"$Input\fuie24\Edif_Infra_FUIE24", clear
	
	gen 	edif = substr(cod_edif,2,.)
	destring edif, replace
	
	collapse 	(sum) areadem areasust areari arearc areaic1 areaic2 areasi areatech areatechp1 area_ce* ca_* bp_* ac1_p4* ac1_c* info_area_sec400 		///
				(max) ratiodem zonaamenaza clima escenario zona topografia pendiente matricula infriesgo ac1_p1* ac1_a*									///
				(count) edif, by(cod_local)		
	
	gen		int_st = areasust > 0  & ratiodem >= 0.7
	gen		int_sp = areasust > 0 & ratiodem < 0.7  
	gen		int_ri = areari > 0 & ratiodem < 0.7
	gen		int_rc = arearc > 0 & ratiodem < 0.7
	gen		int_ic = (areaic1 > 0 | areaic2 > 0) & ratiodem < 0.7
	
	egen 	areariesgo = rowtotal(areasust areari arearc areaic1 areaic2), missing
	replace areariesgo = areadem if areariesgo < areadem					// No debería suceder.
	replace areariesgo = areatech if areatech < areariesgo					// No debería suceder.
	
	gen 	ratioriesgo = areariesgo / areatech
	replace ratioriesgo = 1 if ratioriesgo > 1 & ratioriesgo != .			// No debería suceder.
	replace ratiodem = ratioriesgo if ratioriesgo < ratiodem				// No debería suceder.
	
	label 	values zonaamenaza zonaamenaza
	label 	values clima clima
	label 	values escenario escenario
	label	values zona zona
	label	values topografia topo
	label 	values pendiente pendiente
	
	compress
	save 	"$Input\fuie24\LE_Infra_FUIE24.dta", replace

	/*_____________________________________________________________________
	|                                                                      |
	|            	INTERVENCIÓN DE CERCOS PERIMÉTRICOS 	               |
	|_____________________________________________________________________*/
	
	* Obtener tramos sin cerco perimétrico
	*--------------------------------------
	use 	"$Raw\fuie24\local_sec302", clear
	destring codlocal, gen(cod_local)
	
	merge 	m:1 cod_local using "$Input\fuie24\LE_Infra_FUIE24", keepusing(zona)
	keep 	if _merge == 3
	drop 	_merge																// Mantener locales que tienen información del estado de la infraestructura.
	
	replace p302_8 = "12" if p302_8 == "--" | p302_8 == "12" | p302_8 == "00"
	replace p302_4 = "2" if p302_8 == "12" | p302_8 == ""
	replace p302_6 = 0 if p302_8 == "12" | p302_8 == ""
	replace p302_4 = "2" if p302_6 == 0 | p302_6 == .
	
	replace p302_4 = "1" if p302_6 != 0 & p302_6 != . & p302_8 != "12" & p302_8 != ""
 	
	replace p302_3 = 1 if p302_3 < 1 						// Límite mínimo de 1m. de tramo.
	replace p302_3 = 5000 if p302_3 > 5000 & p302_3 != .	// Límite máximo de 5,000m. de tramo.
	
	replace p302_6 = 1 if p302_4 == "1" & p302_6 < 1 							// Límite mínimo de 1m. de cerco perimétrico.
	replace p302_6 = 5000 if p302_4 == "1" & p302_6 > 5000 & p302_6 != .		// Límite máximo de 5,000m. de cerco perimétrico.
	
	replace	p302_3 = p302_6 if p302_4 == "1" & p302_6 > p302_3 & p302_6 != .	// Tramo obtiene longitud del cerco si cerco > tramo.
	
	drop	if p302_3 == 0 | p302_3 == .										
	
	replace p302_8 = "12" if p302_8 == "00" | p302_8 == "--"
	replace p302_9 = "04" if p302_9 == "00" | p302_9 == "--"
	replace p302_9 = "04" if p302_4 == "1" & p302_9 == ""
	
	destring p302_8 p302_9, replace
	recode 	p302_8 (1 = 2 "Muro de ladrillo o piedra con columnas") (2 4 7 8 9 10 = 5 "Adobe/tapia/pirca u otros") 		///		VERIFICAR categorías con Ingeniero (consultar si Piedra en Bloque = Muro de ladrillo o piedra con columnas")
			(3 = 3 "Muro de ladrillo o piedra sin columnas") (5 6 = 4 "Malla metálica/alambre") (11 = 6 "Cerco vivo"), gen(matconst)
	replace matconst = . if matconst == 12
	label 	define estcons 1 "Sin daño" 2 "Fisuras leves" 3 "Fisuras moderadas / ataque de sales" 4 "Agrietamiento / colapso"
	label 	values p302_9 estcons
	
	gen		cp = p302_4 == "1"
	gen		cpf = p302_3 - p302_6 											// Cerco perimétrico faltante en tramos con cerco perimétrico parcial.
	
	gen		cp_n = p302_3 if cp == 0 & zona == 2 
	gen		cp_nv =	p302_3 if cp == 0 & zona == 1
	
	gen		cp_dr = p302_6 if cp == 1 & zona == 2 & (((matconst == 1 | matconst == 2) & p302_9 == 4) | (matconst != 1 & matconst != 2))
	gen		cp_drv = p302_6 if cp == 1 & zona == 1 & (((matconst == 1 | matconst == 2) & p302_9 == 4) | (matconst != 1 & matconst != 2))

	gen		cp_mm = p302_6 if cp == 1 & (matconst == 1 | matconst == 2) & p302_9 == 3
	gen		cp_mb = p302_6 if cp == 1 & (matconst == 1 | matconst == 2) & p302_9 == 2
	gen		cp_si = p302_6 if cp == 1 & (matconst == 1 | matconst == 2) & p302_9 == 1
	
	replace cp_n = cpf if cp == 1 & zona == 2 & cpf > 0 & cpf != .
	replace cp_nv = cpf if cp == 1 & zona == 1 & cpf > 0 & cpf != .	
	
	save 	"$Input\fuie24\Tramos_Cerco_FUIE24.dta", replace
	
	collapse 	(sum) cp_* p302_3 p302_6 cpf cp (max) zona, by(cod_local)
	drop 	if p302_3 == 0
	
	rename 	(p302_3 p302_6) (tramo cerco)
	
	compress
	save 	"$Input\fuie24\LE_Cerco_FUIE24.dta", replace
	
	
	/*_____________________________________________________________________
	|                                                                      |
	|            	CÁLCULO DE ÁREAS DE TERRENO,						   | 
	|				ALUMNOS POR TURNO Y POR NIVEL	           			   |
	|_____________________________________________________________________*/
		 	
	* Cálculo de áreas de terreno
	*------------------------------
	use 	"$Raw\fuie24\local_lineal_01.dta", clear
	destring codlocal, gen(cod_local)
	keep	codlocal cod_local p105
	merge 	1:1 cod_local using "$Input\fuie24\LE_Infra_FUIE24", keepusing(zona areatechp1)
	keep 	if _merge == 3
	drop 	_merge																// Mantener locales que tienen información del estado de la infraestructura y área de terreno.
	
	gen 	areaterr = p105
	replace	areaterr = areatechp1 if areatechp1 > areaterr
	
	compress
	save	"$Input\fuie24\LE_Aterr_FUIE24.dta", replace
			
	* Fusión de bases de IIEE con Anexos y cálculo de alumnos por turno y por nivel
	*-------------------------------------------------------------------------------
	use 	"$Raw\fuie24\local_sec500.dta", clear
	destring codlocal, gen(cod_local)
	merge 	m:1 cod_local using "$Input\fuie24\LE_Infra_FUIE24", keepusing(zona)
	keep 	if _merge == 3
	drop 	_merge																// Mantener ambientes en locales que tienen información del estado de la infraestructura y de aulas.
	
	label	define nivel 1 "Inicial" 2 "Primaria" 3 "Secundaria" 4 "Educación Básica Alternativa (EBA)" ///
			5 "Educación Básica Especial (EBE)" 6 "Educación Superior de Formación Artística (ESFA)" ///
			7 "Instituto Superior Tecnológico (IST)" 8 "Instituto Superior Pedagógico (ISP)" ///
			9 "Centro de Estudios Técnico Productivo (CETPRO)"
			
	forvalues i = 1/3 {
		gen 	_niv`i' = substr(p501_5_`i'1,1,1)
		gen		niv`i' = 1 if _niv`i' == "A"
		replace niv`i' = 2 if _niv`i' == "B"
		replace niv`i' = 3 if _niv`i' == "F"
		replace niv`i' = 4 if _niv`i' == "D"
		replace niv`i' = 5 if _niv`i' == "E"
		replace niv`i' = 6 if _niv`i' == "M"
		replace niv`i' = 7 if _niv`i' == "T" | _niv`i' == "S"
		replace niv`i' = 8 if _niv`i' == "K" | _niv`i' == "P"
		replace niv`i' = 9 if _niv`i' == "L"
		drop _niv`i'
		label values niv`i' nivel 
	}
		
	forvalues i = 1/9 {
			gen alum`i'_t1 = p501_5_14 if niv1 == `i'
			gen alum`i'_t2 = p501_5_24 if niv2 == `i'
			gen alum`i'_t3 = p501_5_34 if niv3 == `i' 
	}
	
	forvalues i = 1/2 {
			gen alum`i' = max(alum`i'_t1, alum`i'_t2, alum`i'_t3)
			egen alum`i'tot = rowtotal(alum`i'_t1 alum`i'_t2 alum`i'_t3)
			gen alum`i'max = max(alum`i'_t1, alum`i'_t2, alum`i'_t3)
	}
	egen 	alum3 = rowtotal(alum3_t1 alum3_t2 alum3_t3)
	egen	alum3tot = rowtotal(alum3_t1 alum3_t2 alum3_t3)
	gen		alum3max = max(alum3_t1, alum3_t2, alum3_t3)
	
	forvalues i = 4/9 {
			gen alum`i' = max(alum`i'_t1, alum`i'_t2, alum`i'_t3)
			egen alum`i'tot = rowtotal(alum`i'_t1 alum`i'_t2 alum`i'_t3)
			gen alum`i'max = max(alum`i'_t1, alum`i'_t2, alum`i'_t3)
	}
	drop	alum1_*	alum2_* alum3_* alum4_* alum5_* alum6_* alum7_* alum8_* alum9_*
	
	collapse	(sum) alum*, by (cod_local)
	
	egen	alum = rowtotal(alum1 alum2 alum3 alum4 alum5 alum6 alum7 alum8 alum9)
	egen	alumtot = rowtotal(alum1tot alum2tot alum3tot alum4tot alum5tot alum6tot alum7tot alum8tot alum9tot)
	egen	alummax = rowtotal(alum1max alum2max alum3max alum4max alum5max alum6max alum7max alum8max alum9max)
	
	gen		ac1_alum = alum == 0
	
	* Acotar número de alumnos máximo y total por nivel a matrícula registrada.
	*------------------------------------------------------------------------------
	merge 	m:1 cod_local using "$Input\LE_BaseAdic", keepusing(zona matricula m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO)
	drop 	if _merge == 2
	drop 	_merge	
	
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local niv = `niv' + 1
		count if alum`niv'tot  > `v'
		count if alum`niv'  > `v'
		count if alum`niv'max  > `v'
		table zona, stat(sum alum`niv'tot)
		table zona, stat(sum `v')
	}
	
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local niv = `niv' + 1
		replace alum`niv'tot = `v' if alum`niv'tot  > `v'
		replace alum`niv' = `v' if alum`niv'  > `v'
		replace alum`niv'max = `v' if alum`niv'max  > `v'
	}
	
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local niv = `niv' + 1
		count if alum`niv'tot  > `v'
		count if alum`niv'  > `v'
		count if alum`niv'max  > `v'
		table zona, stat(sum alum`niv'tot)
		table zona, stat(sum `v')
	}
	
	* Si el nivel solo tiene un turno, usar matrícula registrada.
	*--------------------------------------------------------------
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local 	niv = `niv' + 1
		count 	if alum`niv'tot  < `v' & alum`niv'tot == alum`niv'max & alum`niv'tot != 0
		gen 	turno`niv'uni = 1 if alum`niv'tot == alum`niv'max & alum`niv'tot != 0
		replace	turno`niv'uni = 0 if alum`niv'tot > alum`niv'max & alum`niv'tot != 0
		replace	turno`niv'uni = 2 if alum`niv'tot < alum`niv'max & alum`niv'tot != 0				// Corregir si aparece esta categoría en las variables.
	}
	
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local niv = `niv' + 1
		replace 	alum`niv'tot = `v' 	if turno`niv'uni == 1
		replace 	alum`niv' = `v' 	if turno`niv'uni == 1
		replace 	alum`niv'max = `v'  if turno`niv'uni == 1
	}
	
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local 	niv = `niv' + 1
		count if alum`niv'tot  < `v' & alum`niv'tot == alum`niv'max & alum`niv'tot != 0
		table 	zona, stat(sum alum`niv'tot)
		table 	zona, stat(sum `v')
	}	
	
	* Aumento de información de matrícula (supuesto: 1 turno en todos los niveles)
	*------------------------------------------------------------------------------
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local niv = `niv' + 1
		replace alum`niv'tot = `v' if alum`niv'tot == 0
		replace alum`niv' = `v' if alum`niv' == 0
		replace alum`niv'max = `v' if alum`niv'max == 0
	}	
	
	drop	alum alumtot alummax
	egen	alum = rowtotal(alum1 alum2 alum3 alum4 alum5 alum6 alum7 alum8 alum9)
	egen	alumtot = rowtotal(alum1tot alum2tot alum3tot alum4tot alum5tot alum6tot alum7tot alum8tot alum9tot)
	egen	alummax = rowtotal(alum1max alum2max alum3max alum4max alum5max alum6max alum7max alum8max alum9max)

	drop 	if alumtot == 0
	compress
	save	"$Input\fuie24\LE_Alum_FUIE24.dta", replace
	
	/*_____________________________________________________________________
	|                                                                      |
	|            	CÁLCULO DE ÁREAS PARA AMPLIACIÓN	 	               |
	|_____________________________________________________________________*/
	
	use		"$Input\fuie24\LE_Alum_FUIE24", replace
	
	* Cálculo de áreas techadas mínimas
	*-----------------------------------	
	merge 	1:1 cod_local using "$Input\fuie24\LE_Infra_FUIE24", keepusing(zona areatechp1 areatech)
	keep 	if _merge == 3
	drop 	_merge																// Mantener locales que tienen información del estado de la infraestructura y de alumnos.
	
	merge 	1:1 cod_local using "$Input\LE_BaseAdic", keepusing(codgeo)
	drop 	if _merge == 2
	drop	 _merge
	
	gen		areaminu1 = 15.96 if zona == 1 & (alum1 > 0 & alum1 <= 20)
	replace	areaminu1 = 9.38 if zona == 1 & (alum1 > 20 & alum1 <= 40)
	replace	areaminu1 = 7.47 if zona == 1 & (alum1 > 40 & alum1 != .)	
	replace	areaminu1 = 6.53 if zona == 2 & (alum1 > 0 & alum1 <= 150)
	replace areaminu1 = 6.06 if zona == 2 & (alum1 > 150 & alum1 <= 300)
	replace areaminu1 = 5.59 if zona == 2 & (alum1 > 300 & alum1 != .)	
	
	gen		areaminu2 = 17.17 if zona == 1 & (alum2 > 0 & alum2 <= 70)
	replace	areaminu2 = 8.95 if zona == 1 & (alum2 > 70 & alum2 <= 140)
	replace	areaminu2 = 4.79 if zona == 1 & (alum2 > 140 & alum2 != .)	
	replace	areaminu2 = 5.19 if zona == 2 & (alum2 > 0 & alum2 <= 210)
	replace areaminu2 = 4.53 if zona == 2 & (alum2 > 210 & alum2 <= 420)
	replace areaminu2 = 4.42 if zona == 2 & (alum2 > 420 & alum2 != .)		
	
	gen		areaminu3 = 7.39 if zona == 1 & (alum3 > 0 & alum3 <= 175)
	replace	areaminu3 = 5.19 if zona == 1 & (alum3 > 175 & alum3 <= 350)
	replace	areaminu3 = 5.02 if zona == 1 & (alum3 > 350 & alum3 != .)	
	replace	areaminu3 = 7.39 if zona == 2 & (alum3 > 0 & alum3 <= 291)
	replace areaminu3 = 5.29 if zona == 2 & (alum3 > 291 & alum3 <= 583)
	replace areaminu3 = 4.86 if zona == 2 & (alum3 > 583 & alum3 != .)		
	
	gen		areaminu4 = 8.31 if zona == 1 & (alum4 > 0 & alum4 <= 75)
	replace	areaminu4 = 6.05 if zona == 1 & (alum4 > 75 & alum4 <= 150)
	replace	areaminu4 = 5.06 if zona == 1 & (alum4 > 150 & alum4 != .)	
	replace	areaminu4 = 5.49 if zona == 2 & (alum4 > 0 & alum4 <= 170)
	replace areaminu4 = 3.74 if zona == 2 & (alum4 > 170 & alum4 <= 340)
	replace areaminu4 = 3.78 if zona == 2 & (alum4 > 340 & alum4 != .)	
	
	gen		areaminu5 = 25.91 if (alum5 > 0 & alum5 <= 50)
	replace	areaminu5 = 20.34 if (alum5 > 50 & alum5 <= 100)
	replace	areaminu5 = 16.89 if (alum5 > 100 & alum5 != .)	
	
	gen		areaminu6 = 14.67 if (alum6 > 0 & alum6 <= 45)
	replace	areaminu6 = 10.38 if (alum6 > 45 & alum6 <= 150)
	replace	areaminu6 = 7.92 if (alum6 > 150 & alum6 != .)	
	
	gen		areaminu7 = 6.28 if (alum7 > 0 & alum7 <= 260)
	replace	areaminu7 = 5.08 if (alum7 > 260 & alum7 <= 750)
	replace	areaminu7 = 3.90 if (alum7 > 750 & alum7 != .)	
	
	gen		areaminu8 = 5.51 if (alum8 > 0 & alum8 <= 125)
	replace	areaminu8 = 4.74 if (alum8 > 125 & alum8 <= 360)
	replace	areaminu8 = 3.69 if (alum8 > 360 & alum8 != .)	

	gen		areaminu9 = 5.33 if (alum9 > 0 & alum9 <= 80)
	replace	areaminu9 = 5.05 if (alum9 > 80 & alum9 <= 200)
	replace	areaminu9 = 4.06 if (alum9 > 200 & alum9 != .)	
	
	scalar	FPS = 0.7 															// Factor que considera si un local tiene primaria y secundaria para el cálculo de área mínima.
	
	forvalues i = 1/9 {
			gen areamin`i' = alum`i' * areaminu`i'
	}
	
	replace areamin2 = areamin2 * FPS if areamin3 > areamin2 & areamin2 != . & areamin3 != .
	replace areamin3 = areamin3 * FPS if areamin2 > areamin3 & areamin2 != . & areamin3 != .
	
	* Cálculo de áreas techadas reales y ratios de ampliación
	*----------------------------------------------------------
	merge 	1:1 cod_local using "$Input\fuie24\LE_Aterr_FUIE24", keepusing(areaterr)
	drop 	if _merge == 2
	drop	_merge
	replace areaterr = areatechp1 if areaterr == .
		
	forvalues i = 1/9 {
			gen areatech`i' = (alum`i' / alum) * areatech
			gen ramp`i' = (areamin`i' - areatech`i') / areatech if alum`i' > 0
			gen	int_amp`i' = ramp`i' >= 0.1 & alum`i' > 0
			gen sc_amp`i' = ramp`i' <= -0.1 & alum`i' > 0
	}

	* Optimización de las áreas de ampliación
	*-----------------------------------------	
	forvalues i = 1/3 {
			gen 	areaamp`i' = (areamin`i' - areatech`i') if int_amp`i' == 1
			gen 	areasc`i' = 0.8 * (areatech`i' - areamin`i') if sc_amp`i' == 1
			egen 	areaamp`i'_dz = sum(areaamp`i') if codgeo != "", by (codgeo zona)
			egen 	areasc`i'_dz = sum(areasc`i') if codgeo != "", by (codgeo zona)
			gen 	areard`i'_dz = min(areasc`i'_dz * 0.5, areaamp`i'_dz) if areasc`i'_dz >= 60 & areasc`i'_dz != . & areaamp`i'_dz != .
			gen 	factrd`i'_dz = min(areard`i'_dz / areaamp`i'_dz, 1) if areard`i'_dz != . & areaamp`i'_dz != .
			replace	areaamp`i' = areaamp`i' * (1 - factrd`i'_dz) if factrd`i'_dz != .
	}
	
	forvalues i = 4/9 {
			gen 	areaamp`i' = (areamin`i' - areatech`i') if int_amp`i' == 1
	}
	egen 	areaamp = 	rowtotal(areaamp1 areaamp2 areaamp3 areaamp4 areaamp5 ///
						areaamp6 areaamp7 areaamp8 areaamp9)
	
	compress
	save 	"$Input\fuie24\LE_Amp_FUIE24.dta", replace

	/*_____________________________________________________________________
	|                                                                      |
	|       			IDONEIDAD DE SSHH Y BEBEDEROS				       |
	|_____________________________________________________________________*/
	
	use 	"$Raw\fuie24\local_sec700", clear
	
	* Preparar variables iniciales
	*------------------------------
	destring codlocal, gen(cod_local)
	rename 	id_edif cod_edif

	forvalues i = 1/9 {
			replace cod_edif = "E0`i'" if cod_edif == "E`i'" | cod_edif == "ED`i'" | cod_edif == "EO`i'" | 	cod_edif == "`i'"
	}
	replace cod_edif = "E01" if cod_edif == "0" | cod_edif == "(1)" | cod_edif == "-1" | cod_edif == "TE0" | cod_edif == "---" 
	replace cod_edif = "E19" if cod_edif == "e19"

	merge 	m:1 cod_local cod_edif using "$Input\fuie24\Edif_Infra_FUIE24", keepusing(cod_local cod_edif)
	keep 	if _merge == 3
	drop 	_merge																		// Mantener ambientes en edificaciones que tienen información del estado de la infraestructura y de SSHH.
	
	gen		amb1 = strpos(p701_4,"A") > 0
	gen		amb2 = strpos(p701_4,"B") > 0
	gen		amb3 = strpos(p701_4,"F") > 0
	gen		amb4 = strpos(p701_4,"D") > 0
	gen		amb5 = strpos(p701_4,"E") > 0
	gen		amb6 = strpos(p701_4,"M") > 0
	gen		amb7 = strpos(p701_4,"T") > 0 | strpos(p701_4,"S") > 0
	gen		amb8 = strpos(p701_4,"K") > 0 | strpos(p701_4,"P") > 0
	gen		amb9 = strpos(p701_4,"L") > 0
	
	* Cálculo de baños e inodoros
	*-----------------------------
	gen		piso = int(p701_2)
	replace piso = 1 if piso < 1 | piso == .
	replace piso = 6 if piso > 6 & piso != .
	
	foreach v of varlist p701_6_7 p701_6_8 p701_11_* p701_12_2 p701_12_3 p701_12_4 p701_12_5 p701_12_6 p701_12_7 p701_13_* p701_14_* {
		replace `v' = abs(`v')
	}
	replace p701_6_7 = p701_12_3 if p701_6_7 > p701_12_3 & p701_6_7 != .
	egen	lavop = rowtotal(p701_13_12 p701_13_22), missing
	replace p701_6_8 = lavop if p701_6_8 > lavop & p701_6_8 != .
	
	gen 	sshh = (p701_3 != "140" & p701_3 != "141") | ((p701_12_3 != 0 & p701_12_3 != .) & ((p701_13_12 != 0 & p701_13_12 != .) | (p701_13_22 != 0 & p701_13_22 != .)))		// Es SS.HH.? Se considera vestidor con 1 inodoro y 1 lavatorio operativo.
	drop	if sshh == 0
	
	gen		_ino = p701_12_3
	gen		_inon = p701_12_3 if p701_12_1 == "01"
	
	gen		ino5 = _ino if piso == 1 & amb5 == 1
	gen 	ino1 = _inon if piso == 1 & amb5 == 0 & amb1 == 1						// Para Inicial y EBE, solo SSHH de primer piso. Para Inicial, solo inodoros para niños.
	gen		ino2 = _ino if ino5 == . & ino1 == . & amb2 == 1
	gen		ino3 = _ino if ino5 == . & ino1 == . & amb2 == 0 & amb3 == 1
	gen		ino8 = _ino if ino5 == . & ino1 == . & amb2 == 0 & amb3 == 0 & amb8 == 1
	gen		ino9 = _ino if ino5 == . & ino1 == . & amb2 == 0 & amb3 == 0 & amb8 == 0 & amb9 == 1
	gen		ino7 = _ino if ino5 == . & ino1 == . & amb2 == 0 & amb3 == 0 & amb8 == 0 & amb9 == 0 & amb7 == 1
	gen		ino4 = _ino if ino5 == . & ino1 == . & amb2 == 0 & amb3 == 0 & amb8 == 0 & amb9 == 0 & amb7 == 0 & amb4 == 1
	gen 	ino6 = _ino if ino5 == . & ino1 == . & amb2 == 0 & amb3 == 0 & amb8 == 0 & amb9 == 0 & amb7 == 0 & amb4 == 0 & amb6 == 1	// Número de inodoros por nivel.
	egen	ino = rowtotal(ino1 ino2 ino3 ino4 ino5 ino6 ino7 ino8 ino9)			// Número de inodoros en uso.
	drop	_ino
	
	egen	_aux = rowtotal(amb*)
	gen		amb_comp = _aux > 1
	egen	amb_comp_le = sum(amb_comp), by(cod_local)
	gen		excl = amb_comp_le == 0													// Local solo contiene ambientes exclusivos a un nivel.
	drop	amb_comp* _aux
	
	gen 	_inod = p701_12_7
	gen 	inod = _inod if (amb1 == 0 & amb5 == 0) | piso == 1 | excl == 0			// Número de inodoros con función de descarga.
	replace inod = 0 if inod == .
	drop 	_inod																	// Se excluyen ambientes de Inicial y EBE que no estén en primer piso en locales con ambientes exclusivos.
	
	gen  	acc_ino = abs(p701_6_7)													// Números de inodoros accesibles en uso.
			
	gen		_uri = 	p701_14_12 + 3 * p701_14_22
	gen		uri5 = _uri if piso == 1 & amb5 == 1									// Para EBE, solo SSHH de primer piso. No se considera Inicial para urinarios.
	gen		uri2 = _uri if uri5 == . & amb2 == 1
	gen		uri3 = _uri if uri5 == . & amb2 == 0 & amb3 == 1
	gen		uri8 = _uri if uri5 == . & amb2 == 0 & amb3 == 0 & amb8 == 1
	gen		uri9 = _uri if uri5 == . & amb2 == 0 & amb3 == 0 & amb8 == 0 & amb9 == 1
	gen		uri7 = _uri if uri5 == . & amb2 == 0 & amb3 == 0 & amb8 == 0 & amb9 == 0 & amb7 == 1
	gen		uri4 = _uri if uri5 == . & amb2 == 0 & amb3 == 0 & amb8 == 0 & amb9 == 0 & amb7 == 0 & amb4 == 1
	gen 	uri6 = _uri if uri5 == . & amb2 == 0 & amb3 == 0 & amb8 == 0 & amb9 == 0 & amb7 == 0 & amb4 == 0 & amb6 == 1	// Número de urinarios por nivel.
	egen	uri = rowtotal(uri2 uri3 uri4 uri5 uri6 uri7 uri8 uri9)					// Número de urinarios en uso.
	drop 	_uri
	
	gen 	urid = p701_14_16 + 3 * p701_14_26 if (amb1 == 0 & amb5 == 0) | piso == 1 | excl == 0
	replace urid = 0 if urid == .
	
	collapse 	(sum) acc_ino ino* uri* sshh, by(cod_local cod_edif)
	
	* Cálculo de edificaciones con SSHH, y con agua y saneamiento
	*-------------------------------------------------
	merge 	1:1 cod_local cod_edif using "$Input\fuie24\Edif_Infra_FUIE24", keepusing(cod_local cod_edif codlocal p401_13_1 p401_13_2)
	drop 	if _merge == 1
	drop 	_merge																	// Mantener únicamente las edificaciones con información del estado de la infraestructura (Edif_Infra_FUIE23)
	
	gen		sshh_edif = sshh > 0 & sshh != .
	gen 	ad_edif = p401_13_1 == "1" & p401_13_2 == "1" & sshh_edif > 0
	gen		num_edif = 1
	
	collapse	(sum) sshh sshh_edif ad_edif acc_ino ino* uri* num_edif (firstnm) codlocal, by(cod_local)

	* Cálculo de brecha de inodoros y urinarios por local
	*-----------------------------------------------------
	merge	1:1 cod_local using "$Input\fuie24\LE_Alum_FUIE24"
	drop	if _merge == 2
	drop	_merge alum3 alum
	rename	alum3max alum3
	egen	alum = rowtotal(alum1 alum2 alum3 alum4 alum5 alum6 alum7 alum8 alum9)

	forvalues i = 1/9 {
			replace alum`i' = 0 if alum`i' == .
			replace ino`i' = 0 if ino`i' == .
			gen alum_inor`i' = ceil(max(alum`i' - ino`i' * 30, 0))
	} 
	forvalues i = 2/9 {
			replace uri`i' = 0 if uri`i' == .
	}
	egen	alum_inor = rowtotal(alum_inor1 alum_inor2 alum_inor3 alum_inor4 alum_inor5 alum_inor6 alum_inor7 alum_inor8 alum_inor9)
	
	gen		alum_ino = min(ino * 30, alum) if ino != .
	gen 	alum_inodr = ceil(alum_ino - (alum_ino * min(inod / ino,1))) if ino > 0 & ino != .
	replace	alum_inodr = ceil(alum_ino) if ino == 0
		
	gen 	alum_urir2 = ceil(max(alum2 * 0.50 - ino2 * 30 * 0.50 - uri2 * 30, 0))
	gen 	alum_urir3 = ceil(max(alum3 * 0.50 - ino3 * 30 * 0.50 - uri3 * 30, 0))
	gen 	alum_urir4 = ceil(max(alum4 * 0.55 - ino4 * 30 * 0.55 - uri4 * 30, 0))
	gen 	alum_urir5 = ceil(max(alum5 * 0.60 - ino5 * 30 * 0.60 - uri5 * 30, 0))
	gen 	alum_urir6 = ceil(max(alum6 * 0.67 - ino6 * 30 * 0.67 - uri6 * 30, 0))
	gen 	alum_urir7 = ceil(max(alum7 * 0.42 - ino7 * 30 * 0.42 - uri7 * 30, 0))
	gen 	alum_urir8 = ceil(max(alum8 * 0.30 - ino8 * 30 * 0.30 - uri8 * 30, 0))
	gen 	alum_urir9 = ceil(max(alum9 * 0.36 - ino9 * 30 * 0.36 - uri9 * 30, 0))
	egen	alum_urir = rowtotal(alum_urir2 alum_urir3 alum_urir4 alum_urir5 alum_urir6 alum_urir7 alum_urir8 alum_urir9)
	
	gen		alum_uri2 = ceil(min(ino2 * 30 * 0.50 + uri2 * 30, alum2 * 0.50))
	gen		alum_uri3 = ceil(min(ino3 * 30 * 0.50 + uri3 * 30, alum3 * 0.50))
	gen		alum_uri4 = ceil(min(ino4 * 30 * 0.55 + uri4 * 30, alum4 * 0.55))
	gen		alum_uri5 = ceil(min(ino5 * 30 * 0.60 + uri5 * 30, alum5 * 0.60))
	gen		alum_uri6 = ceil(min(ino6 * 30 * 0.67 + uri6 * 30, alum6 * 0.67))
	gen		alum_uri7 = ceil(min(ino7 * 30 * 0.42 + uri7 * 30, alum7 * 0.42))
	gen		alum_uri8 = ceil(min(ino8 * 30 * 0.30 + uri8 * 30, alum8 * 0.30))
	gen		alum_uri9 = ceil(min(ino9 * 30 * 0.36 + uri9 * 30, alum9 * 0.36))
	
	forvalues i = 2/9 {
			gen 	alum_uridr`i' = ceil(alum_uri`i' - (alum_uri`i' * min(urid / uri,1))) if uri > 0 & uri != .
			replace alum_uridr`i' = ceil(alum_uri`i') if uri == 0
	}
	egen	alum_uridr = rowtotal(alum_uridr2 alum_uridr3 alum_uridr4 alum_uridr5 alum_uridr6 alum_uridr7 alum_uridr8 alum_uridr9)

	gen		int_sshh = sshh == 0 | ad_edif < sshh_edif | ad_edif == 0	
	
	gen		ba_inoamp = alum_inor if int_sshh == 0	
	replace	ba_inoamp = alum if int_sshh == 1
	gen		ba_inoreh = alum_inodr if int_sshh == 0 & alum != . & alum > ba_inoamp
	
	gen		alumh = ceil(alum2 * 0.50) + ceil(alum3 * 0.50) + ceil(alum4 * 0.55) + ceil(alum5 * 0.60) + ceil(alum6 * 0.67) + ceil(alum7 * 0.42) + ceil(alum8 * 0.30) + ceil(alum9 * 0.36)
	gen		ba_uriamp = alum_urir if int_sshh == 0
	replace ba_uriamp =	alumh if int_sshh == 1
	gen		ba_urireh = alum_uridr if int_sshh == 0 & alumh != . & alumh > ba_uriamp
	
	* Cálculo de brecha de bebederos
	*--------------------------------	
	merge	1:1 codlocal using "$Raw\fuie24\local_lineal_02", keepusing(p209*)
	drop 	if _merge == 2
	drop 	_merge																
	
	gen		beb = p209_b_est if p209 == "1"
	replace beb = 0	if beb == .
	gen 	b_beb = max(max(1, ceil(alummax / 40)) - beb, 0)
	
	compress
	save 	"$Input\fuie24\LE_SSHH_FUIE24.dta", replace
	
	/*_____________________________________________________________________
	|                                                                      |
	|      				INTERVENCIÓN EN ACCESIBILIDAD	 	  		       |
	|_____________________________________________________________________*/
	
	use 	"$Input\fuie24\Edif_Infra_FUIE24.dta", clear

	gen		acc_edif1 = numpisos == 1 & (p401_14_1 == "1" | p401_14_21 == "1" | p401_14_22 == "1") & (p401_16 == "01" | p401_16 == "03")
	gen		acc_edif2 = numpisos == 2 & (p401_14_22 == "1" | (p401_14_41 == "1" & p401_14_42 == "1" & p401_14_43 >= 2023)) & (p401_16 == "01" | p401_16 == "03")
	gen		acc_edif3 = numpisos == 3 & (p401_14_22 == "1" | (p401_14_41 == "1" & p401_14_42 == "1" & p401_14_43 >= 2023)) & (p401_16 == "01" | p401_16 == "03")
	gen		acc_edif4 = numpisos == 4 & (p401_14_22 == "1" | (p401_14_41 == "1" & p401_14_42 == "1" & p401_14_43 >= 2023)) & (p401_16 == "01" | p401_16 == "03")
	gen		acc_edif5 = numpisos == 5 & (p401_14_22 == "1" | (p401_14_41 == "1" & p401_14_42 == "1" & p401_14_43 >= 2023)) & (p401_16 == "01" | p401_16 == "03")
	gen		acc_edif6 = numpisos == 6 & (p401_14_22 == "1" | (p401_14_41 == "1" & p401_14_42 == "1" & p401_14_43 >= 2023)) & (p401_16 == "01" | p401_16 == "03")
	gen		acc_edif = acc_edif1 == 1 | acc_edif2 == 1 | acc_edif3 == 1 | acc_edif4 == 1 | acc_edif5 == 1 | acc_edif6 == 1
	gen		accno_edif1 = numpisos == 1 & acc_edif == 0
	gen		accno_edif2_5 = numpisos > 1 & acc_edif == 0
	gen		acc_pisos = numpisos if acc_edif == 1
	gen		num_edif = 1
	
	save 	"$Input\fuie24\Edif_Acc_FUIE24.dta", replace
	
	collapse	(sum) acc_edif* accno_edif* acc_pisos numpisos areatech areatechp1 num_edif (max) zona, by(cod_local)
	
	* Cálculo de áreas de accesibilidad mínimas
	*-------------------------------------------	
	merge	1:1 cod_local using "$Input\fuie24\LE_Alum_FUIE24"
	keep	if _merge == 3
	drop	_merge

	gen		areaminau1 = 22.80 if zona == 1 & (alum1 > 0 & alum1 <= 20)
	replace	areaminau1 = 13.40 if zona == 1 & (alum1 > 20 & alum1 <= 40)
	replace	areaminau1 = 10.67 if zona == 1 & (alum1 > 40 & alum1 != .)	
	replace	areaminau1 = 9.33 if zona == 2 & (alum1 > 0 & alum1 <= 150)
	replace areaminau1 = 8.66 if zona == 2 & (alum1 > 150 & alum1 <= 300)
	replace areaminau1 = 7.99 if zona == 2 & (alum1 > 300 & alum1 != .)	
	
	gen		areaminau2 = 18.47 if zona == 1 & (alum2 > 0 & alum2 <= 70)
	replace	areaminau2 = 15.13 if zona == 1 & (alum2 > 70 & alum2 <= 140)
	replace	areaminau2 = 8.95 if zona == 1 & (alum2 > 140 & alum2 != .)	
	replace	areaminau2 = 9.35 if zona == 2 & (alum2 > 0 & alum2 <= 210)
	replace areaminau2 = 8.61 if zona == 2 & (alum2 > 210 & alum2 <= 420)
	replace areaminau2 = 8.36 if zona == 2 & (alum2 > 420 & alum2 != .)		
	
	gen		areaminau3 = 12.55 if zona == 1 & (alum3 > 0 & alum3 <= 175)
	replace	areaminau3 = 9.20 if zona == 1 & (alum3 > 175 & alum3 <= 350)
	replace	areaminau3 = 9.03 if zona == 1 & (alum3 > 350 & alum3 != .)	
	replace	areaminau3 = 12.55 if zona == 2 & (alum3 > 0 & alum3 <= 291)
	replace areaminau3 = 9.52 if zona == 2 & (alum3 > 291 & alum3 <= 583)
	replace areaminau3 = 9.07 if zona == 2 & (alum3 > 583 & alum3 != .)		
	
	gen		areaminau4 = 9.81 if zona == 1 & (alum4 > 0 & alum4 <= 75)
	replace	areaminau4 = 7.55 if zona == 1 & (alum4 > 75 & alum4 <= 150)
	replace	areaminau4 = 6.56 if zona == 1 & (alum4 > 150 & alum4 != .)	
	replace	areaminau4 = 6.99 if zona == 2 & (alum4 > 0 & alum4 <= 170)
	replace areaminau4 = 5.24 if zona == 2 & (alum4 > 170 & alum4 <= 340)
	replace areaminau4 = 5.28 if zona == 2 & (alum4 > 340 & alum4 != .)	
	
	gen		areaminau5 = 31.25 if (alum5 > 0 & alum5 <= 50)
	replace	areaminau5 = 26.39 if (alum5 > 50 & alum5 <= 100)
	replace	areaminau5 = 23.39 if (alum5 > 100 & alum5 != .)	
	
	gen		areaminau6 = 20.95 if (alum6 > 0 & alum6 <= 45)
	replace	areaminau6 = 14.83 if (alum6 > 45 & alum6 <= 150)
	replace	areaminau6 = 11.31 if (alum6 > 150 & alum6 != .)	
	
	gen		areaminau7 = 8.98 if (alum7 > 0 & alum7 <= 260)
	replace	areaminau7 = 7.26 if (alum7 > 260 & alum7 <= 750)
	replace	areaminau7 = 5.56 if (alum7 > 750 & alum7 != .)	
	
	gen		areaminau8 = 7.88 if (alum8 > 0 & alum8 <= 125)
	replace	areaminau8 = 6.77 if (alum8 > 125 & alum8 <= 360)
	replace	areaminau8 = 5.27 if (alum8 > 360 & alum8 != .)	
	
	gen		areaminau9 = 21.67 if (alum9 > 0 & alum9 <= 80)
	replace	areaminau9 = 16.19 if (alum9 > 80 & alum9 <= 200)
	replace	areaminau9 = 11.15 if (alum9 > 200 & alum9 != .)	
	
	scalar	FPS = 0.7 															// Factor que considera si un local tiene primaria y secundaria para el cálculo de área mínima.
	
	forvalues i = 1/9 {
			gen areamina`i' = alum`i' * areaminau`i'
	}
	
	replace areamina2 = areamina2 * FPS if areamina3 > areamina2 & areamina2 != . & areamina3 != .
	replace areamina3 = areamina3 * FPS if areamina2 > areamina3 & areamina2 != . & areamina3 != .
	
	egen	areamina = rowtotal(areamina1 areamina2 areamina3 areamina4 areamina5 ///
					   areamina6 areamina7 areamina8 areamina9)
	
	* Cálculo de requerimiento de ampliación de terrenos
	*----------------------------------------------------
	merge 	1:1 cod_local using "$Input\fuie24\LE_Aterr_FUIE24", keepusing(areaterr)
	drop 	if _merge == 2
	drop	_merge
	replace areaterr = areatechp1 if areaterr == .
	
	gen		calc_ampt = areaterr + 10 >= areatechp1								// No considerar inconsistencias (> 10 m2 = inconsistencia).
	gen		int_ampt = areamina > areaterr if calc_ampt == 1
	replace int_ampt = 0 if calc_ampt == 0
	
	* Cálculo de rampas y ascensores requeridos
	*-------------------------------------------
	gen 	num_niv = 0
	forvalues i = 1/9 {
			replace num_niv = num_niv + 1 if alum`i' > 0 & alum`i' != .
	}
	replace	num_niv = 1 if num_niv == 0
	
	gen		acc_rr = max(min(num_niv, num_edif) - acc_edif, 0) if int_ampt == 0
	gen		acc_rr2_5 = min(acc_rr, accno_edif2_5) if int_ampt == 0
	gen		acc_rr1 = max(acc_rr-acc_rr2_5, 0) if int_ampt == 0
	gen		acc_ar = max(min(num_niv, num_edif - (acc_edif + accno_edif1)), 0) if int_ampt == 1 & zona == 2	

	* Cálculo de inodoros accesibles requeridos
	*-------------------------------------------
	merge 	1:1 cod_local using "$Input\fuie24\LE_SSHH_FUIE24", keepusing(acc_ino)
	drop 	if _merge == 2
	drop	_merge
	replace acc_ino = 0 if acc_ino == .
	
	gen		acc_ir = max(2 * num_niv - acc_ino, 0)
	
	compress
	save 	"$Input\fuie24\LE_Acc_FUIE24.dta", replace
	
	/*_____________________________________________________________________
	|                                                                      |
	|            	ESTADO DE ELEMENTOS NO ESTRUCTURALES               	   |
	|_____________________________________________________________________*/	
	
	* Cálculo de elementos no estructurales en aulas
	*-------------------------------------------------
	use 	"$Raw\fuie24\local_sec500", clear
	destring codlocal, gen(cod_local)
	rename 	id_edif cod_edif
	
	forvalues i = 1/9 {
			replace cod_edif = "E0`i'" if cod_edif == "E`i'" | cod_edif == "ED`i'" | cod_edif == "EO`i'" | 	cod_edif == "`i'"
	}
	replace cod_edif = "E01" if cod_edif == "0" | cod_edif == "SI" | cod_edif == "--"
	replace cod_edif = "E19" if cod_edif == "e19"
	
	merge 	m:1 cod_local cod_edif using "$Input\fuie24\Edif_Infra_FUIE24", keepusing(cod_local cod_edif)
	keep 	if _merge == 3
	drop 	_merge																		// Mantener ambientes en edificaciones que tienen información del estado de la infraestructura y de aulas.
	
	foreach v of varlist p501_10_3 p501_11_2 p501_12_3 p501_12_5 p501_15_2 {
		replace `v' = "01" if `v' == "1"
		replace `v' = "02" if `v' == "2"
		replace `v' = "03" if `v' == "3"
		replace `v' = "04" if `v' == "4"
		replace `v' = ""   if `v' != "01" & `v' != "02" & `v' != "03" & `v' != "04"
	}
	
	replace p501_10_2 = "08" if (p501_10_1 == 0 | p501_10_1 == .) & (p501_10_2 == "--" | p501_10_2 == "")
	replace p501_10_2 = "07" if p501_10_1 > 0 & p501_10_1 != . & (p501_10_2 == "--" | p501_10_2 == "")				// Corregir material de construcción de puertas.
	replace p501_12_2 = "06" if (p501_12_1 == 0 | p501_12_1 == .) & (p501_12_2 == "--" | p501_12_2 == "")
	replace p501_12_2 = "05" if p501_12_1 > 0 & p501_12_1 != . & (p501_12_2 == "--" | p501_12_2 == "")				// Corregir material de construcción de marco de ventana.
	replace p501_13_1 = "09" if p501_13_1 == "--" | p501_13_1 == ""													// Corregir material de construcción de paredes.
	replace p501_14_1 = "10" if p501_14_1 == "--" | p501_14_1 == ""													// Corregir material de construcción de techo.
	replace p501_15_1 = "07" if p501_15_1 == "--" | p501_15_1 == ""													// Corregir material de construcción de piso.

	replace	p501_10_1 = 1 if p501_10_2 != "09" & (p501_10_1 < 1 | p501_10_1 == .) & p501_10_3 != ""
	replace p501_10_1 = 1 if p501_10_2 == "08" & (p501_10_1 < 1 | p501_10_1 == .) 										// Número mínimo: 1 puerta si no tiene pero lo requiere.
	replace	p501_10_1 = 20 if p501_10_1 > 20 & p501_10_1 != .
	replace	p501_11_1 = 1 if (p501_11_1 < 1 | p501_11_1 == .) & p501_11_2 != "" & p501_11_2 != "04"
	replace	p501_11_1 = 20 if p501_11_1 > 20 & p501_11_1 != .
	replace	p501_12_1 = 1 if p501_12_2 != "07" & (p501_12_1 < 1 | p501_12_1 == .) & p501_12_3 != "" & p501_12_5 != ""	// Número mínimo elementos por ambiente: 1, solo si se registró estado de conservación; de lo contrario: 0.
	replace p501_12_1 = 1 if p501_12_2 == "06" & (p501_12_1 < 1 | p501_12_1 == .)										// Número mínimo: 1 ventana si no tiene pero lo requiere.
	replace	p501_12_1 = 20 if p501_12_1 > 20 & p501_12_1 != .															// Número máximo elementos por ambiente: 20.
	
	gen 	puertasb = p501_10_1 if p501_10_2 == "09" | (p501_10_2 != "09" & p501_10_3 == "01")
	gen 	puertasr = p501_10_1 if p501_10_2 != "09" & p501_10_3 == "02"
	gen 	puertasm = p501_10_1 if p501_10_2 != "09" & (p501_10_3 == "03" | p501_10_3 == "04" | p501_10_3 == "")
	
	gen		ventanab = p501_12_1 if p501_12_2 == "07" | (p501_12_2 != "07" & p501_12_3 == "01")
	gen 	ventanar = p501_12_1 if p501_12_2 != "07" & p501_12_3 == "02"
	gen 	ventanam = p501_12_1 if p501_12_2 != "07" & (p501_12_3 == "03" | p501_12_3 == "04" | p501_12_3 == "")
	
	gen 	cerradb = p501_11_1 if p501_11_2 == "01"
	gen 	cerradr = p501_11_1 if p501_11_2 == "02"
	gen 	cerradm = p501_11_1 if p501_11_2 == "03"
	gen		cerradf = p501_11_1 if p501_11_2 == "04" | p501_11_2 == ""
	
	gen 	pisob = p501_15_2 == "01"
	gen 	pisor = p501_15_2 == "02"
	gen 	pisom = p501_15_2 == "03" | p501_15_2 == "04" | p501_15_2 == ""
	
	gen		vidriob = p501_12_1 if p501_12_5 == "01"
	gen 	vidrior = p501_12_1 if p501_12_5 == "02"
	gen 	vidriom = p501_12_1 if p501_12_5 == "03" | p501_12_5 == "04" | p501_12_5 == ""
		
	gen		aulas = 1
	keep	puertas* ventana* cerrad* piso* vidrio* aulas cod_local cod_edif

	save	"$Input\fuie24\Amb_EneAulas_FUIE24.dta", replace
	
	* Cálculo de elementos no estructurales en otros espacios
	*--------------------------------------------------------
	use 	"$Raw\fuie24\local_sec600", clear
	destring codlocal, gen(cod_local)
	rename 	id_edif cod_edif
	
	forvalues i = 1/9 {
			replace cod_edif = "E0`i'" if cod_edif == "E`i'" | cod_edif == "ED`i'" | cod_edif == "EO`i'" | 	cod_edif == "`i'"
	}
	replace cod_edif = "E01" if cod_edif == "0" | substr(cod_edif,1,1) != "E"
	replace cod_edif = "E19" if cod_edif == "e19"
	
	merge 	m:1 cod_local cod_edif using "$Input\fuie24\Edif_Infra_FUIE24", keepusing(cod_local cod_edif)
	keep 	if _merge == 3
	drop 	_merge																		// Mantener ambientes en edificaciones que tienen información del estado de la infraestructura y de aulas.
	
	foreach v of varlist p601_10_3 p601_11_2 p601_12_3 p601_12_5 p601_15_2 {
		replace `v' = "01" if `v' == "1"
		replace `v' = "02" if `v' == "2"
		replace `v' = "03" if `v' == "3"
		replace `v' = "04" if `v' == "4"
		replace `v' = ""   if `v' != "01" & `v' != "02" & `v' != "03" & `v' != "04"
	}
	
	replace p601_10_2 = "08" if (p601_10_1 == 0 | p601_10_1 == .) & (p601_10_2 == "--" | p601_10_2 == "" | p601_10_2 == "0")
	replace p601_10_2 = "07" if p601_10_1 > 0 & p601_10_1 != . & (p601_10_2 == "--" | p601_10_2 == "" | p601_10_2 == "0")		// Corregir material de construcción de puertas.
	replace p601_12_2 = "06" if (p601_12_1 == 0 | p601_12_1 == .) & (p601_12_2 == "--" | p601_12_2 == "")
	replace p601_12_2 = "05" if p601_12_1 > 0 & p601_12_1 != . & (p601_12_2 == "--" | p601_12_2 == "")							// Corregir material de construcción de marco de ventana.
	replace p601_13_1 = "09" if p601_13_1 == "--" | p601_13_1 == ""																// Corregir material de construcción de paredes.
	replace p601_14_1 = "10" if p601_14_1 == "--" | p601_14_1 == ""																// Corregir material de construcción de techo.
	replace p601_15_1 = "07" if p601_15_1 == "--" | p601_15_1 == ""																// Corregir material de construcción de piso.
	
	replace	p601_10_1 = 1 if p601_10_2 != "09" & (p601_10_1 < 1 | p601_10_1 == .) & p601_10_3 != ""
	replace p601_10_1 = 1 if p601_10_2 == "08" & (p601_10_1 < 1 | p601_10_1 == .) 										// Número mínimo: 1 puerta si no tiene pero lo requiere.
	replace	p601_10_1 = 20 if p601_10_1 > 20 & p601_10_1 != .
	replace	p601_11_1 = 1 if (p601_11_1 < 1 | p601_11_1 == .) & p601_11_2 != "" & p601_11_2 != "04"
	replace	p601_11_1 = 20 if p601_11_1 > 20 & p601_11_1 != .
	replace	p601_12_1 = 1 if p601_12_2 != "07" & (p601_12_1 < 1 | p601_12_1 == .) & p601_12_3 != "" & p601_12_5 != ""	// Número mínimo elementos por ambiente: 1, solo si se registró estado de conservación; de lo contrario: 0.
	replace p601_12_1 = 1 if p601_12_2 == "06" & (p601_12_1 < 1 | p601_12_1 == .)										// Número mínimo: 1 ventana si no tiene pero lo requiere.
	replace	p601_12_1 = 20 if p601_12_1 > 20 & p601_12_1 != .															// Número máximo elementos por ambiente: 20.
	
	gen 	puertasb = p601_10_1 if p601_10_2 == "09" | (p601_10_2 != "09" & p601_10_3 == "01")
	gen 	puertasr = p601_10_1 if p601_10_2 != "09" & p601_10_3 == "02"
	gen 	puertasm = p601_10_1 if p601_10_2 != "09" & (p601_10_3 == "03" | p601_10_3 == "04" | p601_10_3 == "")
	
	gen		ventanab = p601_12_1 if p601_12_2 == "09" | (p601_12_2 != "09" & p601_12_3 == "01")
	gen 	ventanar = p601_12_1 if p601_12_2 != "09" & p601_12_3 == "02"
	gen 	ventanam = p601_12_1 if p601_12_2 != "09" & (p601_12_3 == "03" | p601_12_3 == "04" | p601_12_3 == "")
	
	gen 	cerradb = p601_11_1 if p601_11_2 == "01"
	gen 	cerradr = p601_11_1 if p601_11_2 == "02"
	gen 	cerradm = p601_11_1 if p601_11_2 == "03" 
	gen 	cerradf = p601_11_1 if p601_11_2 == "04" | p601_11_2 == ""
	
	gen 	pisob = p601_15_2 == "01"
	gen 	pisor = p601_15_2 == "02"
	gen 	pisom = p601_15_2 == "03" | p601_15_2 == "04" | p601_15_2 == ""
	
	gen		vidriob = p601_12_1 if p601_12_5 == "01"
	gen 	vidrior = p601_12_1 if p601_12_5 == "02"
	gen 	vidriom = p601_12_1 if p601_12_5 == "03" | p601_12_5 == "04" | p601_12_5 == ""
	
	gen		otesp = 1
	keep	puertas* ventana* cerrad* piso* vidrio* otesp cod_local cod_edif
	
	* Cálculo de elementos no estructurales por edificación y por local educativo
	*----------------------------------------------------------------------------
	append 	using "$Input\fuie24\Amb_EneAulas_FUIE24.dta"
	
	collapse	(sum) puertas* ventana* cerrad* piso* vidrio* aulas otesp, by(cod_local cod_edif)
	
	merge 	1:1 cod_local cod_edif using "$Input\fuie24\Edif_Infra_FUIE24", keepusing(codlocal ratiodem areasust areari arearc areatech)
	drop 	if _merge == 1
	drop 	_merge																// Mantener únicamente las edificaciones con información del estado de la infraestructura (Edif_Infra_FUIE23)
	keep 	if areasust == . & areari == . & arearc == . & ratiodem < 0.7 		// Solo mantener edificaciones sin intervención de sustitución o reforzamiento.
	
	gen 	num_ene2b = puertasb + ventanab
	gen 	num_ene2r = puertasr + ventanar 
	gen 	num_ene2m = puertasm + ventanam
	
	gen		area_ene1 = areatech * (0 * pisob + 0.5 * pisor + 1 * pisom) / (pisob + pisor + pisom)
	gen		area_ene2 = areatech * (0 * num_ene2b + 0.5 * num_ene2r  + 1 * num_ene2m) / (num_ene2b + num_ene2r + num_ene2m)
	gen		area_ene3 = areatech * (0 * cerradb + 0.25 * cerradr + 0.75 * cerradm + 1 * cerradf) / (cerradb + cerradr + cerradm + cerradf)
	gen		area_ene4 = areatech * (0 * vidriob + 0.5 * vidrior + 1 * vidriom) / (vidriob + vidrior + vidriom)
	
	keep	cod_local cod_edif areatech area_ene* aulas otesp
	compress
	save	"$Input\fuie24\Edif_Ene_FUIE24.dta", replace
	
	gen		num_edif = 1
	collapse	(sum) areatech area_ene* aulas otesp num_edif, by(cod_local)
	
	compress
	save	"$Input\fuie24\LE_Ene_FUIE24.dta", replace

	/*_____________________________________________________________________
	|                                                                      |
	|            CÁLCULO DE COSTO DE ACCESO DE AGUA Y DESAGÜE        	   |
	|_____________________________________________________________________*/	
	
	use 	"$Raw2\fuie24\241130\local_ssbb", clear
	destring codlocal, gen(cod_local)
	
	merge 	m:1 cod_local using "$Input\fuie24\LE_Infra_FUIE24", keepusing(clima)
	keep 	if _merge == 3
	drop 	_merge																// Mantener locales que tienen información del estado de la infraestructura y de acceso a agua y desagüe.
	
	* Ajuste de valores previo (ajustes que no involucran análisis de consistencia)
	*--------------------------------------------------------------------------------
	foreach v of varlist terreno_1 terreno_2 terreno_3 terreno_4 {
		replace `v' = "X" if `v' == "x" | `v' == "0" | `v' == "1" | `v' == "S" | `v' == "N"
		replace `v' = "" if `v' == "-"
	}
	
	* Elaboración de indicadores
	*----------------------------
	gen		aa_rp = tipo == "RED PÚBLICA (AGUA POTABLE)"				if cuadro == "C202"	& predomina == "X"	// Acceso de agua: red pública (agua potable).
	gen		aa_pp = tipo == "PILÓN DE USO PÚBLICO (AGUA POTABLE)"		if cuadro == "C202" & predomina == "X"	// Acceso de agua: pilón de uso público de agua potable.
	gen		aa_cc = tipo == "CAMIÓN CISTERNA U OTRO SIMILAR" 			if cuadro == "C202" & predomina == "X"	// Acceso de agua: camión cisterna u otro similar.
	gen		aa_po = tipo == "POZO" 										if cuadro == "C202"	& predomina == "X"	// Acceso de agua: pozo.
	gen		aa_ra = tipo == "RÍO, ACEQUIA, MANANTIAL U OTRO"			if cuadro == "C202" & predomina == "X"	// Acceso de agua: río, acequia, manantial o similar.
	gen		aa_ot = tipo == "OTRO"   									if cuadro == "C202"	& predomina == "X"	// Acceso de agua: otra fuente.
	gen		aa_no = tipo == "NO TIENE" | tipo == "" | ///
			tipo == "SISTEMA DE CAPTACIÓN DE AGUA DE LLUVIA" 			if cuadro == "C202"	& predomina == "X"	// Acceso de agua: no tiene. VERIFICAR NUEVA CATEGORÍA

	gen 	aa_pred = (terreno_1 == "X") + (terreno_2 == "X") + (terreno_3 == "X") + (terreno_4 == "X")	if cuadro == "C202" & predomina == "X"
	
	gen		ad_rp = tipo == "RED PÚBLICA"								if cuadro == "C212" & predomina == "X"	// Acceso de desagüe: red pública.
	gen		ad_pp = tipo == "POZO PERCOLADOR" 							if cuadro == "C212"	& predomina == "X"	// Acceso de desagüe: pozo percolador.
	gen		ad_pct = tipo == "TANQUE SÉPTICO"							if cuadro == "C212"	& predomina == "X"	// Acceso de desagüe: pozo con tratamiento / tanque séptico.
	gen		ad_pst = tipo == "POZO SIN TRATAMIENTO"						if cuadro == "C212"	& predomina == "X"	// Acceso de desagüe: pozo sin tratamiento.
	gen		ad_ra = tipo == "RÍO, ACEQUIA O CANAL" 						if cuadro == "C212"	& predomina == "X"	// Acceso de desagüe: río, acequia o canal.
	gen		ad_zf = tipo == "ZANJA FILTRANTE" 							if cuadro == "C212"	& predomina == "X"	// Acceso de desagüe: zanja filtrante.
	gen		ad_bd = tipo == "BIODIGESTOR" 								if cuadro == "C212"	& predomina == "X"	// Acceso de desagüe: biodigestor.
	gen		ad_ot = tipo == "OTRO" 										if cuadro == "C212"	& predomina == "X"	// Acceso de desagüe: otro.
	gen		ad_no = tipo == "NO TIENE" | tipo == ""	| ///
			tipo == "UNIDADES BÁSICAS DE SANEAMIENTO (UBS)" 			if cuadro == "C212"	& predomina == "X"	// Acceso de desagüe: no tiene.	VERIFICAR NUEVA CATEGORÍA
	
	gen 	ad_pred = (terreno_1 == "X") + (terreno_2 == "X") + (terreno_3 == "X") + (terreno_4 == "X")	if cuadro == "C206" & predomina == "X"
	
	* Análisis de consistencia de datos: revisión
	*---------------------------------------------
	gen		ac1_p202 = tipo != "RED PÚBLICA (AGUA POTABLE)" & tipo != "PILÓN DE USO PÚBLICO (AGUA POTABLE)" & tipo != "CAMIÓN CISTERNA U OTRO SIMILAR" & tipo != "POZO" & ///
			tipo != "RÍO, ACEQUIA, MANANTIAL U OTRO" & tipo != "OTRO" & tipo != "NO TIENE" & tipo != "" & tipo != "SISTEMA DE CAPTACIÓN DE AGUA DE LLUVIA"  						if cuadro == "C202"
	gen		ac1_p212 = tipo != "RED PÚBLICA" & tipo != "POZO PERCOLADOR" & tipo != "TANQUE SÉPTICO" & tipo != "POZO SIN TRATAMIENTO" & tipo != "RÍO, ACEQUIA O CANAL" & ///
			tipo != "ZANJA FILTRANTE" & tipo != "BIODIGESTOR" & tipo != "OTRO" & tipo != "NO TIENE" & tipo == "" & tipo == "UNIDADES BÁSICAS DE SANEAMIENTO (UBS)"					if cuadro == "C212"			
	gen		ac1_p216 = tipo != "RED PÚBLICA (DE UNA EMPRESA DISTRIBUIDORA DE ENER" & tipo != "PANELES SOLARES (ENERGÍA FOTOVOLTAICA)" & tipo != "ENERGÍA EÓLICA" & ///
			tipo != "GRUPO ELECTRÓGENO" & tipo != "NO TIENE" & tipo != "OTRO" 																										if cuadro == "C216"			
			
	collapse (sum) aa_* ad_* ac1_* (firstnm) codlocal clima, by(cod_local)
	
	merge	1:1 codlocal using "$Raw\fuie24\local_lineal_02", keepusing(p201_1 p201_2 p201_3 p207 p208)
	drop 	if _merge == 2
	drop 	_merge
	
	gen		aa_loc = p201_2 == "1"										// Centro poblado tiene servicio de agua.
	gen		ad_loc = p201_3 == "1"										// Centro poblado tiene servicio de desagüe.
	
	* Cálculo de costos de intervención
	*-----------------------------------	
	gen		int_aa_no = aa_rp == 1 | aa_cc == 1 | aa_po == 1													// Sin intervención.
	gen		int_aa_cnrp = (aa_rp != 1 & aa_cc != 1 & aa_po != 1) & aa_loc == 1									// Conexión a red pública + medidor.
	gen		int_aa_alpt = (aa_rp != 1 & aa_cc != 1 & aa_po != 1) & aa_loc != 1 & (clima >= 7 & clima <= 9)		// Agua de lluvia y planta de tratamiento.
	gen		int_aa_pasc = (aa_rp != 1 & aa_cc != 1 & aa_po != 1) & aa_loc != 1 & (clima >= 1 & clima <= 6) 		// Pozo de agua y sistema de cloración.
	
	gen		int_ad_no = ad_rp == 1 | ad_pp == 1	| ad_bd == 1													// Sin intervención.
	gen		int_ad_cnrp = (ad_rp != 1 & ad_pp != 1 & ad_bd != 1) & ad_loc == 1									// Conexión a red pública.
	gen		int_ad_zinu = (ad_rp != 1 & ad_pp != 1 & ad_bd != 1) & ad_loc != 1 & clima == 9						// En zona inundable.
	gen		int_ad_sinsitu = (ad_rp != 1 & ad_pp != 1 & ad_bd != 1) & ad_loc != 1 & (clima >= 1 & clima <= 8)	// Sistema in-situ.
	
	replace aa_pred = 1 if aa_pred == 0
	replace ad_pred = 1 if ad_pred == 0
	
	* Análisis de consistencia de datos: revisión
	*---------------------------------------------
	gen		ac1_p201_1 = p201_1 != "1" & p201_1 != "2"
	gen		ac1_p201_2 = p201_2 != "1" & p201_2 != "2"
	gen		ac1_p201_3 = p201_3 != "1" & p201_3 != "2"
	gen		ac1_p207 = aa_rp == 1 & (p207 != "1" & p207 != "2")													// Revisar criterio para cálculo 2024.
	gen		ac1_p208 = aa_rp == 1 & (p208 != "1" & p208 != "2" & p208 != "3")									// Revisar criterio para cálculo 2024.
	
	compress
	save	"$Input\fuie24\LE_Aad_FUIE24.dta", replace
	
	/*_____________________________________________________________________
	|                                                                      |
	|       	SISTEMA DE ALMACENAMIENTO DE IMPULSIÓN DE AGUA		       |
	|_____________________________________________________________________*/
	
	* Procesamiento de variables de tanques elevados, cisternas	y bombas		
	*-------------------------------------------------------------------
	use 	"$Raw\fuie24\local_sec303", clear
	destring codlocal, gen(cod_local)
	
	merge 	m:1 cod_local using "$Input\fuie24\LE_Infra_FUIE24", keepusing(zona)
	keep 	if _merge == 3
	drop 	_merge																// Mantener elementos en locales que tienen información del estado de la infraestructura y de tanques elevados/cisternas.
	
	keep 	if numero == "1" | numero == "2" | numero == "3"
	
	egen	cis = rowtotal(p303_4_1 p303_4_2) if numero == "2" & p303_2 == "1"							// Local tiene cisterna en uso, en buen o regular estado.
	egen	bom = rowtotal(p303_4_1 p303_4_2) if numero == "3" & p303_2 == "1"							// Local tiene bomba sumergible en uso, en buen o regular estado.
	gen		te_si = p303_4_1 if numero == "1" & p303_2 == "1"											// Tanque elevado sin intervención.
	gen		te_mb = p303_4_2 if numero == "1" & p303_2 == "1"											// Tanque elevado requiere mantenimiento bajo.
	*gen		te_mm = p303_4_2 if numero == "1" & p303_2 == "1"										// Tanque elevado requiere mantenimiento moderado.											
	gen		te_sus = p303_4_3 if numero == "1" & p303_2 == "1"											// Tanque elevado requiere sustitución.				// REVISAR POR QUÉ QUITARON CATEGORÍA DE FUIE?
	
	collapse	(sum) cis bom te_* (firstnm) codlocal zona, by(cod_local)
	
	merge	1:1	cod_local using "$Input\fuie24\LE_Aad_FUIE24.dta", keepusing(aa_* p207 p208)
	drop	if _merge == 2
	drop	_merge
	gen 	agualv = p207 == "1" & (p208 == "1" | p208 == "3") 											// Variable de servicio de agua potable de lunes a viernes durante horario de clases. REVISAR.
		
	* Implementación de cisterna / bomba / tanque elevado
	*-------------------------------------------------------
	foreach v of varlist cis bom te_* {
		gen _`v' = `v'
		replace _`v' = 0 if `v' == .
	}
	
	gen		cis_imp = max(aa_pred - _cis , 0) * ((aa_rp == 1 | aa_cc == 1 | aa_po == 1) & agualv == 0) + aa_pred * (aa_rp != 1 & aa_cc != 1 & aa_po != 1)
	gen 	bom_imp = max(aa_pred - _bom , 0) * ((aa_rp == 1 | aa_cc == 1 | aa_po == 1) & agualv == 0) + aa_pred * (aa_rp != 1 & aa_cc != 1 & aa_po != 1)
	gen		te_imp = max(aa_pred - _te_si - _te_mb - _te_sus , 0) * ((aa_rp == 1 | aa_cc == 1 | aa_po == 1) & agualv == 0) + aa_pred * (aa_rp != 1 & aa_cc != 1 & aa_po != 1) 	// & (-_te_mm == 0)
	
	drop 	_*
			
	* Cálculo de metros cúbicos de tanques y cisternas según alumnos
	*----------------------------------------------------------------
	merge	1:1 cod_local using "$Input\fuie24\LE_Alum_FUIE24", keepusing(alumtot alum1tot alum2tot alum3tot alum4tot alum5tot alum6tot alum7tot alum8tot alum9tot)
	drop	if _merge == 2
	drop	_merge
	
	gen		aguar = alumtot * 50 if zona == 2
	replace	aguar = (alum1tot + alum2tot) * 15 + (alum3tot + alum4tot + alum5tot + alum6tot + alum7tot + alum8tot + alum9tot) * 20 if zona == 1
	
	gen		cisvol = aguar * (3 / 4) / 1000
	gen		tevol = aguar * (1 / 3) / 1000
	
	compress
	save	"$Input\fuie24\LE_Alma_FUIE24.dta", replace

	/*_____________________________________________________________________
	|                                                                      |
	|            	CÁLCULO DE COSTO DE ACCESO DE ENERGÍA              	   |
	|_____________________________________________________________________*/	
	
	use 	"$Raw2\fuie24\241130\local_ssbb", clear
	destring codlocal, gen(cod_local)
	
	merge 	m:1 cod_local using "$Input\fuie24\LE_Infra_FUIE24", keepusing(zona areatech int_st)
	keep 	if _merge == 3
	drop 	_merge																// Mantener locales que tienen información del estado de la infraestructura y de acceso a agua y desagüe.
		
	* Elaboración de indicadores
	*----------------------------
	gen		ae_rp = tipo == "RED PÚBLICA (DE UNA EMPRESA DISTRIBUIDORA DE ENER" 		if cuadro == "C216" & predomina == "X"			// Acceso de energía: red pública
	gen		ae_gm = tipo == "GRUPO ELECTRÓGENO" 										if cuadro == "C216" & predomina == "X" 			// Acceso de energía: generador/motor.
	gen		ae_ps = tipo == "PANELES SOLARES (ENERGÍA FOTOVOLTAICA)"					if cuadro == "C216"	& predomina == "X" 			// Acceso de energía: panel solar.
	gen		ae_ee = tipo == "ENERGÍA EÓLICA" 											if cuadro == "C216"	& predomina == "X" 			// Acceso de energía: energía eólica.
	gen		ae_ot = tipo == "OTRO" 			 											if cuadro == "C216"	& predomina == "X"	 		// Acceso de energía: otra fuente.
	gen		ae_no = tipo == "NO TIENE" | tipo == ""			 							if cuadro == "C216"	& predomina == "X"	 		// Acceso de energía: no tiene.
	gen		ae_le = ae_rp == 1 | ae_gm == 1 | ae_ps == 1| ae_ee == 1 | ae_ot == 1		if cuadro == "C216"	& predomina == "X"			// Local educativo tiene acceso de energía.
	
	collapse (sum) ae_* (firstnm) codlocal zona areatech int_st, by(cod_local)
	
	merge	1:1 codlocal using "$Raw\fuie24\local_lineal_02", keepusing(p201_1)
	drop 	if _merge == 2
	drop 	_merge
	
	gen		ae_loc = p201_1 == "1"												// Centro poblado tiene servicio de energía.
	
	* Cálculo de costos de intervención
	*-----------------------------------		
	gen		int_ae_no = ((ae_loc == 1 & ae_le == 1) | (ae_loc == 0 & ae_le == 1)) & (ae_rp == 1 & ae_gm == 0 & ae_ps == 0 & ae_ee == 0 & ae_ot == 0 & ae_no == 0)		// Sin intervención.
	gen		int_ae_adec = ((ae_loc == 1 & ae_le == 1) & (ae_gm == 1 | ae_ps == 1 | ae_ee == 1 | ae_ot == 1)) | 	///
						  ((ae_loc == 0 & ae_le == 1) & (ae_rp == 1 & (ae_gm == 1 | ae_ee == 1 | ae_ps == 1 | ae_ot == 1))) 											// Adecuar conexión de energía.
	gen		int_ae_prloc = (ae_loc == 0 & ae_le == 0) | ((ae_loc == 0 & ae_le == 1) & (ae_rp == 0 & (ae_gm == 1 | ae_ps == 1 | ae_ee == 1 | ae_ot == 1)))				// Proveer conexión a localidad.
	gen		int_ae_prle = ae_loc == 1 & ae_le == 0																														// Proveer conexión a local educativo.
	replace	int_ae_adec = 1 if ae_le == 1 & (ae_rp == 0 & ae_gm == 0 & ae_ps == 0 & ae_ee == 0 & ae_ot == 0)															// Adecuar conexión de energía (caso de inconsistencia).
	
	merge 	1:1 cod_local using "$Input\fuie24\LE_Amp_FUIE24", keepusing(areatech1 areatech2 areatech3 areatech4 areatech5 areatech6 areatech7 areatech8 areatech9)
	drop	if _merge == 2
	drop	_merge
	
	egen	areatechmax = rowmax(areatech1 areatech2 areatech3 areatech4 areatech5 areatech6 areatech7 areatech8 areatech9)
	replace areatechmax = areatech if areatechmax == .							// Los locales sin información de áreas techadas por nivel se incorporan con el área techada total.
	
	gen		ct_ae = 0 if int_ae_no == 1
	replace	ct_ae = 748 if zona == 2 & areatechmax > 0 & areatechmax <= 979 & (int_ae_adec == 1 | int_ae_prloc == 1 | int_ae_prle == 1)
	replace	ct_ae = 1819 if zona == 2 & areatechmax > 979 & areatechmax <= 2036 & (int_ae_adec == 1 | int_ae_prloc == 1 | int_ae_prle == 1)
	replace	ct_ae = 2332 if zona == 2 & areatechmax > 2036 & areatechmax <= 5061 & (int_ae_adec == 1 | int_ae_prloc == 1 | int_ae_prle == 1)
	replace	ct_ae = 2910 if zona == 2 & areatechmax > 5061 & (int_ae_adec == 1 | int_ae_prloc == 1 | int_ae_prle == 1)

	* Solo si área máxima es Secundaria.
	replace	ct_ae = 1819 if zona == 2 & areatechmax > 0 & areatechmax <= 1282 & (int_ae_adec == 1 | int_ae_prloc == 1 | int_ae_prle == 1) & areatech3 > 0 & areatech3 == areatechmax
	replace	ct_ae = 2332 if zona == 2 & areatechmax > 1282 & areatechmax <= 2613 & (int_ae_adec == 1 | int_ae_prloc == 1 | int_ae_prle == 1) & areatech3 > 0 & areatech3 == areatechmax
	replace	ct_ae = 2910 if zona == 2 & areatechmax > 2613 & (int_ae_adec == 1 | int_ae_prloc == 1 | int_ae_prle == 1) & areatech3 > 0 & areatech3 == areatechmax
	
	replace	ct_ae = 24900 if zona == 1 & areatechmax > 0 & areatechmax <= 543 & (int_ae_adec == 1 | int_ae_prloc == 1 | int_ae_prle == 1)
	replace	ct_ae = 49800 if zona == 1 & areatechmax > 543 & (int_ae_adec == 1 | int_ae_prloc == 1 | int_ae_prle == 1)
	
	gen	 	ct_st_ae = ct_ae if int_st == 1
	replace ct_st_ae = 0 if int_st != 1
	replace	ct_ae = 0 if int_st == 1 											// Locales que requieren sustitución total se registran por separado.
	
	compress
	save	"$Input\fuie24\LE_Ae_FUIE24.dta", replace	
	
	/*_____________________________________________________________________
	|                                                                      |
	|            	TENENCIA DEL PREDIO DEL LOCAL EDUCATIVO	               |
	|_____________________________________________________________________*/
	
	* Obtener predios y tenencia del predio
	*--------------------------------------
	use 	"$Raw2\fuie24\241130\local_sec108.dta", clear
	destring codlocal, gen(cod_local)
	
	merge 	m:1 cod_local using "$Input\fuie24\LE_Infra_FUIE24", keepusing(cod_local)
	keep 	if _merge == 3
	drop 	_merge						// Mantener locales que tienen información del estado de la infraestructura.
	
	* Análisis de consistencia de datos: revisión
	*---------------------------------------------
	gen		ac1_p108_1 = p108_1 != "01" & p108_1 != "02" & p108_1 != "03" & p108_1 != "04" & p108_1 != "05"
	
	replace p108_1 = "05" if p108_1 != "01" & p108_1 != "02" & p108_1 != "03" & ///
			p108_1 != "04" & p108_1 != "05"
	
	forvalues i = 1/5 {
		gen pred_ten`i' = p108_1 == "0`i'"
	}
	
	collapse (sum) pred_ten* ac1_*, by(cod_local)
	
	compress
	save	"$Input\fuie24\LE_TenPred_FUIE24.dta", replace
	
	/*_____________________________________________________________________
	|                                                                      |
	|            			CÁLCULO DE BRECHA 	                    	   |
	|_____________________________________________________________________*/
	
	use 	"$Input\fuie24\LE_Infra_FUIE24", clear
	
	merge	1:1 cod_local using "$Input\fuie24\LE_Cerco_FUIE24", keepusing(cp cp_* tramo)
	drop 	if _merge == 2
	drop	_merge
	
	merge	1:1 cod_local using "$Input\fuie24\LE_Amp_FUIE24", keepusing(areaamp)
	drop 	if _merge == 2
	drop	_merge
	replace	areaamp = 0 if areaamp == .
	
	merge	1:1 cod_local using "$Input\fuie24\LE_Acc_FUIE24", keepusing(acc_ir acc_rr1 acc_rr2_5 acc_ar)
	drop 	if _merge == 2
	drop	_merge
	
	merge	1:1 cod_local using "$Input\fuie24\LE_Ene_FUIE24", keepusing(area_ene1 area_ene2 area_ene3 area_ene4)		
	drop 	if _merge == 2
	drop	_merge
	
	merge	1:1 cod_local using "$Input\fuie24\LE_SSHH_FUIE24", keepusing(ba_* b_* int_sshh)
	drop 	if _merge == 2
	drop	_merge
	
	merge	1:1 cod_local using "$Input\fuie24\LE_Aad_FUIE24", keepusing(int_* ac1_* aa_pred ad_pred)
	drop 	if _merge == 2
	drop	_merge
	
	merge	1:1 cod_local using "$Input\fuie24\LE_Alma_FUIE24", keepusing(cis* te* bom*)
	drop 	if _merge == 2
	drop	_merge
	
	merge	1:1 cod_local using "$Input\fuie24\LE_Ae_FUIE24", keepusing(ct_ae ct_st_ae)
	drop 	if _merge == 2
	drop	_merge
	
	merge	1:1 cod_local using "$Input\fuie24\LE_TenPred_FUIE24", keepusing(pred_ten* ac1_*)
	drop 	if _merge == 2
	drop	_merge
	
	merge 	1:1 cod_local using "$Input\LE_BaseAdic", keepusing(disafil b_safil)
	drop 	if _merge == 2
	drop 	_merge
	
	* Obtener costos unitarios, aplicar actualización por inflación y definir factores
	*---------------------------------------------------------------------------------
	merge	m:1 escenario clima pendiente using "$Input\Cunit_Atech"									// Usar Cunit_Atech2018 para costos 2018.
	drop 	if _merge == 2
	drop	_merge
	
	merge	m:1 zona escenario using "$Input\Cunit_Cp"													// Usar Cunit_Cp2018 para costos 2018.
	drop 	if _merge == 2
	drop	_merge			// Se ha proyectado el costo unitario de cerco perimétrico para escenarios 4 y 5
							// según los costos unitarios promedio de área techada (4.28% y 29.09% respectivamente sobre costo promedio de escenario 3).
	
	merge	m:1 escenario topografia using "$Input\Cunit_Acc"
	drop 	if _merge == 2
	drop	_merge			// Se ha proyectado el costo unitario de ascensores para escenarios 4 y 5
							// según los costos unitarios promedio de área techada (4.28% y 29.09% respectivamente sobre costo promedio de escenario 3).

	merge	m:1 escenario clima using "$Input\Cunit_AgSn"
	drop 	if _merge == 2
	drop	_merge
	gen		cu_ad_zinu = 20000
	
	merge 	m:1 clima using "$Input\Cprop_Ce"
	drop 	if _merge == 2
	drop	_merge
	
	merge 	m:1 escenario clima using "$Input\Cprop_Ene"
	drop 	if _merge == 2
	drop	_merge	
	
	foreach v of varlist cu_aa_* cu_ad_* cu_cis_* cu_te_* cu_sshh_* cu_beb* cu_ca cu_bp cu_ino cu_ram* cu_asc ct_ae ct_st_ae b_safil {
			replace `v' = `v' * 1.367811	// Actualización de costos por inflación 2015-2018: 6.9382% (2016: 3.2349%, 2017: 1.3649%, 2018: 2.1925%). Usar 1.069382 para costos 2018. 
	}										// Actualización de costos por inflación 2015-2019: 8.9702% (2019: 1.9001%).
											// Actualización de costos por inflación 2015-2020: 11.1204% (2020: 1.9732%).
											// Actualización de costos por inflación 2015-2021: 18.2659% (2021: 6.4304%).
											// Actualización de costos por inflación 2015-2022: 28.9393% (2022: 8.4592%).
											// Actualización de costos por inflación 2015-2023: 32.4228% (2023: 3.2374%).
											// Actualización de costos por inflación 2015-2024: 35.0266% (2024: 1.9663%).
											// Actualización de costos por inflación 2015-06/2025: 36.7811% (01-06/2025: 1.2994%).
											// No aplica: cu_at cu_atyoe cu_cp - Info de PRONIED está actualizada a 10/2024.
											
	foreach v of varlist cu_at cu_atyoe cu_cp {										
			replace `v' = `v' * 1.015019	// Actualización de costos por inflación 10/2024-06/2025: 1.5019%.
	}																									
		
	scalar 	FSC = 1.05		// Factor que incluye supervisión (4%) y contingencia (1%).
	scalar	FSCX = 1.09		// Factor que incluye supervisión (4%), contingencia (1%) y expediente técnico (4%). 
	scalar 	FGGU = 1.20		// Factor que incluye los gastos generales y utilidades (20%). PNIE = 18%, se actualizó con información de PRONIED.
	scalar 	FIGV = 1.18		// Factor que considera el IGV (18%).
	scalar 	FSEAC = 0.75	// Factor que considera sistema estructural de albañilería confinada y sistema de techo económico.																														
	
	* Obtener otros costos proporcionales para distintos componentes de brecha
	*--------------------------------------------------------------------------	
	gen		cpr_me = 0.05 if zona == 1
	replace	cpr_me = 0.16 if zona == 2		// Mobiliario y Equipamiento.
		
	* Cálculo del Grupo 1: Vulnerabilidad de la Infraestructura
	*-----------------------------------------------------------
	gen		ct_st_d = 	(areatech * cu_at * 0.35) * FSCX * FGGU * FIGV if int_st == 1
	gen		ct_sp_d = 	(areasust * cu_at * 0.35) * FSCX * FGGU * FIGV if int_sp == 1
	gen		ct_ri = 	(areari * cu_at * 0.3) * FSC * FGGU * FIGV if int_ri == 1
	gen		ct_ic = 	((areaic1 + areaic2) * cu_at * 0.15) * FSC * FGGU * FIGV if int_ic == 1
	gen		ct_cp = 	(cp_n + cp_dr * 1.33 + cp_mm * 0.66 + cp_mb * 0.33) * cu_cp * FSCX * FGGU* FIGV if int_st != 1	
	
	egen	b_1 = 		rowtotal(ct_st_d ct_sp_d ct_ri ct_ic ct_cp), missing
	
	* Cálculo del Grupo 2: Servicios Básicos de Agua y Saneamiento
	*--------------------------------------------------------------
	gen		f_sp =			areasust / areatech if int_sp == 1					// Factor con proporción de área techada que requiere sustitución en intervención de sustitución parcial.
	replace	f_sp = 			1					if int_st == 1
	replace	f_sp = 			0					if f_sp == .
	
	gen		ct_aa_cnrp = 	int_aa_cnrp * cu_aa_cnrp_m * aa_pred * FSCX * FGGU * FIGV
	gen		ct_aa_alpt = 	int_aa_alpt * cu_aa_alpt * aa_pred *  FSCX * FGGU * FIGV
	gen		ct_aa_pasc = 	int_aa_pasc * cu_aa_pasc * aa_pred *  FSCX * FGGU * FIGV
	gen		ct_ad_cnrp = 	int_ad_cnrp * cu_ad_cnrp_m * ad_pred * FSCX * FGGU * FIGV
	gen		ct_ad_zinu = 	cu_ad_zinu if int_ad_zinu > 0 & int_ad_zinu != .
	gen		ct_ad_sinsitu = int_ad_sinsitu * (cu_ad_sinsitu_ts + cu_ad_sinsitu_pp + cu_ad_sinsitu_bc) * ad_pred * FSCX * FGGU * FIGV
	egen	ct_aad = 		rowtotal(ct_aa_cnrp ct_aa_alpt ct_aa_pasc ct_ad_cnrp ct_ad_zinu ct_ad_sinsitu), missing
	gen	 	ct_st_aad = 	ct_aad if int_st == 1
	replace ct_st_aad = 	0 if int_st != 1
	replace	ct_aad = 		0 if int_st == 1 									// Locales que requieren sustitución total se registran por separado.
	
	gen		ct_cis_imp = 	(cisvol * cu_cis_m3) * FSCX * FGGU * FIGV if cis_imp > 0 & cis_imp != . & cisvol > 0 & cisvol != . & int_st != 1
	gen		ct_bom_imp = 	(cu_cis_cb + cu_cis_eb) * FSCX * FGGU * FIGV if bom_imp > 0 & bom_imp != . & int_st != 1
	gen		ct_te_sus = 	tevol * cu_te_m3 * 1.20 * FSCX * FGGU * FIGV if te_sus > 0 & te_sus != . & int_st != 1				// ¿CONSIDERAR CADA TANQUE O SOLO M3 NECESARIOS?
	gen		ct_te_imp = 	tevol * cu_te_m3 * FSCX * FGGU * FIGV if te_imp > 0 & te_imp != . & (ct_te_sus == . | ct_te_sus == 0) & int_st != 1 
	gen		ct_te_mb = 		tevol * cu_te_m3 * 0.20 * FSCX * FGGU * FIGV if te_mb > 0 & te_mb != . & (ct_te_sus == . | ct_te_sus == 0) & (ct_te_imp == . | ct_te_imp == 0) & int_st != 1
	*gen		ct_te_mm = 		tevol * cu_te_m3 * 0.30 * FSCX * FGGU * FIGV if te_mm > 0 & te_mm != . & (ct_te_sus == . | ct_te_sus == 0) & (ct_te_imp == . | ct_te_imp == 0) & int_st != 1
	egen	ct_cis_te = 	rowtotal(ct_cis_imp ct_bom_imp ct_te_imp ct_te_mb ct_te_sus), missing		// ct_te_mm							
	
	gen		ct_inoamp = 	ba_inoamp * (cu_sshh_ino + cu_sshh_cub + cu_sshh_lav + cu_sshh_sd + cu_sshh_sa) * (1 - f_sp) * FSCX * FGGU * FIGV if int_st != 1
	gen		ct_inoreh = 	ba_inoreh * (cu_sshh_ino + cu_sshh_cub + cu_sshh_lav + cu_sshh_sd + cu_sshh_sa) * 0.30 * (1 - f_sp) * FSCX * FGGU * FIGV if int_st != 1
	gen		ct_uriamp =		ba_uriamp * cu_sshh_uri * (1 - f_sp) * FSCX * FGGU * FIGV if int_st != 1
	gen		ct_urireh =		ba_urireh * cu_sshh_uri * 0.30 * (1 - f_sp) * FSCX * FGGU * FIGV if int_st != 1
	gen		ct_beb =		b_beb * (cu_beb + cu_beb_osm) * FSCX * FGGU * FIGV if int_st != 1
	egen	ct_sshh_beb =	rowtotal(ct_inoamp ct_inoreh ct_uriamp ct_urireh ct_beb), missing
	
	gen		ct_ca_reh = 	ca_reh * cu_ca * 0.30 * (1 - f_sp) * FSCX * FGGU * FIGV if int_st != 1
	gen		ct_ca_sus =		ca_sus * cu_ca * 1.10 * (1 - f_sp) * FSCX * FGGU * FIGV if int_st != 1
	gen		ct_ca_imp =		ca_imp * cu_ca * (1 - f_sp) * FSCX * FGGU * FIGV if int_st != 1
	gen		ct_bp_reh =		bp_reh * cu_bp * 0.30 * (1 - f_sp) * FSCX * FGGU * FIGV if int_st != 1
	gen		ct_bp_sus = 	bp_sus * cu_bp * 1.10 * (1 - f_sp) * FSCX * FGGU * FIGV if int_st != 1
	gen		ct_bp_imp =		bp_imp * cu_bp * (1 - f_sp) * FSCX * FGGU * FIGV if int_st != 1
	egen	ct_ca_bp =		rowtotal(ct_ca_reh ct_ca_sus ct_ca_imp ct_bp_reh ct_bp_sus ct_bp_imp), missing
	
	egen	ct_cad =		rowtotal(ct_cis_te ct_sshh_beb ct_ca_bp), missing
	egen	b_2 =			rowtotal(ct_aad ct_cad), missing
	
	* Cálculo del Grupo 4: Mejoramiento y Ampliación de Locales
	*-----------------------------------------------------------
	gen		ct_sp_r = 	(areasust * cu_at * FSEAC) * FSCX * FGGU * FIGV if int_sp == 1
	gen		ct_rc = 	(arearc * cu_at * 0.5) * FSCX * FGGU * FIGV if int_rc == 1
	gen		ct_ic_r = 	(areaic1 * cu_at * 1.35) * FSC * FGGU * FIGV if int_ic == 1
	gen		ct_amp = 	(areaamp * cu_atyoe) * FSCX * FGGU * FIGV if int_st != 1		// ¿Por qué no considerar FSEAC?
	
	gen		ct_acc1 = 	(acc_ir * cu_ino) * FSCX * FGGU * FIGV if int_st != 1
	gen		ct_acc2 =	(acc_rr1 * cu_ram1) * FSCX * FGGU * FIGV if int_st != 1
	gen		ct_acc3 = 	(acc_rr2_5 * cu_ram2_5) * FSCX * FGGU * FIGV if int_st != 1
	gen 	ct_acc4 = 	(acc_ar * cu_asc) * FSCX * FGGU * FIGV if int_st != 1
	egen	ct_acc = 	rowtotal(ct_acc1 ct_acc2 ct_acc3 ct_acc4), missing
	
	egen	b_4 = 		rowtotal(ct_sp_r ct_rc ct_ic_r ct_amp ct_ae ct_acc), missing
	
	* Cálculo del Grupo 5: Nueva Infraestructura
	*--------------------------------------------
	scalar 	FCR = 0.94 															// Factor que reduce el costo para no considerar cercos perimétricos en la zona rural.
	
	gen		ct_st_ra = 	((areatech + areaamp) * cu_atyoe * FSEAC) * FSCX * FGGU * FIGV if int_st == 1 & zona == 2
	replace	ct_st_ra = 	((areatech + areaamp) * cu_atyoe * FSEAC * FCR) * FSCX * FGGU * FIGV if int_st == 1 & zona == 1
	
	egen	b_5 = 		rowtotal(ct_st_ra ct_st_ae ct_st_aad), missing
	
	* Cálculo del Grupo 3: Mantenimiento de Infraestructura Educativa
	*-----------------------------------------------------------------
	gen		ct_st_me = 	ct_st_ra * cpr_me if int_st == 1
	gen		ct_sp_me = 	ct_sp_r * cpr_me if int_sp == 1
	gen		ct_ri_me = 	ct_ri * cpr_me if int_ri == 1
	gen		ct_rc_me = 	ct_rc * cpr_me if int_rc == 1
	gen		ct_ic_me = 	ct_ic * cpr_me if int_ic == 1
	gen		ct_amp_me =	ct_amp * cpr_me if int_st != 1
	
	gen		ct_ce1 = 	(area_ce1 * cu_at * cpr_ce1) * FSCX * FGGU * FIGV
	gen		ct_ce2 = 	(area_ce2 * cu_at * cpr_ce1 * cpr_ce2) * FSCX * FGGU * FIGV				
	gen		ct_ce3 = 	(area_ce3 * cu_at * cpr_ce1 * cpr_ce3) * FSCX * FGGU * FIGV
	gen		ct_ce4 = 	(area_ce4 * cu_at * cpr_ce1 * cpr_ce4_5) * FSCX * FGGU * FIGV
	gen		ct_ce5 = 	(area_ce5 * cu_at * cpr_ce1 * cpr_ce4_5) * FSCX * FGGU * FIGV
	egen	ct_ce = 	rowtotal(ct_ce1 ct_ce2 ct_ce3 ct_ce4 ct_ce5), missing								
	
	gen		ct_ene1 = 	(area_ene1 * cu_at * cpr_ene1) * FSCX * FGGU * FIGV 
	gen		ct_ene2 = 	(area_ene2 * cu_at * cpr_ene2) * FSCX * FGGU * FIGV 
	gen		ct_ene3 = 	(area_ene3 * cu_at * cpr_ene3) * FSCX * FGGU * FIGV 
	gen		ct_ene4 = 	(area_ene4 * cu_at * cpr_ene4) * FSCX * FGGU * FIGV 					
	egen	ct_ene =	rowtotal(ct_ene1 ct_ene2 ct_ene3 ct_ene4), missing									
	
	egen	b_3 = 		rowtotal(ct_st_me ct_sp_me ct_ri_me ct_rc_me ct_ic_me ct_amp_me ct_ce ct_ene), missing
	
	egen	brecha = 	rowtotal(b_1 b_2 b_3 b_4 b_5 b_safil), missing
	order	cod_local b_1 b_2 b_3 b_4 b_5 b_safil brecha areadem ratiodem areariesgo ratioriesgo int_st int_sp int_ri int_rc int_ic 
	sort	cod_local
	
	compress
	save	"$Input\fuie24\LE_Brecha_FUIE24.dta", replace	

	/*_____________________________________________________________________
	|                                                                      |
	|            		CONSOLIDACIÓN DE BASES 	                    	   |
	|_____________________________________________________________________*/
	
	use 	"$Input\fuie24\LE_Brecha_FUIE24", clear
	
	merge	1:1 cod_local using "$Input\fuie24\LE_Alum_FUIE24", keepusing(alumtot alum1tot alum2tot alum3tot alum4tot alum5tot alum6tot alum7tot alum8tot alum9tot ac1_*)
	drop	if _merge == 2
	drop 	_merge

	gen 	FUIE = 1
	append 	using "$Input\LE_Brecha_CIE-SRI-FUIE23.dta"
		
	gen		finfo2 = finfo + 1 if finfo != 1
	replace finfo2 = 1 if finfo == 1
	replace	finfo2 = 2 if FUIE == 1
	drop 	finfo FUIE
	rename	(finfo2) (finfo)
	
	label 	define finfo 1 "Brecha Cerrada" 2 "FUIE-CE 2024" 3 "FUIE-CE 2023" 4 "FUIE-CE 2022" 5 "FUIE-CE 2021" 6 "SRI 2018-2022" 7 "DRELM 2018" 8 "PRONIED 2018" 9 "DIPLAN 2016"  ///
						 10 "UGEL 2017 - Etapa 2" 11 "UGEL 2017 - Etapa 1" 12 "CIE 2013" 13 "Sin información", replace
	label 	values finfo finfo
	
	duplicates tag cod_local, gen(_aux)
	merge	m:1 cod_local using "$Input\LE_NoFUIE24", keepusing(cod_local) keep (1 3)		// LE_NoFUIE24 contiene códigos de local que no usan la FUIE 2024 como fuente de información.
	preserve
		keep if _aux == 1 & finfo == 2 & _merge == 3
		drop	_merge
		merge	m:1 cod_local using "$Input\LE_NoFUIE24", keepusing(no_f24) keep (1 3)
		drop 	_merge _aux
		save "$Input\LE_Descarte_FUIE24.dta", replace
	restore
	drop 	if _aux == 1 & finfo == 2 & _merge == 3
	drop 	_merge _aux
	
	duplicates tag cod_local, gen(_aux)
	merge	m:1 cod_local using "$Input\LE_SiFUIE24", keepusing(cod_local si_f24) keep (1 3)		// LE_SiFUIE24 contiene códigos de local con información confirmada de FUIE 2024, y FUIE 2022-2023 en los que
	drop 	if _aux == 1 & finfo != 2 & _merge == 3 												// el área techada no ha variado más de 20% (no pasan por análisis de consistencia).
	gen		ac2_f24_ok = finfo == 2 & _merge == 3
	
	replace ac1_p105 = ac1_p105_sif24 if ac2_f24_ok == 1 & si_f24 != "Análisis de consistencia de datos"
	replace ac1_p106 = ac1_p106_sif24 if ac2_f24_ok == 1 & si_f24 != "Análisis de consistencia de datos"
	replace ac1_p106_4 = ac1_p106_4_sif24 if ac2_f24_ok == 1 & si_f24 != "Análisis de consistencia de datos"
	replace ac1_p401_7 = ac1_p401_7_sif24 if ac2_f24_ok == 1 & si_f24 != "Análisis de consistencia de datos"
	replace ac1_areatech_le = ac1_areatech_le_sif24 if ac2_f24_ok == 1 & si_f24 != "Análisis de consistencia de datos"
	drop 	_merge _aux
	
	duplicates tag cod_local, gen(_aux)
	merge	m:1 cod_local using "$Input\LE_SiFUIE23", keepusing(cod_local si_f23) keep (1 3)		// LE_SiFUIE23 contiene códigos de local con información confirmada de la FUIE 2023.
	preserve
		keep if _aux == 1 & finfo == 2 & (si_f23 == "Articulación con regiones 2023-09" | si_f23 == "Articulación con regiones 2023-10" | 	///
				si_f23 == "Articulación con regiones 2023-11" | si_f23 == "Articulación con regiones 2023-12" | si_f23 == "Articulación con regiones 2024-07")  & _merge == 3 
		append using "$Input\LE_Descarte_FUIE24.dta"
		replace no_f24 = "FUIE-CE 2023 validada por Articulación con regiones y Cambio AT > 20%" if _merge == 3
		drop 	_merge _aux si_f23
		save "$Input\LE_Descarte_FUIE24.dta", replace
	restore	
	drop 	if _aux == 1 & finfo != 3 & (si_f23 == "Articulación con regiones 2023-09" | si_f23 == "Articulación con regiones 2023-10" | 	///
				si_f23 == "Articulación con regiones 2023-11" | si_f23 == "Articulación con regiones 2023-12" | 	///
				si_f23 == "Articulación con regiones 2024-07") & _merge == 3						// Los duplicados de FUIE-CE 2024 no se usan en este caso. Como no están en la lista anterior, han variado más de 20% en área techada.
	drop 	_merge _aux
	
	duplicates tag cod_local, gen(_aux)
	merge	m:1 cod_local using "$Input\LE_SiFUIE22", keepusing(cod_local si_f22) keep (1 3)		// LE_SiFUIE22 contiene códigos de local con información confirmada de la FUIE 2022.
	preserve
		keep if _aux == 1 & finfo == 2 & si_f22 == "Articulación con regiones 2022" & _merge == 3 
		append using "$Input\LE_Descarte_FUIE24.dta"
		replace no_f24 = "FUIE-CE 2022 validada por Articulación con regiones y Cambio AT > 20%" if _merge == 3
		drop 	_merge _aux si_f22
		save "$Input\LE_Descarte_FUIE24.dta", replace
	restore	
	drop 	if _aux == 1 & finfo != 4 & si_f22 == "Articulación con regiones 2022" & _merge == 3	// Los duplicados de FUIE-CE 2024 no se usan en este caso. Como no están en la lista anterior, han variado más de 20% en área techada.
	drop 	_merge _aux
	
	* Análisis de consistencia: comparación (duplicados)
	*-----------------------------------------------------------------
	drop	matricula ac2_r1 ac2_r1_ind
	merge 	m:1 cod_local using "$Input\LE_BaseAdic", keepusing(matricula) keep(1 3) nogen
	replace matricula = alumtot if matricula == 0 & alumtot != . & (finfo == 2 | finfo == 3 | finfo == 4 | finfo == 5 | finfo == 6)				// Agregar # alumnos de aulas de la FUIE/SRI si no hay información de matrícula.	
	
	gen		ac2_r1 = areatech / matricula if (matricula != 0 | matricula != .)
	gen 	ac2_r1_ind = ac2_r1 < 1.5 if ac2_r1 != .
	
	duplicates tag cod_local, gen(_aux)
	egen	_aux8 = sum(ac2_r1_sri_ok) if _aux == 1, by(cod_local)
	replace ac2_r1_sri_ok = 1 if _aux8 == 1
	egen	_aux9 = sum(ac2_r1_f21_ok) if _aux == 1, by(cod_local)
	replace ac2_r1_f21_ok = 1 if _aux9 == 1
	egen	_aux10 = sum(ac2_r1_f22_ok) if _aux == 1, by(cod_local)
	replace ac2_r1_f22_ok = 1 if _aux10 == 1
	egen	_aux11 = sum(ac2_r1_f23_ok) if _aux == 1, by(cod_local)
	replace ac2_r1_f23_ok = 1 if _aux11 == 1
	drop 	_aux8 _aux9 _aux10 _aux11
	replace ac2_r1_ind = 0 if ac2_r1 != . & (ac2_r1_sri_ok == 1 | ac2_r1_f21_ok == 1 | ac2_r1_f22_ok == 1 | ac2_r1_f23_ok == 1)
	tab 	finfo ac2_r1_ind
	
	egen	_aux3 = max(finfo) if _aux == 1, by(cod_local)													// Fuente de información no preferida en duplicados.
	egen	_aux4 = sum(ac2_r1_ind) if _aux == 1, by(cod_local)												// Número de veces que criterio no se cumple en duplicados.
	gen 	_aux5 = ac2_r1_ind == 1 & (finfo == 7 | finfo == 8 | finfo == 9 | finfo == 12) if _aux == 1			
	egen	ac2_r1_dupcie2 = sum(_aux5) if _aux == 1, by(cod_local)											// Número de veces que criterio no se cumple en duplicados para fuentes de información 7-9, 12 (CIE 2013 y actualizado).
	tab 	finfo ac2_r1_ind if _aux == 1
	tab 	_aux4 ac2_r1_dupcie2
	
	egen	_aux6 = min(areatech) if _aux == 1, by(cod_local)												// Área techada mínima en duplicado.
	egen	_aux7 = max(areatech) if _aux == 1, by(cod_local)												// Área techada máxima en duplicado.
	
	rename	areatech areatech2
	merge 	m:1 cod_local using "$Input\cie\LE_Brecha_CIE", keepusing(areatech CIE) keep(1 3) nogen
	replace areatech = . if CIE == 0
	gen		_aux6a = min(areatech, areatech2) if _aux == 1
	gen		_aux7a = max(areatech, areatech2) if _aux == 1
	
	gen	 	ac2_r2_f24 = (_aux7 -_aux6)/_aux6 if _aux == 1 & 				///
			(_aux3 == 7 | _aux3 == 8 | _aux3 == 9 | _aux3 == 12)												//  Cálculo de ratio R2 solo si fuente de información 7-9, 12 (CIE 2013 y actualizado).
	replace ac2_r2_f24 = (_aux7a -_aux6a)/_aux6a if _aux == 1 & 			///
			(_aux3 == 3 | _aux3 == 4 | _aux3 == 5 | _aux3 == 6 | _aux3 == 10 | _aux3 == 11) & areatech != .		//  Cálculo adicional de ratio R2 solo si fuente de información 3-6, 10-11 (FUIE, SRI, UGEL 2017) y hay información CIE 2013 (triple dup).
	gen 	ac2_r2_f24_ind = ac2_r2_f24 > 3 & ac2_r2_f24 != . if _aux == 1 & 	///									
			((_aux3 == 7 | _aux3 == 8 | _aux3 == 9 | _aux3 == 12) | 	///	
			((_aux3 == 3 | _aux3 == 4 | _aux3 == 5 | _aux3 == 6 | _aux3 == 10 | _aux3 == 11) & areatech != .))	//	Indicador si variación área techada mín. y máx. es muy grande: > 300% del mín.
	drop	areatech
	rename  areatech2 areatech
																											
	gen 	ac2_r1_dupdrop = 1 if _aux == 1 & ac2_r1_ind == 1 & _aux4 == 1 & (finfo == 2 | finfo == 3 | finfo == 4  | finfo == 5 | finfo == 6 | finfo == 10 | finfo == 11)
	replace ac2_r1_dupdrop = 0 if _aux == 1 & ac2_r1_dupdrop == .
	egen	ac2_r1_f24_drop = max(ac2_r1_dupdrop) if _aux == 1, by(cod_local)
	
	gen 	ac2_r2_dupdrop = 1 if _aux == 1 & ac2_r2_f23_ind == 1 & finfo == 2 & (ac2_r1_f24_drop == 0 | (ac2_r1_f24_drop == 1 & ac2_r1_dupdrop == 1))
	replace ac2_r2_dupdrop = 0 if _aux == 1 & ac2_r2_dupdrop == .
	egen	ac2_r2_f24_drop = max(ac2_r2_dupdrop) if _aux == 1, by(cod_local)
	
	tab 	ac2_r1_dupdrop ac2_r2_dupdrop
	
	gen		ac2_r1_f24_ok = _aux4 == 2 & (_aux3 == 7 | _aux3 == 8 | _aux3 == 9 | _aux3 == 12) if _aux == 1
	drop	ac2_r1_ind
	gen 	ac2_r1_ind = ac2_r1 < 1.5 if ac2_r1 != .
	
	save	"$Input\LE_AC_FUIE24.dta", replace
	
	replace ac2_r1_dupcie = 1 if ac2_r1_dupcie2 == 1
	drop	if ac2_r1_dupdrop == 1
	drop	if ac2_r2_dupdrop == 1 
	drop	ac2_r1_dupdrop ac2_r2_dupdrop ac2_r1_dupcie2 _aux* CIE
	*-----------------------------------------------------------------
	
	duplicates tag cod_local, gen(_aux)
	egen	_aux2 = min(finfo) if _aux == 1, by(cod_local)
	
	forvalues i = 2/11 {
			drop if _aux == 1 & _aux2 == `i' & finfo != `i'			// Redundante, no es necesario el bucle. Se deja por consistencia con código SRI.
	}	
	drop	_aux*
	
	* Preparación final de base para cálculo de orden de prioridad
	*-----------------------------------------------------------------
	merge 	1:1 cod_local using "$Input\LE_BaseAdic", keepusing(disafil m_*) update replace
	keep 	if _merge >= 3
	drop 	_merge							// Mantener únicamente LL.EE. que tengan información de brecha y en BaseAdic = LL.EE. con al menos un servicio educativo (código modular) público activo.
	replace b_safil = 0 if disafil == 1
	
	egen 	_auxmat = rowtotal(m_*)
	
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local niv = `niv' + 1
		replace `v' = alum`niv'tot	if matricula != _auxmat
	}
	drop	_auxmat
	
	table 	finfo, stat(count brecha) stat(sum brecha) stat(mean brecha) nformat(%15.1gc)
	
	sort	cod_local
	compress
	save	"$Input\LE_Brecha.dta", replace
	
	* Para revisar resultados
	*------------------------
	gen 	aux1 = b_1 > 0 & b_1 != .
	gen 	aux2 = b_2 > 0 & b_2 != .
	gen 	aux3 = b_3 > 0 & b_3 != .
	gen 	aux4 = b_4 > 0 & b_4 != .
	gen 	aux5 = b_5 > 0 & b_5 != .
	gen 	aux6 = b_safil > 0 & b_safil  != .
	egen 	auxt = rowtotal (aux*)
	
	use 	"$Input\LE_AC_FUIE24.dta", clear
	keep	if (ac2_r1_dupdrop == 1 | ac2_r2_dupdrop == 1) & finfo == 2
	drop	ac2_r1_dupdrop ac2_r2_dupdrop 

	save	"$Input\LE_AC_FUIE24.dta", replace
