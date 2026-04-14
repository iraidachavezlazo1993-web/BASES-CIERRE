	/*___________________________________________________________
	|	                                                      	|
	|	MINEDU - Intervenciones Brecha Cerrada - Validación		|						
	|	Actualizado: 27/12/2025	                				|
	|___________________________________________________________*/

 	* 0. Prólogo
	* -------------------------------------
	clear 	all
	
	global 	Main	=	"C:\CalcBrPr2512\intcb\" 			// Carpeta Principal
	global 	Raw		=	"C:\CalcBrPr2512\intcb\raw"			// Carpeta Bases Iniciales
	global 	Input	=   "C:\CalcBrPr2512\intcb\input" 		// Carpeta Input 
	global 	Output	=   "C:\CalcBrPr2512\intcb\output" 		// Carpeta Output
	global 	Temp	=   "C:\CalcBrPr2512\intcb\temp" 		// Carpeta Temp
	global  CalcAnt = 	"C:\CalcBrPr2507" 					// Carpeta Cálculo Anterior
	
	cd 		"$main"

	set 	more off 
	set 	varabbrev off
	set 	type double
	set 	seed 339487731
	set 	excelxlsxlargefile on	

 	* 1. Criterios iniciales
	* ------------------------
		
	* 	VALIDAR MÓDULOS ACTIVOS
	use 	"$Input\Mod_Padron_Validar", clear
	cap 	drop 	_aux1
	
	*	[1] Módulos activos con brecha cerrada
	* 	———————————————————
	merge 	1:1 cod_mod anexo using "$Input\M_251203", keepusing(act) keep(1 3)
	drop 	if act == 0 // eliminar si no están activos
	drop	_merge

	*	[2] LE sin vinculación o sin PI encontrado
	* 	———————————————————
	merge 	1:m cod_mod anexo using "$CalcAnt\intcb\output\CUIM_CBr_Final", keep(1 3) gen(_aux1) keepusing(finfo cui fuente fecha_fuente)
	replace cui = 2495217 if cod_mod == 724724		// Vinculación faltante.
	merge	1:1 cod_mod anexo cui using "$Input\CUIMod_Inv_251227", keep(1 3) gen(_aux2)
	replace conclusion = 3 if _aux2 == 1			// Sin vinculación con BI.

	preserve		
		keep 	if conclusion == 3
		duplicates drop
		order 	cod_mod anexo cod_local cui finfo fuente fecha_fuente conclusion
		save 	"$Output\CUIM_BrCe_SinVinc.dta", replace
	restore
	drop	if conclusion == 3
	
	* 	Fuentes confiables
	replace conclusion = 1 if finfo == "ARCC y PRONIED" | finfo == "Coordinación OXI" | finfo == "PRONIED y Coordinación OXI" | finfo == "PRONIED" | finfo == "PEIP" | finfo == "UE 118"	// ANIN no se agrega pues no afirman que cierra brecha.
	
	preserve		
		keep 	if conclusion == 1
		duplicates drop
		order 	cod_mod anexo cod_local cui finfo fuente fecha_fuente conclusion
		save 	"$Output\CUIM_BrCe_ValidProc1.dta", replace
	restore
	drop 	if conclusion == 1
	
	* 2. Criterios generales
	* ------------------------
	
	* 2.1. Descartar inversiones desactivadas
	* -------------------------------------------
	gen 	crit_1 = 1 if estado != "" & (estado == "DESACTIVADO PERMANENTE" | estado == "DESACTIVADO TEMPORAL")
	drop 	if crit_1 == 1
	
	* 2.2. Descartar inversiones inactivas, no vigentes, en formulacion, con ET, según proxy prelación		
	* -------------------------------------------------------------------------------------------------
	gen 	_prelacion = substr(proxy_prelacion,1,1)				
	gen 	crit_2 = 1 if _prelacion == "Z" & proxy_prelacion != "Z4 - RCC"
	replace crit_2 = 1 if _prelacion == "D" | _prelacion == "E" | _prelacion == "F" | _prelacion == "G"
	drop 	if crit_2 == 1
	
	* 2.3. Descartar inversiones por revisar
	* ----------------------------------------
	gen 	crit_3 = 1 if _prelacion == "R"
	drop 	if crit_3 == 1
	
	* 2.4. Descartar inversiones antiguas
	* -------------------------------------
	merge 	m:1 cod_local using "$Input\LE_SiCBr.dta", keepusing(cod_local) gen(_auxsi)
	drop 	if _auxsi == 2
	gen		si_cbr = 1 if _auxsi == 3
	drop 	_auxsi
		
	* Método 1: Fecha de cierre
	tab		estado cerrado_f9, m
	tab		estado proxy_prelacion
	gen	 	cerrado = cerrado_f9
	replace	cerrado = "NO" if estado == "ACTIVO"
	replace cerrado = "SI" if estado == "CERRADO"
	cap 	drop fecha_cierre
	tab		des_cierre_f9 cerrado, m
	gen 	fecha_cierre = date(fec_reg_cierre_f9, "YMD")
	gen 	CieAño = year(fecha_cierre)
	tab 	CieAño, m
	replace CieAño = . if fecha_cierre == .
	gen 	crit_4 = 1 if CieAño < 2018 & cerrado == "SÍ"
	
	* Método 2: Último devengado
	replace	crit_4 = 1 if ano_ultimo_dev < 2018
	replace crit_4 = 0 if si_cbr == 1
	drop 	if crit_4 == 1
		
	* 2.5. Descartar inversiones sin componente de infraestructura (o PI de activos estratégicos)
	* ----------------------------------------------------------------------------------------------
	
	* En variables de componentes F8, alternativa y componentes devengado SIAF. No considerar IOARR en este punto.
	gen 	infra = 1 if des_tipo_formato != "IOARR" & (ustrpos(componentes_f8, "AULAS") != 0 | ustrpos(componentes_f8, "INFRAESTRUCTURA") != 0)
	replace infra = 1 if des_tipo_formato != "IOARR" & (ustrpos(alternativa, "AULAS") != 0 | ustrpos(alternativa, "INFRAESTRUCTURA") != 0)
	replace infra = 1 if des_tipo_formato != "IOARR" & (ustrpos(componentes_dev_siaf_historico, "AULAS") != 0 | ustrpos(componentes_dev_siaf_historico, "INFRAESTRUCTURA") != 0)
	
	* Si no hay información en las variables anteriores, verificar nombre de inversión, pero considerar palabras clave que deben ser excluidas.
	local 	string INSTALACION INSTALACIÓN IMPLEMENTACION IMPLEMENTACIÓN EQUIPAMIENTO MOBILIARIO 
	foreach	var of local string {
		gen 	Ind_Nom_`var' = ustrpos(nombre_inversion, "`var'") > 0 if componentes_f8 == "" & alternativa == "" & componentes_dev_siaf_historico == ""
		replace Ind_Nom_`var' = 0 if Ind_Nom_`var' == .
		tab 	Ind_Nom_`var'
	}
	
	* En nombre de inversión, no considerar los siguientes activos estratégicos o palabras clave.
	local 	string2	MODULOS MÓDULOS TECHO PROTECCION PROTECCIÓN PSICOMOTRICIDAD INNOVACION INNOVACIÓN LABORATORIO 	///
					BIBLIOTECA AUDITORIO LIVIANA AMBIENTE ADMINISTRACION ADMINISTRACIÓN DEPOSITO DEPÓSITO 			///
					RESIDENCIA CERCO EXTERIOR COMPUTADORA COMEDOR MULTIMEDIA	
	foreach	var of local string2 {
		gen 	Ind_Nom_`var' = ustrpos(nombre_inversion, "`var'") > 0
		replace Ind_Nom_`var' = 0 if Ind_Nom_`var' == .
		tab 	Ind_Nom_`var'
	}
	replace Ind_Nom_CERCO = 0 if cui == 2179208 		// PI es de infraestructura.
	
	gen 	Ind_Nom_LOSA = ustrpos(nombre_inversion, "LOSA DEPORTIVA") > 0 | ustrpos(nombre_inversion, "LOSA MULTIDEPORTIVA") > 0
	replace Ind_Nom_LOSA = 0 if Ind_Nom_LOSA == .
	tab 	Ind_Nom_LOSA
	gen 	Ind_Nom_INFRADEPORTIVA = ustrpos(nombre_inversion, "INFRAESTRUCTURA DEPORTIVA") > 0 | ustrpos(nombre_inversion, "SERVICIOS DEPORTIVOS") > 0
	replace Ind_Nom_INFRADEPORTIVA = 0 if Ind_Nom_INFRADEPORTIVA == .
	tab 	Ind_Nom_INFRADEPORTIVA
	gen 	Ind_Nom_PROTSOLAR = ustrpos(nombre_inversion, "PROTECCIÓN SOLAR") > 0 | ustrpos(nombre_inversion, "PROTECCION SOLAR") > 0
	replace Ind_Nom_PROTSOLAR = 0 if Ind_Nom_PROTSOLAR == .
	tab 	Ind_Nom_PROTSOLAR
	gen 	Ind_Nom_EDUCFISICA = ustrpos(nombre_inversion, "EDUCACIÓN FÍSICA") > 0 | ustrpos(nombre_inversion, "EDUCACIÓN FISICA") > 0 | ustrpos(nombre_inversion, "EDUCACION FÍSICA") > 0 | ustrpos(nombre_inversion, "EDUCACION FISICA") > 0
	replace Ind_Nom_EDUCFISICA = 0 if Ind_Nom_EDUCFISICA == .
	tab 	Ind_Nom_EDUCFISICA
	gen		Ind_Nom_SSHH = ustrpos(nombre_inversion, "SSHH") > 0 | ustrpos(nombre_inversion, "SS.HH") > 0 | ustrpos(nombre_inversion, "HIGIÉNICOS") > 0  | ustrpos(nombre_inversion, "HIGIENICOS") > 0
	replace Ind_Nom_SSHH = 0 if Ind_Nom_SSHH == .
	tab 	Ind_Nom_SSHH
	
	* Revisar nombres de inversiones por excluir a partir de proceso anterior.
	local 	string3	INSTALACION INSTALACIÓN IMPLEMENTACION IMPLEMENTACIÓN EQUIPAMIENTO MOBILIARIO 					///
					MODULOS MÓDULOS TECHO PROTECCION PROTECCIÓN PSICOMOTRICIDAD INNOVACION INNOVACIÓN LABORATORIO 	///
					BIBLIOTECA AUDITORIO LIVIANA AMBIENTE ADMINISTRACION ADMINISTRACIÓN DEPOSITO DEPÓSITO  			///
					RESIDENCIA CERCO EXTERIOR COMPUTADORA COMEDOR MULTIMEDIA LOSA INFRADEPORTIVA PROTSOLAR 			///
					EDUCFISICA SSHH
	foreach var of local string3 {
		di	"`var'"
		count 	if Ind_Nom_`var' != 0
	}
	
local 	string3	INSTALACION INSTALACIÓN IMPLEMENTACION IMPLEMENTACIÓN EQUIPAMIENTO MOBILIARIO 	
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}	
	local 	string3	MODULOS MÓDULOS TECHO
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}
	local 	string3	PROTECCION
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}
	local 	string3	PROTECCIÓN PSICOMOTRICIDAD
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}
	local 	string3	INNOVACION
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}
	local 	string3	INNOVACIÓN LABORATORIO
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}
	local 	string3	BIBLIOTECA AUDITORIO LIVIANA
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}
	local 	string3	AMBIENTE
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}
	local 	string3	ADMINISTRACION ADMINISTRACIÓN DEPOSITO DEPÓSITO RESIDENCIA
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}	
	local 	string3	CERCO
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}
	local 	string3	EXTERIOR COMPUTADORA 
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}
	local 	string3	COMEDOR MULTIMEDIA 
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}
	local 	string3	LOSA INFRADEPORTIVA
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}	
	local 	string3	PROTSOLAR EDUCFISICA
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}	
	local 	string3	SSHH
	foreach var of local string3 {
		list 	nombre_inversion if Ind_Nom_`var' != 0
	}	
	
	egen	ActEst_NoInf = rowtotal(Ind_Nom_*)
	tab 	ActEst_NoInf
	replace infra = 0 if des_tipo_formato != "IOARR" & ActEst_NoInf != 0
	
	* Para IOARR, solo considerar nombre de inversión.	// REVISIÓN PENDIENTE
	replace infra = 1 if des_tipo_formato == "IOARR" & ActEst_NoInf == 0 & (ustrpos(nombre_inversion, "AULAS") != 0 | ustrpos(nombre_inversion, "INFRAESTRUCTURA") != 0)
	
	gen		crit_5 = infra != 1
	merge 	m:1 cui using "$Input\CUI_InvNoInf_250731.dta", keepusing(cui)			// Base con CUI que no son PI de infraestructura.
	replace crit_5 = 1 if _merge == 3
	replace infra = 0 if _merge == 3
	drop	if _merge == 2
	drop 	_merge
	drop 	if crit_5 == 1
	
	* 2.6. Descartar inversiones masivas (por lo general sólo involucran equipamiento)
	* ---------------------------------------------------------------------------------
	duplicates tag cui, gen(_aux)
	tab		_aux, m
	gen		crit_6 = 1 if _aux >= 9 

	* Excepciones:
	replace crit_6 = 0 if cui == 2074334 | cui == 2109591 | cui == 2152407 | cui == 2191110 | cui == 2156156 | 		///
						  cui == 2159270 | cui == 2303935 | cui == 2229653 											// PI masivos que involucran infraestructura
		
	drop 	if crit_6 == 1
	drop 	_aux
	
	* 2.7. Avance físico de la obra
	* ---------------------------------
	gen		crit_7 = 1 if avance_fisico_f12b < 0.85 & (_prelacion == "B" | _prelacion == "C")
	replace	crit_7 = 1 if avance_fisico_f12b == . & (_prelacion == "B" | _prelacion == "C")
	drop 	if crit_7 == 1
	
	* 2.8. Porcentaje de ejecución del coste
	* ---------------------------------
	gen 	crit_8 = 1 if p_ejec_costo_total < 0.85
	replace	crit_8 = 1 if p_ejec_costo_total == .
	drop 	if crit_8 == 1

	* 2.9. Descartar IOARRs
	* ---------------------------------
 	gen 	crit_9 = 1 if des_tipo_formato == "IOARR"
 	drop 	if crit_9 == 1
	
	* 2.10. Descartar LL.EE. con informe de riesgo u otra información adicional
	* ---------------------------------------------------------------------------
	merge 	m:1 cod_local using "$Input\LE_NoCBr.dta", keepusing(cod_local) gen(_auxno)
	drop 	if _auxno == 2
	gen		crit_10 = 1 if _auxno == 3
	drop 	if crit_10 == 1
	drop 	_auxno
	
	* Final
	* --------
	*	CUI que cumplen con todas los criterios.
	replace conclusion = 13 
	append 	using "$Output\CUIM_BrCe_ValidProc1.dta"
		
	gen 	fuente_final = finfo if conclusion == 1
	replace fuente_final = "Criterios DIPLAN-DIGEIE (V)" if conclusion == 13
		
	keep 	cod_mod anexo cod_local cui fuente fecha_fuente fuente_final conclusion si_cbr
	duplicates report cod_local cod_mod cui
	
	compress
	save 	"$Output\CUIM_CBr_Valid.dta", replace
	
	
 	* 3. Consolidación
	* -------------------------------------
	* Intervenciones que cierran brecha
	use		"$Output\CUIM_CBr_Valid", clear
	
	sort 	cod_local cod_mod anexo cui
	duplicates report cod_local
	duplicates report cod_mod anexo cui
	
	* Informacion de la base de inversiones
	merge 	1:m cod_mod anexo cui using "$Input\CUIMod_Inv_251227", keep(3) nogen
	
	* Verificando servicios --> CEBA
	merge 	m:1 cod_mod anexo using "$Input\M_251203", keepusing(niv_mod) keep(1 3)
	duplicates report cod_local cod_mod cui
	drop 	if _merge == 1 // solo activos
	drop 	_m*
	 
	*tab  	d_niv_mod niv_mod
	gen 	niv = substr(niv_mod,1,1)
	gen 	cui_aux = cui
	list 	if 	cui_aux == .
	bysort cod_local: replace cui_aux = cui[_n-1] if niv == "D" & missing(cui)
	bysort cod_local: replace conclusion = conclusion[_n-1] if niv == "D" & missing(cui)
	
	* Corrigiendo errores
	replace des_tipo_formato = trim(itrim(des_tipo_formato))
	replace estado = trim(itrim(estado))
	replace cerrado_f9 = "NO" if estado == "ACTIVO"
	replace cerrado_f9 = "SÍ" if estado == "CERRADO"
	
	gen 	fecha_cierre = date(fec_reg_cierre_f9, "YMD")
	gen 	CieAño = year(fecha_cierre)
	replace fecha_cierre = . if CieAño == 1899
	replace CieAño = . if fecha_cierre == .
	drop 	CieAño
	
	replace componentes_f8 = trim(itrim(componentes_f8))
	replace alternativa = trim(itrim(alternativa))
	replace alternativa = upper(alternativa)
	replace nombre_inversion = trim(itrim(nombre_inversion))
	replace nombre_inversion = upper(nombre_inversion)
	
	* Verificar nuevamente el año de término
		* Método 1: Fecha de cierre
		tab		estado cerrado_f9, m
		tab		estado proxy_prelacion
		gen	 	cerrado = cerrado_f9
		replace	cerrado = "NO" if estado == "ACTIVO"
		replace cerrado = "SI" if estado == "CERRADO"
		
		tab		des_cierre_f9 cerrado, m
		cap 	drop fecha_cierre
		gen 	fecha_cierre = date(fec_reg_cierre_f9, "YMD")
		gen 	CieAño = year(fecha_cierre)
		tab 	CieAño, m
		
		replace CieAño = . if fecha_cierre == .
		gen 	crit_4 = 1 if CieAño < 2018 & cerrado_f9 == "SÍ"
	
		* Método 2: Último devengado
		replace	crit_4 = 1 if ano_ultimo_dev < 2018
		replace crit_4 = 0 if si_cbr == 1
		drop 	if crit_4 == 1

	* Variables adicionales
	gen 	CUIM_CBr = conclusion == 1 | conclusion == 13
	tab 	conclusion CUIM_CBr
	
	lab		define dico 0 "NO" 1 "SI"
	label 	values CUIM_CBr dico
	
	* 	Guardar data
	duplicates report cod_mod cui
	compress
	save 	"$Output\CUIM_CBr_Valid_Reporte.dta", replace
	