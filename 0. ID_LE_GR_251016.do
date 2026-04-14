	/*___________________________________________________________
	|	                                                      	|
	|	MINEDU - Base de Proy Potenciales Brecha Cerrada - GR	|
	|	Actualizado: 16/10/2025	                				|
	|___________________________________________________________*/

 	* 0. Prólogo
	* -------------------------------------

	clear 	all
	
	global 	Main	=	"C:\CalcBrPr2512\intcb\id" 			// Carpeta Principal ID
	global 	Raw		=	"C:\CalcBrPr2512\intcb\raw"			// Carpeta Bases Iniciales
	global 	Input	=   "C:\CalcBrPr2512\intcb\input" 		// Carpeta Input 
	global 	Output	=   "C:\CalcBrPr2512\intcb\output" 		// Carpeta Output
	global  CalcAnt = 	"C:\CalcBrPr2507\" 					// Carpeta Cálculo Anterior
	
	cd 		"$Main"

	set 	more off 
	set 	varabbrev off
	set 	type double
	set 	seed 339487731
	set 	excelxlsxlargefile on	

	* 1. Criterios generales
	* ------------------------
	use 	"$Input\CUIMod_Inv_251013.dta", clear

	* 1.1. Descartar inversiones desactivadas
	* ----------------------------------
	gen 	crit_1 = 1 if estado != "" & (estado == "DESACTIVADO PERMANENTE" | estado == "DESACTIVADO TEMPORAL")
	drop 	if crit_1 == 1
	
	* 1.2. Descartar inversiones inactivas, no vigentes, en formulacion, con ET, según proxy prelación		
	* -------------------------------------------------------------------------------------------------
	gen 	_prelacion = substr(proxy_prelacion,1,1)
	gen 	crit_2 = 1 if _prelacion == "Z" & proxy_prelacion != "Z4 - RCC"
	replace crit_2 = 1 if _prelacion == "D" | _prelacion == "E" | _prelacion == "F" | _prelacion == "G"
	drop 	if crit_2 == 1

	* 1.3. Descartar inversiones por revisar
	* ----------------------------------------
	gen 	crit_3 = 1 if _prelacion == "R"
	drop 	if crit_3 == 1 
	
	* 1.4. Descartar inversiones antiguas
	* -------------------------------------
		
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
	drop 	if crit_4 == 1
		
	* 1.5. Descartar inversiones sin componente de infraestructura (o PI de activos estratégicos)
	* ----------------------------------------------------------------------------------------------
	
	* En variables de componentes F8, alternativa y componentes devengado SIAF. No considerar IOARR en este punto.
	gen 	infra = 1 if des_tipo_formato != "IOARR" & (ustrpos(componentes_f8, "AULAS") != 0 | ustrpos(componentes_f8, "INFRAESTRUCTURA") != 0)
	replace infra = 1 if des_tipo_formato != "IOARR" & (ustrpos(alternativa, "AULAS") != 0 | ustrpos(alternativa, "INFRAESTRUCTURA") != 0)
	replace infra = 1 if des_tipo_formato != "IOARR" & (ustrpos(componentes_dev_siaf_historico, "AULAS") != 0 | ustrpos(componentes_dev_siaf_historico, "INFRAESTRUCTURA") != 0)
	
	* Si no hay información en las variables anteriores, verificar nombre de inversión, pero considerar palabras clave que deben ser excluidas.
	local 	string INSTALACION IMPLEMENTACION INSTALACIÓN IMPLEMENTACIÓN EQUIPAMIENTO MOBILIARIO
	foreach	var of local string {
		gen 	Ind_Nom_`var' = ustrpos(nombre_inversion, "`var'") > 0 if componentes_f8 == "" & alternativa == "" & componentes_dev_siaf_historico == ""
		replace Ind_Nom_`var' = 0 if Ind_Nom_`var' == .
		tab 	Ind_Nom_`var'
	}
	
	* En nombre de inversión, no considerar los siguientes activos estratégicos o palabras clave.
	local 	string2	MÓDULOS TECHO PROTECCION PROTECCIÓN PSICOMOTRICIDAD INNOVACION INNOVACIÓN LABORATORIO 	///
					BIBLIOTECA AUDITORIO LIVIANA AMBIENTE ADMINISTRACION ADMINISTRACIÓN DEPÓSITO DEPOSITO 	///
					RESIDENCIA CERCO EXTERIOR COMPUTADORA COMEDOR MULTIMEDIA	
	foreach	var of local string2 {
		gen 	Ind_Nom_`var' = ustrpos(nombre_inversion, "`var'") > 0
		tab 	Ind_Nom_`var'
	}
	replace Ind_Nom_CERCO = 0 if cui == 2179208 		// PI es de infraestructura.
	
	gen 	Ind_Nom_LOSA = ustrpos(nombre_inversion, "LOSA DEPORTIVA") > 0 | ustrpos(nombre_inversion, "LOSA MULTIDEPORTIVA") > 0
	tab 	Ind_Nom_LOSA
	gen 	Ind_Nom_INFRADEPORTIVA = ustrpos(nombre_inversion, "INFRAESTRUCTURA DEPORTIVA") > 0 | ustrpos(nombre_inversion, "SERVICIOS DEPORTIVOS") > 0
	tab 	Ind_Nom_INFRADEPORTIVA
	gen 	Ind_Nom_PROTSOLAR = ustrpos(nombre_inversion, "PROTECCIÓN SOLAR") > 0 | ustrpos(nombre_inversion, "PROTECCION SOLAR") > 0
	tab 	Ind_Nom_PROTSOLAR
	gen 	Ind_Nom_EDUCFISICA = ustrpos(nombre_inversion, "EDUCACIÓN FÍSICA") > 0 | ustrpos(nombre_inversion, "EDUCACIÓN FISICA") > 0 | ustrpos(nombre_inversion, "EDUCACION FÍSICA") > 0 | ustrpos(nombre_inversion, "EDUCACION FISICA") > 0
	tab 	Ind_Nom_EDUCFISICA
	gen		Ind_Nom_SSHH = ustrpos(nombre_inversion, "SSHH") > 0 | ustrpos(nombre_inversion, "SS.HH") > 0 | ustrpos(nombre_inversion, "HIGIÉNICOS") > 0  | ustrpos(nombre_inversion, "HIGIENICOS") > 0
	tab 	Ind_Nom_SSHH
	
	* Revisar nombres de inversiones por excluir a partir de proceso anterior.
	local 	string3	INSTALACION IMPLEMENTACION INSTALACIÓN IMPLEMENTACIÓN EQUIPAMIENTO MOBILIARIO 			///
					MÓDULOS TECHO PROTECCION PROTECCIÓN PSICOMOTRICIDAD INNOVACION INNOVACIÓN LABORATORIO 	///
					BIBLIOTECA AUDITORIO LIVIANA AMBIENTE ADMINISTRACION ADMINISTRACIÓN DEPÓSITO DEPOSITO 	///
					RESIDENCIA CERCO EXTERIOR COMPUTADORA COMEDOR MULTIMEDIA LOSA INFRADEPORTIVA PROTSOLAR 	///
					EDUCFISICA SSHH
	
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
	
	* 1.6. Descartar inversiones masivas (por lo general sólo involucran equipamiento)
	* ---------------------------------------------------------------------------------
	duplicates tag cui, gen(_aux)
	tab		_aux, m
	gen		crit_6 = 1 if _aux >= 9 
	
	* Excepciones:
	replace crit_6 = 0 if cui == 2074334 | cui == 2109591 | cui == 2152407 | cui == 2191110 | cui == 2156156 | 	///
						  cui == 2159270 | cui == 2303935 | cui == 2229653 										// PI masivos que involucran infraestructura
		
	drop 	if crit_6 == 1
	drop 	_aux
	
	* 1.7. Avance físico de la obra >= 85%
	* -------------------------------------
	gen		crit_7 = 1 if avance_fisico_f12b < 0.85 & (_prelacion == "B" | _prelacion == "C")
	replace	crit_7 = 1 if avance_fisico_f12b == . & (_prelacion == "B" | _prelacion == "C")
	drop 	if crit_7 == 1
	
	* 1.8. Porcentaje de ejecución del coste >= 85%
	* ----------------------------------------------
	gen 	crit_8 = 1 if p_ejec_costo_total < 0.85
	replace	crit_8 = 1 if p_ejec_costo_total == .
	drop 	if crit_8 == 1

	* 1.9. Descartar IOARRs
	* ---------------------------------
 	gen 	crit_9 = 1 if des_tipo_formato == "IOARR"
 	drop 	if crit_9 == 1
	
	* 1.10. Descartar LL.EE. con informe de riesgo u otra información adicional
	* ---------------------------------------------------------------------------	
	merge 	m:1 cod_local using "$Input\LE_NoCBr.dta", keepusing(cod_local) gen(_auxno)
	drop 	if _auxno == 2
	gen		crit_10 = 1 if _auxno == 3
	drop 	if crit_10 == 1
	drop 	_auxno	
	
	
	* 2. Criterios específicos
	* --------------------------
	
	* 2.1. Solo considerar inversiones de GR
	* ---------------------------------------
 	gen 	crite_1 = 1 if nivel == "GR"
 	keep 	if crite_1 == 1
	
	* 3. Consolidación de información
	* ---------------------------------

	//	Unir con la base verde para saber ubicacion
	merge 	m:1 cod_local using "$Input\LE_Activos_251010.dta", keep(1 3) keepusing(region prov dist) gen(_aux1)
	
	merge	m:1 cod_local using "$CalcAnt\LE_BasePr.dta", keep(1 3) keepusing(region prov dist prioridad) gen(_aux2)
	drop 	if prioridad == 6 // ya considerados con brecha cerrada
	
	order 	cod_local cod_mod region prov dist cui nombre_inversion des_tipo_formato estado situacion marco  ///
			uep_ultima uf uei fecha_registro fecha_viabilidad costo_actualizado_bi etapa_f8  ///
			componentes_f8 tiene_f9 ano_ultimo_dev dev_acum_ano_actual avance_fisico_f12b ///
			p_ejec_costo_total	
			
	sort 	cod_local cod_mod cui
	
	compress
	save 	"LEModCUI_CrBr_GR.dta", replace
	
	* Exportar a excel
	use 	"LEModCUI_CrBr_GR.dta", clear
	
	order 	cod_local cod_mod region prov dist cui nombre_inversion des_tipo_formato estado situacion marco  ///
			uep_ultima uf uei fecha_registro fecha_viabilidad costo_actualizado_bi etapa_f8  ///
			componentes_f8 tiene_f9 ano_ultimo_dev dev_acum_ano_actual avance_fisico_f12b ///
			p_ejec_costo_total
			
	keep 	cod_local cod_mod region prov dist cui nombre_inversion des_tipo_formato estado situacion marco  ///
			uep_ultima uf uei fecha_registro fecha_viabilidad costo_actualizado_bi etapa_f8  ///
			componentes_f8 tiene_f9 ano_ultimo_dev dev_acum_ano_actual avance_fisico_f12b ///
			p_ejec_costo_total		
	
	export 	excel using "Anexo_GR.xlsx", firstrow(var) replace