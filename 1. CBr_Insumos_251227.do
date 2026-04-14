	/*___________________________________________________________
	|	                                                      	|
	|	MINEDU - Intervenciones Brecha Cerrada - Insumos		|						
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
	global  CalcAnt = 	"C:\CalcBrPr2507\" 					// Carpeta Cálculo Anterior
	global  InputAnt = 	"C:\CalcBrPr2507\intcb\input" 		// Carpeta Cálculo Anterior (Input IntCB)
	global  CalcAnt2 = 	"C:\CalcBrPr2412\" 					// Carpeta Cálculo Anterior (2024-12)
	
	cd 		"$main"

	set 	more off 
	set 	varabbrev off
	set 	type double
	set 	seed 339487731
	set 	excelxlsxlargefile on	
	
 	* 1. Insumos para validación
	* -------------------------------------	
		
**	1.1 BASE MAESTRA
	
	use 	"$CalcAnt\LE_BasePr.dta", clear // base del último cálculo
		
	preserve
		keep 	codlocal cod_local region prov dist ugel nom_local brecha_ini intcb_ue intcb_niv
		compress
		save 	"$Input\LE_Maestro.dta", replace
	restore
	
	keep 	if finfo == 1
	keep 	cod_local codlocal intcb_ue intcb_niv
	compress
	save 	"$Input\LE_BrCe.dta", replace
	
**	1.2 PADRÓN DEL ÚLTIMO CÁLCULO
	
	use 	"$CalcAnt\input\M_Matri.dta" , clear 									// Esta base ya contiene el análisis de matrícula y estado
	*drop 	if act == 0*
	rename	id_local cod_local
	keep 	cod_mod anexo cod_local
	save 	"$Input\ModAnexo_Padron_CaBrCe.dta", replace
		
**	1.3 PADRÓN ACTUAL
	
	use 	"$Input\M_251203.dta", clear
	
*** Verificar si nuevos módulos activos cuentan con matricula
	gen 	alert_1 = act == 1 & matri == 0
	gen 	act2 = act 
	replace act2 = 0 if alert_1 == 1

	keep 	cod_mod anexo cod_local alert_1
	save 	"$Input\ModAnexo_Padron_Nuevo.dta", replace
 	
	* 2. Insumos para identificación
	* -------------------------------------	
	
*	2.1 PRONIED
*	———————————————————					

	* Procesar bases 2025-12 y Evaluación PNIE
	import excel "$Raw\PRONIED_Anexo1", sheet("Anexo") cellrange(A1:M44) firstrow case(lower) clear
	rename 	(códigolocal códigomodular cui escorrectoqueestelocaleduc)	///
			(cod_local cod_mod cui culm)	
	keep 	if culm == "SI"
	
	gen		anexo = 0
	gen 	fuente = "PRONIED"
	gen		fecha_fuente = "Dic-2025"
	keep 	cod_local cod_mod anexo cui fuente fecha_fuente
	order 	cod_local cod_mod anexo cui fuente fecha_fuente
	compress	
 	save	"$Input\PRONIED_2512_1", replace
	
	import excel "$Raw\PRONIED_Anexo2", sheet("LLEE_Proyectos de Inversión") cellrange(A2:O6) firstrow case(lower) clear
	rename 	(códigolocal códigomodular códigounificadodeinversiónc etapadelproyectovernota1 estadodelproyectovernota2 avancefísicodelproyecto indicarelalcancedelproyecto)	///
			(cod_local cod_mod cui etapa estado avfisico alcance)	
	keep 	if etapa >= 4 & etapa <= 7 & (estado == 1 | estado == 4) & avfisico >= 0.85 & avfisico != . & (alcance == 3 | alcance == 5)
	replace cod_mod = "" if cod_mod == "-"
	destring cod_local, replace
	destring cod_mod, replace

	merge 	1:m cod_local using "$Input\M_251203.dta", keepusing(cod_mod anexo) keep(1 3 4 5) update replace
	gen 	fuente = "PRONIED"
	gen		fecha_fuente = "Dic-2025"
	keep 	cod_local cod_mod anexo cui fuente fecha_fuente
	order 	cod_local cod_mod anexo cui fuente fecha_fuente
	compress	
 	save	"$Input\PRONIED_2512_2", replace
	
	import excel "$Raw\PRONIED_EvPNIE", sheet("Data") cellrange(A1:F137) firstrow case(lower) clear
	rename 	(códigoúnicodeinversióncui códigodellocaleducativo fechaderecepcióndeobraoes fuente)	///
			(cui cod_local f_obra fuente2)	
	gen		y_obra = year(f_obra)

	merge 	1:m cod_local using "$Input\M_251203.dta", keepusing(cod_mod anexo) keep(1 3)
	gen 	fuente = "PRONIED"
	gen		fecha_fuente = "Sep-2025"
	keep 	cod_local cod_mod anexo cui fuente fecha_fuente y_obra fuente2
	order 	cod_local cod_mod anexo cui fuente fecha_fuente y_obra fuente2
	compress	
 	save	"$Input\PRONIED_2509", replace
	
	***** Consolidar
	use 	"$Input\PRONIED_2510_1", clear
	append	using "$Input\PRONIED_2510_2" "$Input\PRONIED_2512_1" "$Input\PRONIED_2512_2", gen(aux)
	
	**** verificar cambios en los códigos de local
	rename 	cod_local cod_local_ini
	merge	m:1 cod_mod anexo using "$Input\M_251203.dta", keepusing(cod_local) keep(1 3)
	drop 	_merge
	
	duplicates tag cui cod_mod anexo, gen(aux2)
	drop 	if aux2 != 0 & fecha_fuente == "Oct-2025"
	drop	aux2
	
	duplicates report cui cod_mod anexo				// Revisar si hay duplicados
	*drop 	if aux == X &
	drop	aux
	
	merge 	1:1 cod_mod anexo cui using "$Input\PRONIED_2509"
	gen		evpnie = _merge == 2 | _merge == 3
	drop	_merge
	
	sort	cod_local cod_mod anexo cui
	order 	cod_local cod_mod anexo cui fuente fecha_fuente y_obra
	compress		
	save 	"$Input\PRONIED_2512p", replace
		
* 	2.2 Articulación con regiones
* 	———————————————————
	
	import excel "$Raw\Consolidado II - CdB_vf", sheet("Inf. recibida") cellrange(A1:AF633) firstrow case(lower) clear
	rename 	(códigolocal códigomodular códigounificadodeinversiónc seculminólaejecuciónselec añodeculminacióndelaobra seencuentraenprocesodeliqu fechaestimadadecierre)	///
			(cod_local cod_mod cui culm y_culm liq fe_liq)
	replace culm = "Si" if culm == "SI"
	keep 	if culm == "Si" | y_culm < 2025 	// Verificar si hay LL.EE. que están en liquidación pero de 2024 para atrás.
				
	gen 	fuente = "Articulación con Regiones"
	gen		fecha_fuente = "Dic-2025"
	gen 	y_obra = y_culm
	keep 	cod_local cod_mod cui fuente fecha_fuente y_obra

	**** verificar cambios en los códigos de local
	gen		anexo = 0
	rename 	cod_local cod_local_ini
	merge	m:1 cod_mod anexo using "$Input\M_251203.dta", keepusing(cod_local) keep(1 3)
	drop 	_merge
	
	sort	cod_local cod_mod anexo cui
	order 	cod_local cod_mod anexo cui fuente fecha_fuente
	compress		
	save 	"$Input\AcR_2512p", replace
	
* 	2.3 OxI
* 	———————————————————
	use 	"$Input\OXI_2512", clear
	*gen 	fuente = "Coordinación OXI"			// Base ya está procesada.
	*gen	fecha_fuente = "Dic-2024"
	*gen 	y_obra = 2024
	*keep 	codlocal cod_mod anexo cui fuente fecha_fuente y_obra
	
	**** verificar cambios en los códigos de local
	drop	cod_local_ini
	rename 	cod_local cod_local_ini
	merge	m:1 cod_mod anexo using "$Input\M_251203.dta", keepusing(cod_local) keep(1 3)
	drop 	_merge
	
	sort	cod_local cod_mod anexo cui
	order 	cod_local cod_mod anexo cui fuente fecha_fuente
	compress		
	save	"$Input\OXI_2512p", replace
	
* 	2.4 PEIP
* 	———————————————————
	use 	"$Raw\PEIP_2512", clear
	merge 	1:m cui cod_local using "$Input\CUIMod_Vinc_251227", keep(1 3) nogen		//	Unir con base de datos de vinculaciones

	**** verificar cambios en los códigos de local
	rename 	cod_local cod_local_ini
	merge	m:1 cod_mod anexo using "$Input\M_251203.dta", keepusing(cod_local) keep(1 3)
	drop 	_merge
	
 	gen 	fuente = "PEIP"
 	gen		fecha_fuente = "Dic-2025"
 	keep 	cod_local cod_mod anexo cui fuente fecha_fuente y_obra
	sort	cod_local cod_mod anexo cui
	order 	cod_local cod_mod anexo cui fuente fecha_fuente
	compress	
 	save	"$Input\PEIP_2512p", replace
	
*	2.5 ANIN
* 	———————————————————
	import excel "$CalcAnt\intcb\raw\ANIN_2507", sheet("ANIN") cellrange(A2:G152) firstrow case(lower) clear
	rename 	códigoúnicodelairi cui
	merge 	1:m cui using "$Input\CUIMod_Vinc_251227", keep(1 3) nogen		//	Unir con base de datos de vinculaciones
	keep 	if estado == "OBRA CULMINADA"
	
	**** verificar cambios en los códigos de local
	rename 	cod_local cod_local_ini
	merge	m:1 cod_mod anexo using "$Input\M_251203.dta", keepusing(cod_local) keep(1 3)
	drop 	_merge	
	
	gen 	fuente = "ANIN"
	gen		fecha_fuente = "Jul-2025"
	sort	cod_local cod_mod anexo cui
	order 	cod_local cod_mod anexo cui fuente fecha_fuente
	compress		
	save 	"$Input\ANIN_2507p", replace

	import excel "$Raw\ANIN_2512", sheet("Pt.01. I.E brechas cerradas") cellrange(A1:L72) firstrow case(lower) clear
	rename 	fur cui
	merge 	1:m cui using "$Input\CUIMod_Vinc_251227", keep(1 3) nogen		//	Unir con base de datos de vinculaciones
	
	**** verificar cambios en los códigos de local
	rename 	cod_local cod_local_ini
	merge	m:1 cod_mod anexo using "$Input\M_251203.dta", keepusing(cod_local) keep(1 3)
	drop 	_merge	
	
	gen 	fuente = "ANIN"
	gen		fecha_fuente = "Dic-2025"
	sort	cod_local cod_mod anexo cui
	order 	cod_local cod_mod anexo cui fuente fecha_fuente		
	
	append	using "$Input\ANIN_2507p", gen(aux)
	duplicates tag cui cod_mod anexo, gen(aux2)
	drop 	if aux2 != 0 & fecha_fuente == "Jul-2025"
	drop	aux*
	
	sort	cod_local cod_mod anexo cui
	order 	cod_local cod_mod anexo cui fuente fecha_fuente
	compress		
	save 	"$Input\ANIN_2512p", replace
	
* 	2.6 UE 118
* 	———————————————————
	use 	"$Raw\UE118_2512", clear
	merge 	1:m cui cod_mod anexo using "$Input\CUIMod_Vinc_251227", keep(1 3) nogen		//	Unir con base de datos de vinculaciones

	**** verificar cambios en los códigos de local
	rename 	cod_local cod_local_ini
	merge	m:1 cod_mod anexo using "$Input\M_251203.dta", keepusing(cod_local) keep(1 3)
	drop 	_merge
	
 	gen 	fuente = "UE 118"
 	gen		fecha_fuente = "Dic-2025"
 	keep 	cod_local cod_mod anexo cui fuente fecha_fuente y_obra
	sort	cod_local cod_mod anexo cui
	order 	cod_local cod_mod anexo cui fuente fecha_fuente
	compress	
 	save	"$Input\UE118_2512p", replace

	* 3. Niveles por local educativo
	* -------------------------------------	
	use		"$Input\M_251203.dta", clear
	
	* A nivel de LE
	collapse 	(sum) 	Inicial Primaria Secundaria EBA EBE CETPRO IST ISP ESFA, by(cod_local) 
	
	list if 	Primaria > 1 | Secundaria > 1 | Inicial > 2 // LE con dos servicios o más dentro de un mismo nivel
	foreach var of varlist Inicial Primaria Secundaria EBA EBE CETPRO IST ISP ESFA {
			replace `var' = 1 if `var' > 1				// Correción: sólo consideramos un módulo por nivel
	}
	
	compress	
	save	"$Input\LE_NivEdu.dta", replace

	
	* 4. Cambios de código de local educativo
	* -----------------------------------------
	use 	"$Input\ModAnexo_Padron_CaBrCe.dta", clear
	
	merge	m:1 cod_local using "$Input\LE_BrCe.dta", keep(matched) nogen 	// LE con brecha cerrada en el último cálculo
	rename 	cod_local cod_local_ini 										// Para verificar cambios
	drop	codlocal
	merge 	1:1	cod_mod anexo using "$Input\ModAnexo_Padron_Nuevo.dta", keep(matched) gen(_aux1)
	
	* Identificar cambios
	gen		ind1 = cod_local_ini != cod_local
	list 	if 	ind1 == 1
		
	bys 	cod_local_ini: egen ind2 = max(ind1)
	list 	if 	ind2 == 1
		
	/* SSEE que cambian de local */
		
	* Generar indicador de conclusion de lista a validar
	gen		conclusion = ind1 == 1 & cod_local == .
	replace cod_local = cod_local_ini if conclusion == 1 // completa la información con base verde anterior
	
	* Incluir nuevos servicios a un local considerado con brecha cerrada
	merge	1:1 cod_local cod_mod using "$Input\ModAnexo_Padron_Nuevo.dta"
	
	bys		cod_local: egen _aux2 = min(_merge)
	bys		cod_local: egen _aux3 = max(_merge)
	
	tab		_aux2 _aux3, m
	list	cod_mod anexo cod_local cod_local_ini _aux2 _aux3 _aux1 if 	_aux2 == 2 & _aux3 == 3
	
	* Incluir nuevos códigos modulares
	gen 	neo = 1 if _aux2 == 2 & _aux3 == 3
	replace	_aux1 = 3 if _aux2 == 2 & _aux3 == 3 & _aux1 == .
	replace	cod_local_ini = cod_local if _aux2 == 2 & _aux3 == 3 & cod_local_ini == .
	
	drop 	if _merge == 2 & _aux1 == . // servicios fuera del análisis
	drop	_m* _aux*
	
	order	cod_mod anexo cod_local neo, first
	order 	cod_local_ini, last
	tab		anexo, m
	
	compress
	save 	"$Input\Mod_Padron_Validar.dta", replace
	
	
	* 5. Consolidación
	* ------------------
	use		"$Input\PRONIED_2512p.dta", clear
	merge 	1:1 cod_mod anexo cui using "$Input\OXI_2512p", gen(_aux1) 				// OXI
	merge 	1:1 cod_mod anexo cui using "$Input\PEIP_2512p", gen(_aux2)	 			// PEIP EB
	merge 	1:1 cod_mod anexo cui using "$Input\AcR_2512p", gen(_aux3) 				// Art Regiones
	merge	1:1 cod_mod anexo cui using "$Input\ANIN_2512p", gen(_aux4) keepusing(cod_local cod_mod anexo cui fuente fecha_fuente cod_local_ini) 	// ANIN
	merge 	1:1 cod_mod anexo cui using "$Input\UE118_2512p", gen(_aux5)			// UE 118

	duplicates report cui cod_mod anexo

	* Generar fuentes de información
	gen 	finfo_1 = ""
	replace	finfo_1 = "PRONIED" if _aux1 == 1
	replace finfo_1 = "Coordinación OXI" if _aux1 == 2
	replace finfo_1 = "PRONIED y Coordinación OXI" if _aux1 == 3
	gen 	finfo_2 = "PEIP EB" if _aux2 == 2
	gen 	finfo_3 = "Equipo AcR" if _aux3 == 2
	replace finfo_3 = " y Equipo AcR" if _aux3 == 3
	gen 	finfo_4 = "ANIN" if _aux4 == 2
	replace finfo_4 = " y ANIN" if _aux4 == 3
	gen 	finfo_5 = "UE 118" if _aux5 == 2
	replace finfo_5 = " y UE 118" if _aux5 == 3	

	egen 	finfo = concat(finfo_1 finfo_2 finfo_3 finfo_4 finfo_5)
	drop 	_aux* finfo_*
	replace y_obra = . if y_obra > 2025
	
	sort	cod_local cod_mod anexo cui
	order 	cod_local cod_mod anexo cui fuente fecha_fuente finfo
	compress		
	save 	"$Input\CUIM_BrCe_Id.dta", replace
	