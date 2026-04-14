	/*___________________________________________________________
	|	                                                      	|
	|	MINEDU - Intervenciones Brecha Cerrada - Consolidación	|						
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
	
	cd 		"$main"

	set 	more off 
	set 	varabbrev off
	set 	type double
	set 	seed 339487731
	set 	excelxlsxlargefile on
		
 	
	* 1. Preparación de bases
	* ------------------------
	
* 	[1.1] PRIMERA CONSOLIDACION
* 	"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
	* Datos de la primera validación
	use		"$Output\CUIM_CBr_Valid_Reporte.dta", clear
	
	* Datos de la validación de nuevos LE
	append	using "$Output\CUIM_CBr_Id_Reporte.dta", gen(_aux1)
	
	gen 	etapa = 1 if _aux1 == 0
	replace etapa = 2 if _aux1 == 1
	label 	define etapa 1 "VALIDACIÓN" 2 "IDENTIFICACIÓN"
	label 	values etapa etapa
	duplicates tag cod_mod anexo cui, gen(dup)
	sort 	cod_local cod_mod anexo cui etapa
	drop 	if dup == 1 & etapa == 2
	replace fuente_final = "Criterios DIPLAN-DIGEIE" if fuente_final == "Criterios DIPLAN"
	
	compress
	save	"$Output\CUIM_CBr_Consolidado.dta", replace
	
* 	[1.2] MODULOS CON BRECHA CERRADA
* 	"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
	use		"$Output\CUIM_CBr_Consolidado.dta", clear
	
	* Filtrar módulos con brecha cerrada
	tab		CUIM_CBr, m
	keep	if CUIM_CBr == 1
	
	* Verificar que tdos los módulos del local cierran brecha
* 	[1.10] PI INTEGRAL EN TODOS LOS NIVELES
* 	----------------------------------------------
	cap 	drop niv_mod
	merge	m:1 cod_mod anexo using "$Input\M_251203.dta", keep(1 3)
	summarize Inicial Primaria Secundaria EBA EBE CETPRO IST ISP ESFA
	drop   niv niv_mod
	
	* Redimensionar
	sort 	cod_local cod_mod anexo cui
	
	preserve
		collapse (sum) Inicial Primaria Secundaria EBA EBE CETPRO IST ISP ESFA, by(cod_local)
		local 	NivEdu Inicial Primaria Secundaria EBA EBE CETPRO IST ISP ESFA
		foreach var of local NivEdu {
			replace `var' = 1 if `var' > 1 
		}
		rename (Inicial Primaria Secundaria EBA EBE CETPRO IST ISP ESFA) (=_1)
		
		merge	m:1 cod_local using "$Input\LE_NivEdu.dta", keep(1 3)
		local NivEdu Inicial Primaria Secundaria EBA EBE CETPRO IST ISP ESFA
		foreach var of local NivEdu {
				gen Dif_`var' = `var'_1 != `var'
				summarize Dif_`var'
		}
		replace Dif_EBA = 0														//	No se considera diferencia en servicios de EBA.
		replace Dif_CETPRO = 0 if cod_local == 146187 | cod_local == 292254		//	No se considera diferencia en servicios de CETPRO para estos locales. El PI sí interviene en esta forma educativa.
		egen 	Dif_total = rowtotal(Dif_*) 									// 	si # servicios distintos a intervenidos
		tab 	Dif_total
		
		* Manteniendo las intervenciones en cada LE no integrales en niveles
		keep 	if Dif_total != 0
		
		duplicates report cod_local
		compress
		save	"$Input\LE_PINoInt.dta", replace
	restore
	
	* Identificando intervenciones no integrales
	cap 	drop _merge
	merge	m:1	cod_local using "$Input\LE_PINoInt.dta", keepusing(cod_local) keep(1 3)
	replace	conclusion = 10 if _merge == 3
	drop _m*
	
	* Eliminar de la base principal de validación
	drop	if conclusion == 10	
	
	* Guardar
	rename	fuente_final finfo
	duplicates report cod_mod anexo cui
	compress
	save 	"$Output\CUIM_CBr_Final.dta", replace
	
* 	[1.3] LE CON BRECHA CERRADA - BASE PARA CALCULO
* 	"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
	use		"$Output\CUIM_CBr_Final.dta", clear
	keep	cod_local cui nombre_inversion nivel opmi uf uei fuente finfo costo_actualizado_bi _aux1 etapa fuente2 evpnie
	destring costo_actualizado_bi, replace
	duplicates drop
	gsort	cod_local -costo_actualizado_bi
	by		cod_local: gen N = _n
	duplicates report cod_local
	keep	if N == 1
	duplicates report cod_local
	
	* Brecha inicial
	gen 	cost_act = costo_actualizado_bi
	merge 	m:1 cod_local using "$CalcAnt\LE_BasePr", keepusing(brecha_ini finfo_ini) keep(1 3)
	rename	finfo_ini finfo_ini_b
	egen 	inv_total = total(cost_act), by(cod_local)
	gen 	percent_ccb = inv_total / brecha_ini
	
	keep	cod_local uei nivel
	duplicates drop

	compress
	save	"$Main\IntCBr.dta", replace

* 	[1.4] LE CON BRECHA CERRADA - BASE PARA UPI
* 	"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
	use		"$Output\CUIM_CBr_Final.dta", clear
	
	destring costo_actualizado_bi, replace
	keep	cod_local cod_mod cui nivel opmi uf uei uep  costo_actualizado_bi _aux1 etapa
	gsort	cod_local cod_mod cui -costo_actualizado_bi
	gen		cost_act = round(costo_actualizado_bi, 1)
	format	cost_act %12.2g
	
	bys 	cod_local: egen _aux2 = max(cost_act)
	format 	cost_act _aux2 %12.2g
	drop 	if cost_act != _aux2
	
	duplicates report cod_local cod_mod // no hay duplicados
//	
// 	duplicates tag cod_local cod_mod, gen(_aux3)
// 	tab		_aux3
	
	replace uei = uep if uei == ""
	
	keep 	cod_local cod_mod uei nivel etapa
	cap duplicates drop
	compress
	save	"$main\LEMod_IntCBr.dta", replace