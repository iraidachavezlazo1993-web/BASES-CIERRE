	/*__________________________________________________________________________
	|	                                                      					|
	|	MINEDU - Cálculo de Orden de Prioridad (adaptado para uso con FUIE)    	|
	|	Actualizado: 31/07/2025  			    								|
	|__________________________________________________________________________*/

	/*_____________________________________________________________________
	|                                                                      |
	|                               PRÓLOGO                                |
	|_____________________________________________________________________*/ 
	
	clear 	all
	
	global	Raw			= 	"C:\CalcBrPr2507\raw"				// Carpeta Raw
	global 	Input  		=   "C:\CalcBrPr2507\input" 			// Carpeta Input (Actual)
	global 	Final  		=   "C:\CalcBrPr2507"					// Carpeta Final
	cd 		"$Final"

	set 	more off 
	set 	varabbrev off
	set 	type double
	set 	seed 339487731
	set 	excelxlsxlargefile on
	
	/*_____________________________________________________________________
	|                                                                      |
	|            		CÁLCULO DE ORDEN DE PRIORIDAD                      |
	|_____________________________________________________________________*/

	use 	"$Input\LE_Brecha", clear											// Base Inicial
	
	merge 	1:1 cod_local using "$Input\LE_BaseAdic"							// Fusión con Base Adicional.
	drop 	if _merge == 1
	drop	_merge																// La Base de Datos final debe contener solo observaciones (locales) de la Base Adicional.
	replace	b_safil = . if brecha == .											// Esta variable está en las 2 bases (LE_Brecha y LE_BaseAdic).
	table 	finfo, stat(sum intcb) nformat(%5.0fc)				
	
	foreach v of varlist areadem areariesgo ratiodem ratioriesgo brecha b_1 b_2 b_3 b_4 b_5 b_safil {
			replace `v'= . if matricula == 0
			gen `v'_ini = `v' 
			replace `v' = 0 if intcb == 1										// intcb = Intervención de Cierre de Brecha. Si intcb = 1, algunas variables de Base Inicial = 0.
	}																			// Crear variables iniciales para variables provenientes de la Base Inicial.
	gen		finfo_ini = finfo
	replace	finfo = 1 if intcb == 1												 
	label 	define finfo 14 "Sin matrícula", add								// finfo: 1 Brecha Cerrada, 2 FUIE-CE 2024, 3 FUIE-CE 2023, 4 FUIE-CE 2022, 5 FUIE-CE 2021 6 SRI 2018-2021, 7 DRELM 2018, 8 PRONIED 2018, 9 DIPLAN 2016,
																				//		  10 UGEL 2017 - Etapa 2, 11 UGEL 2017 - Etapa 1, 12 CIE 2013, 13 Sin información, 14 Sin matrícula.
	replace	finfo = 13 if finfo == .
	replace finfo_ini = 13 if finfo_ini == .
	replace finfo = 14 if matricula == 0 & finfo_ini != 13
	label	values finfo_ini finfo 
	
	table 	finfo, stat(count cod_local brecha) stat(sum brecha) stat(mean brecha) nformat(%15.0fc)
	table 	finfo, stat(count cod_local brecha) stat(sum brecha) stat(mean brecha) nformat(%18.2fc)
	table 	finfo, stat(sum b_1 b_2 b_3 b_4 b_5 b_safil) nformat(%18.2fc)
	table 	finfo, stat(sum brecha) nformat(%18.2fc)
	table 	finfo, stat(sum b_1_ini b_2_ini b_3_ini b_4_ini b_5_ini b_safil_ini) nformat(%18.2fc)
	table 	finfo, stat(sum brecha_ini) nformat(%18.2fc)	
	
	gen		mat_riesgo = ratioriesgo * matricula								// Calcular matrícula (alumnos) en riesgo.
	
	gen		ac_calc_rank = ac2_r1_ind == 0 | (ac2_r1_ind == 1 & (finfo == 7 | finfo == 8 | finfo == 9 | finfo == 12 | ac2_r1_sri_ok == 1 |  		///
			ac2_r1_f21_ok == 1 | ac2_r1_f22_ok == 1 | ac2_r1_f23_ok == 1 | ac2_r1_f24_ok == 1 | ///
			ac2_f21_ok == 1 | ac2_f22_ok == 1 | ac2_f23_ok == 1 | ac2_f24_ok == 1)) if ratiodem != . & intcb == 0				// Crear variable de cálculo de orden de prioridad según análisis de consistencia.
	gen 	calc_rank = ratiodem != . & intcb == 0	& matricula != 0 & ac_calc_rank == 1										// Crear variable de cálculo de orden de prioridad (calc_rank).
	tab		finfo calc_rank
	
	gen 	eficiencia = matricula / (brecha/1000000) if calc_rank == 1			// Crear variable eficiencia.
	replace eficiencia = 0	if eficiencia == . & calc_rank == 1					// Para los casos en los que brecha = 0.
	pctile 	eficiencia_u = eficiencia if calc_rank == 1 & brecha != 0, nq(5)	// Verificar umbrales de eficiencia. Actual: 7, 11, 20, 43.
	tab		eficiencia_u
	
	gen 	equidad = 1 if calc_rank == 1 & (vraem == 1 | huallaga == 1 | frontera == 1 | frontera_ac == 1) 
	replace equidad = 2 if equidad == . & calc_rank == 1 & pobreza > 50 & pobreza != .
	replace equidad = 3 if equidad == . & calc_rank == 1 & rural == 1
	replace equidad = 4 if equidad == . & calc_rank == 1						// Crear variable equidad.
	
	gen 	prioridad = 1 if calc_rank == 1 & ratiodem >= 0.7
	replace prioridad = 2 if prioridad == . & calc_rank == 1 & (equidad == 1 | (eficiencia >= 43 & eficiencia != .))
	replace prioridad = 3 if prioridad == . & calc_rank == 1 & (equidad == 2 | (eficiencia >= 20 & eficiencia < 43))
	replace prioridad = 4 if prioridad == . & calc_rank == 1 & (equidad == 3 | (eficiencia >= 11 & eficiencia < 20))
	replace prioridad = 5 if prioridad == . & calc_rank == 1 & (equidad == 4 | (eficiencia < 11))												
	replace prioridad = 6 if prioridad == . & calc_rank == 0 & intcb == 1
	replace prioridad = 7 if prioridad == .																									// Crear variable de grupos de prioridad. ACTUALIZAR QUINTILES.
	
	gen		prioridad2 = prioridad if prioridad != 7
	replace prioridad2 = 7 if prioridad2 == . & calc_rank == 0 & ratiodem != . & intcb == 0 & matricula != 0 & ac_calc_rank == 0
	replace prioridad2 = 8 if prioridad2 == . & calc_rank == 0 & finfo == 13
	replace prioridad2 = 9 if prioridad2 == . & calc_rank == 0 & finfo == 14																	// Crear variable de grupos de prioridad con detalle de categorías sin grupo de prioridad.
	
	gen     orden = mat_riesgo if calc_rank == 1
	replace orden = eficiencia if prioridad == 1 & calc_rank == 1				// Crear variable líder de ordenamiento.
	
	gsort 	prioridad -orden -eficiencia -matricula equidad -matri2024 -talumno2024 -matri2022 -talumno2022 -alumtot			// Ordenar observaciones (locales).
	gen 	rank_nac = _n if calc_rank == 1										// Crear variable orden de prioridad. Repetir luego para órdenes específicos.
		
	gsort 	dre prioridad -orden -eficiencia -matricula equidad -matri2024 -talumno2024 -matri2023 -talumno2023 -matri2022 -talumno2022  -alumtot
	bys 	dre: gen rank_dre = _n if calc_rank == 1
	
	gsort 	dre ugel prioridad -orden -eficiencia -matricula equidad -matri2024 -talumno2024 -matri2023 -talumno2023 -matri2022 -talumno2022 -alumtot
	bys 	dre ugel: gen rank_ugel = _n if calc_rank == 1
		
	gsort 	region prioridad -orden -eficiencia -matricula equidad -matri2024 -talumno2024 -matri2023 -talumno2023 -matri2022 -talumno2022 -alumtot
	bys 	region: gen rank_reg = _n if calc_rank == 1
		
	gsort 	region prov prioridad -orden -eficiencia -matricula equidad -matri2024 -talumno2024 -matri2023 -talumno2023 -matri2022 -talumno2022 -alumtot
	bys 	region prov: gen rank_prov = _n if calc_rank == 1
	
	gsort 	region prov dist prioridad -orden -eficiencia -matricula equidad -matri2024 -talumno2024 -matri2023 -talumno2023 -matri2022 -talumno2022 -alumtot
	bys 	region prov dist: gen rank_dist = _n if calc_rank == 1
	
	label 	define equidad 1 "VRAEM/Huallaga/Frontera" 2 "Pobreza > 50%" 3 "Rural" 4 "Urbano" 
	label 	values equidad equidad
	label 	define prioridad 1 "1: Riesgo" 2 "2: Muy Eficiente o VRAEM/Huallaga/Frontera" 3 "3: Eficiente o Pobre" 	4 "4: Poco Eficiente o Rural" 		///
							 5 "5: No Eficiente o Urbano" 6 "Brecha cerrada" 7 "Sin grupo de prioridad"
	label	values prioridad prioridad											// Crear etiquetas para variables equidad y prioridad.
	label 	define prioridad2 1 "1: Riesgo" 2 "2: Muy Eficiente o VRAEM/Huallaga/Frontera" 3 "3: Eficiente o Pobre" 4 "4: Poco Eficiente o Rural" 		///
							  5 "5: No Eficiente o Urbano" 6 "Brecha cerrada" 7 "Información por revisar" 8 "Sin información" 9 "Sin matrícula"
	label	values prioridad2 prioridad2
	
	tab 	prioridad finfo if prioridad == 6
	tab 	prioridad finfo if prioridad == 7
	tab 	prioridad2 finfo if prioridad2 == 6
	tab 	prioridad2 finfo if prioridad2 == 7
	tab 	prioridad2 finfo if prioridad2 == 8
	tab 	prioridad2 finfo if prioridad2 == 9									// Revisar si variables prioridad y finfo son consistentes.
	
	table 	prioridad if prioridad <= 5, stat(count cod_local brecha) stat(sum brecha) stat(mean brecha) nformat(%15.0fc)
	table 	prioridad, stat(count cod_local brecha) stat(sum brecha) stat(mean brecha) nformat(%15.0fc)
	table 	prioridad2, stat(count cod_local brecha) stat(sum brecha) stat(mean brecha) nformat(%15.0fc)
	
	rename 	areatech areatech2
	merge 	1:1 cod_local using "$Input\cie\LE_Brecha_CIE", keepusing(areatech CIE)	keep(1 3) nogen				// Obtener área techada CIE.
	rename 	areatech areatech_cie
	replace areatech_cie = . if CIE != 1
	merge 	1:1 cod_local using "$Input\sri\LE_Brecha_SRI", keepusing(areatech SRI) keep(1 3) nogen				// Obtener área techada SRI.
	rename 	areatech areatech_sri
	replace areatech_sri = . if SRI != 1
	merge 	1:1 cod_local using "$Input\fuie21\LE_Brecha_FUIE21", keepusing(areatech) keep(1 3) nogen			// Obtener área techada FUIE21.
	rename 	areatech areatech_f21
	merge 	1:1 cod_local using "$Input\fuie22\LE_Brecha_FUIE22", keepusing(areatech) keep(1 3) nogen			// Obtener área techada FUIE22.
	rename 	areatech areatech_f22
	merge 	1:1 cod_local using "$Input\fuie23\LE_Brecha_FUIE23", keepusing(areatech) keep(1 3) nogen			// Obtener área techada FUIE23.
	rename 	areatech areatech_f23
	merge 	1:1 cod_local using "$Input\fuie24\LE_Brecha_FUIE24", keepusing(areatech) keep(1 3) nogen			// Obtener área techada FUIE24.
	rename 	areatech areatech_f24
	rename 	areatech2 areatech

	gen		ac2_r1_cie = areatech_cie / matricula if (matricula != 0 | matricula != .)
	gen		ac2_r1_sri = areatech_sri / matricula if (matricula != 0 | matricula != .)
	gen		ac2_r1_f21 = areatech_f21 / matricula if (matricula != 0 | matricula != .)
	gen		ac2_r1_f22 = areatech_f22 / matricula if (matricula != 0 | matricula != .)
	gen		ac2_r1_f23 = areatech_f23 / matricula if (matricula != 0 | matricula != .)
	gen		ac2_r1_f24 = areatech_f24 / matricula if (matricula != 0 | matricula != .)
	
	tostring cod_local, gen(codlocal) format(%06.0f)
	merge 	1:1 codlocal using "$Raw\fuie24\local_lineal_01", keepusing(otro_lug) keep(1 3) nogen
	rename 	otro_lug uso_local
	
	* Criterios adicionales de análisis de consistencia de datos (actualizar a última FUIE disponible para FUIE 2023, 2024, etc.)
	*------------------------------------------------------------------
	list 	cod_local finfo ac2_r1 areatech prioridad2 if prioridad2 <= 5 & (ac2_r1_ind == 1 & ac2_f21_ok != 1 & ac2_f22_ok != 1 & ac2_f23_ok != 1 & ac2_f24_ok != 1) & finfo != 12 & areatech < 100	 	///
																			// Agregar códigos a listas NoSRI, NoFUIE21, NoFUIE22, NoFUIE23 y ejecutar código.	
	list 	cod_local finfo ac2_r1 ac2_r1_cie ac2_r1_f22 if prioridad == 1 & (ac2_r1_ind == 1 & ac2_f22_ok != 1) & finfo != 12 & ac2_r2_f22_drop == 1 &	///
			(ac2_r1_f22 >= 1.5 & ac2_r1_f22 != .) & ac2_r1_cie < 1.5		// Comparar ratio R1 FUIE22 vs CIE. Si FUIE22 cumple con criterio R1 y CIE no cumple, incluir código de local a SiFUIE22 si no está en NoFUIE22 y ejecutar código.
	list 	cod_local finfo ac2_r1 ac2_r1_cie ac2_r1_f23 if prioridad == 1 & (ac2_r1_ind == 1 & ac2_f23_ok != 1) & finfo != 12 & ac2_r2_f23_drop == 1 &	///
			(ac2_r1_f23 >= 1.5 & ac2_r1_f23 != .) & ac2_r1_cie < 1.5		// Comparar ratio R1 FUIE23 vs CIE. Si FUIE23 cumple con criterio R1 y CIE no cumple, incluir código de local a SiFUIE23 si no está en NoFUIE23 y ejecutar código.
	list 	cod_local finfo ac2_r1 ac2_r1_cie ac2_r1_f24 if prioridad == 1 & (ac2_r1_ind == 1 & ac2_f24_ok != 1) & finfo != 12 & ac2_r2_f24_drop == 1 &	///
			(ac2_r1_f24 >= 1.5 & ac2_r1_f24 != .) & ac2_r1_cie < 1.5		// Comparar ratio R1 FUIE24 vs CIE. Si FUIE24 cumple con criterio R1 y CIE no cumple, incluir código de local a SiFUIE24 si no está en NoFUIE24 y ejecutar código.
	
	list	cod_local finfo ac2_r1 areatech if prioridad == 1 & (ac2_r1_ind == 1 & ac2_f21_ok == 1) & finfo != 12 & areatech < 100		// Verificar las áreas techadas de estos locales con la validación de articulación con regiones.
	list	cod_local finfo ac2_r1 areatech if prioridad == 1 & (ac2_r1_ind == 1 & ac2_f22_ok == 1) & finfo != 12 & areatech < 100		// Verificar las áreas techadas de estos locales con la validación de articulación con regiones.
	list	cod_local finfo ac2_r1 areatech if prioridad == 1 & (ac2_r1_ind == 1 & ac2_f23_ok == 1) & finfo != 12 & areatech < 100		// Verificar las áreas techadas de estos locales con la validación de articulación con regiones.
	list	cod_local finfo ac2_r1 areatech if prioridad == 1 & (ac2_r1_ind == 1 & ac2_f24_ok == 1) & finfo != 12 & areatech < 100		// Verificar las áreas techadas de estos locales con la validación de articulación con regiones.

	* Cálculo de indicador de análisis de consistencia
	*------------------------------------------------------------------
	egen	ac1_cd = rowtotal(ac1_areatech_le ac1_cod_edif ac1_p105 ac1_p106 ac1_p106_3 ac1_p106_4 ac1_p108_1 ac1_p109 ac1_p201_1 ac1_p201_2 ac1_p201_3 ac1_p202 						///
					 ac1_p207 ac1_p208 ac1_p212 ac1_p216 ac1_p401_4 ac1_p401_4a ac1_p401_5_1 ac1_p401_5_2 ac1_p401_6 ac1_p401_6a ac1_p401_7 ac1_p401_8_1 ac1_p401_8_2 		///
					 ac1_p401_10 ac1_p401_10a ac1_p401_13_1 ac1_p401_13_2 ac1_p401_13_3 ac1_p401_14_21 ac1_p401_14_22 ac1_p401_14_41 ac1_p401_14_42 ac1_p401_14_43 			///
					 ac1_p401_17_1 ac1_p401_17_21 ac1_p401_17_22 ac1_p401_17_31 ac1_p401_17_32	ac1_p401_17_41 ac1_p401_17_42 ac1_p401_17_43 ac1_p401_18_1 ac1_p401_18_2) 	///
					 if finfo_ini == 2
	
	merge	1:1 cod_local using "$Input\LE_Descarte_FUIE24", keepusing(cod_local) keep(1 3) 
	rename _merge _m_no_f24
	
	merge	1:1 cod_local using "$Input\LE_AC_FUIE24", keepusing(cod_local) keep(1 3) 
	rename _merge _m_ac_f24
	
	count 	if 	(finfo_ini == 2 & ac1_cd == 0 & (ac2_r1_ind == 0 | (ac2_r1_ind == . | matricula == 0) | ///
				(ac2_r1_ind == 1 & (ac2_r1_sri_ok == 1 | ac2_r1_f21_ok == 1 | ac2_r1_f22_ok == 1 | ac2_r1_f23_ok == 1  | ac2_r1_f24_ok == 1 | ///
				ac2_f21_ok == 1 | ac2_f22_ok == 1 | ac2_f23_ok == 1 | ac2_f24_ok == 1)))) | ///
				(finfo_ini != 2 & uso_local == "1" & _m_no_f24 != 3 & _m_ac_f24 != 3)
	
	gen		ac_infadec = (finfo_ini == 2 & ac1_cd == 0 & (ac2_r1_ind == 0 | (ac2_r1_ind == . | matricula == 0) | ///
						 (ac2_r1_ind == 1 & (ac2_r1_sri_ok == 1 | ac2_r1_f21_ok == 1 | ac2_r1_f22_ok == 1 | ac2_r1_f23_ok == 1 | ac2_r1_f24_ok == 1 | ///
						 ac2_f21_ok == 1 | ac2_f22_ok == 1 | ac2_f23_ok == 1 | ac2_f24_ok == 1)))) | ///
						 (finfo_ini != 2 & uso_local == "1" & _m_no_f24 != 3 & _m_ac_f24 != 3)
	
	* Cálculo de indicadores adicionales y preparación final de base
	*------------------------------------------------------------------
	egen 	cp_ie = rowtotal(cp_n cp_dr)
	gen 	int_cp = cp_ie > 0 if calc_rank == 1
	replace int_sshh = 0 if int_sshh == . & calc_rank == 1
	gen 	at_artreg = rank_reg <= 50 & (ac2_r1_ind == 1 & ac2_f21_ok != 1 & ac2_f22_ok != 1 & ac2_f23_ok != 1 & ac2_f24_ok != 1)		// Locales críticos para llenado, revisión y validación/confirmación de la FUIE (asistencia técnica).
	
	foreach v of varlist int_st int_sp int_ri int_rc int_ic int_cp int_sshh {
			replace `v' = 0 if finfo == 1
			replace `v' = . if prioridad == 7	
	}
	
	* Área de terreno
	*------------------
	
	rename	cod_local id_local
	merge 	1:1 id_local using "$Input\cie\LE_Aterr_CIE", keepusing(areaterr) keep(1 3) nogen				// Obtener área terreno CIE
	rename 	areaterr areaterr_cie
	*replace areaterr_cie = . if CIE == 0
	rename	id_local cod_local
	merge 	1:1 cod_local using "$Input\sri\LE_Aterr_SRI", keepusing(areaterr) keep(1 3) nogen					// Obtener área terreno SRI
	rename 	areaterr areaterr_sri
	merge 	1:1 cod_local using "$Input\fuie21\LE_Aterr_FUIE21", keepusing(areaterr) keep(1 3) nogen			// Obtener área terreno FUIE 2021
	rename 	areaterr areaterr_f21
	merge 	1:1 cod_local using "$Input\fuie22\LE_Aterr_FUIE22", keepusing(areaterr) keep(1 3) nogen			// Obtener área terreno FUIE 2022
	rename 	areaterr areaterr_f22
	merge 	1:1 cod_local using "$Input\fuie23\LE_Aterr_FUIE23", keepusing(areaterr) keep(1 3) nogen			// Obtener área terreno FUIE 2023
	rename 	areaterr areaterr_f23
	merge 	1:1 cod_local using "$Input\fuie24\LE_Aterr_FUIE24", keepusing(areaterr) keep(1 3) nogen			// Obtener área terreno FUIE 2023
	rename 	areaterr areaterr_f24
	
	gen		areaterr = areaterr_f24 if finfo_ini == 2
	replace	areaterr = areaterr_f23 if finfo_ini == 3
	replace	areaterr = areaterr_f22 if finfo_ini == 4
	replace	areaterr = areaterr_f21 if finfo_ini == 5
	replace	areaterr = areaterr_sri if finfo_ini == 6 | finfo_ini == 10
	replace	areaterr = areaterr_cie if finfo_ini == 7 | finfo_ini == 8 | finfo_ini == 9 | finfo_ini == 11 | finfo_ini == 12
	replace areaterr = areatechp1 if areaterr == .
	replace areaterr = 1000000 if areaterr > 1000000 & areaterr != .
	
	drop	orden
	rename 	(uei nivel) (intcb_ue intcb_niv)
	order	cod_local region prov dist codgeo dre ugel nom_local zona urbano rural  		///
			rural_grad vraem huallaga frontera frontera_ac pobreza activo 					///
			Inicial Primaria Secundaria EBA EBE ESFA IST ISP CETPRO							///
			matricula matri* talumno* 														///
			alumtot m_* disafil zonaamenaza aux												///
			areadem_ini areadem areariesgo_ini areariesgo areatech areaterr					///
			ratiodem_ini ratiodem ratioriesgo_ini ratioriesgo mat_riesgo					///
			int_st int_sp int_ri int_rc int_ic    											///
			brecha_ini brecha eficiencia equidad prioridad prioridad2						///
			rank_nac rank_reg rank_prov rank_dist rank_dre rank_ugel						///
			loc_nac loc_reg loc_prov loc_dist loc_dre loc_ugel   							///
			calc_rank finfo finfo_ini intcb intcb_ue intcb_niv  							///
			f22_corte f22_corte_alt ac_infadec at_artreg int_cp int_sshh alb pred_ten*
		
	compress
	save 	"LE_BasePr.dta", replace		// Guardar Base de Datos final.
	
	/*_____________________________________________________________________
	|                                                                      |
	|            	PREPARACIÓN FINAL DE BASE DE DATOS                     |
	|_____________________________________________________________________*/
	
	use		"LE_BasePr", clear
	
	gsort 	region prov dist cod_local
	egen	m_EBR = rowtotal(m_Inicial m_Primaria m_Secundaria)
	egen 	m_EB = rowtotal(m_EBR m_EBA m_EBE)
	
	keep	cod_local region prov dist cen_pob nlat_ie nlong_ie codgeo dre ugel nom_local 	///		FALTA: Arreglar para optimizar creación de base "verde".
			zona rural_grad escenario vraem huallaga frontera frontera_ac pobreza 		 	///
			Inicial Primaria Secundaria EBA EBE ESFA IST ISP CETPRO							///
			matricula matri* talumno* 														///
			alumtot m_* disafil zonaamenaza													///
			areadem_ini areadem areariesgo_ini areariesgo areatech areaterr					///
			ratiodem_ini ratiodem ratioriesgo_ini ratioriesgo mat_riesgo					///
			int_st int_sp int_ri int_rc int_ic    											///
			brecha_ini brecha eficiencia equidad prioridad prioridad2						///
			rank_nac rank_reg rank_prov rank_dist rank_dre rank_ugel						///
			loc_nac loc_reg loc_prov loc_dist loc_dre loc_ugel   							///
			calc_rank finfo finfo_ini intcb intcb_ue intcb_niv  							///
			f22_corte f22_corte_alt ac_infadec at_artreg int_cp int_sshh alb pred_ten*
	
	order	cod_local region prov dist cen_pob nlat_ie nlong_ie codgeo dre ugel nom_local 			///
			zona rural_grad escenario vraem huallaga frontera frontera_ac pobreza  					///
			Inicial Primaria Secundaria EBA EBE ESFA IST ISP CETPRO	matricula 						///
			m_Inicial m_Primaria m_Secundaria m_EBR m_EBA m_EBE m_EB m_ESFA m_IST m_ISP m_CETPRO	///
			disafil zonaamenaza areadem areatech brecha	eficiencia equidad prioridad				///
			prioridad2 calc_rank rank_nac rank_reg rank_prov rank_dist loc_nac loc_reg 				///
			loc_prov loc_dist finfo finfo_ini intcb intcb_ue intcb_niv 								///
			int_cp int_sshh int_st int_sp int_ri int_rc int_ic pred_ten* alb  						///
			brecha_ini brecha eficiencia equidad prioridad prioridad2						

	compress
	save 	"LE_BasePrR.dta", replace
	
	export 	excel using "Calc_Prioridad.xlsx", firstrow(var) sheetmodify					
	
	/*_____________________________________________________________________
	|                                                                      |
	|         PREPARACIÓN DE BASE DE DATOS DE INTERVENCIONES               |
	|_____________________________________________________________________*/
	
	use		"LE_BasePr", clear

* [3.1] Grupo 1: Reduccion de vulnerabilidad
	gen 	interv_1 = ct_st_d > 0 & ct_st_d != .
	gen		interv_2 = ct_sp_d > 0 & ct_sp_d != .
	gen		interv_3 = ct_ri > 0 & ct_ri != .
	gen		interv_4 = ct_ic > 0 & ct_ic != .
	gen		interv_5 = ct_cp > 0 & ct_cp != .

* [3.2] Grupo 2: Mejoramiento de acceso y calidad de SSBB
	gen		interv_6 = ct_aad > 0 & ct_aad != .
	gen		interv_7 = ct_cad > 0 & ct_cad != .
	
* [3.3] Grupo 3: Mantenimiento y/o acondicionamiento
	gen		interv_8 = 	(ct_st_me > 0 & ct_st_me != .) | (ct_sp_me > 0 	& ct_sp_me != .) | (ct_ri_me > 0 & ct_ri_me != .) | 	///
						(ct_rc_me > 0 & ct_rc_me != .) | (ct_ic_me > 0 	& ct_ic_me != .) | (ct_amp_me > 0 	& ct_amp_me != .)
	
	gen		interv_9 = ct_ce > 0 & ct_ce != .
	gen		interv_10 = ct_ene > 0 & ct_ene != .

* [3.4] Grupo 4: Mejoramiento, rehabilitacion y amplicacion del LE
	gen 	interv_11 = ct_ae > 0 & ct_ae != .
	gen		interv_12 = ct_acc > 0 & ct_acc != .
	gen		interv_13 = ct_amp > 0 & ct_amp != .
	gen 	interv_14 = ct_rc > 0 & ct_rc != .
	gen		interv_15 = (ct_sp_r > 0 & ct_sp_r != .) | (ct_ic_r > 0 & ct_ic_r != .)
	
* [3.5] Grupo 5: Sustitutir el LE
	gen		interv_16 = ct_st_ra > 0 & ct_st_ra != . 

* [3.6] Grupo 6: Saneamiento físico legal
	gen		interv_17 = b_safil > 0 & b_safil != .
		
	* Etiquetas 
	label	define dico 0 "NO" 1 "SÍ"
	forvalues i = 1/17 {
		foreach var of varlist interv_`i' {
			lab values `var' dico
		}
	}

	* corregir infor para LE sin grupo de prioridad
	forvalues i = 1/17 {
		foreach var of varlist interv_`i' {
			replace interv_`i' = . if prioridad == 7
		}
	}
	
	* corregir info para LE con brecha cerrada
	forvalues i = 1/17 {
		foreach var of varlist interv_`i' {
			replace interv_`i' = 0 if prioridad == 6
		}
	}	
	* Mantener las variables de interes
	keep	cod_local interv_*
	
	* Guardar
	duplicates drop
	export	excel using "LE_Interv.xlsx", sheet("LE_Int", replace) firstrow(var)