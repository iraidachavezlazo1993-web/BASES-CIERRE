	/*__________________________________________________________________________
	|	                                                      					|
	|	MINEDU - Cálculo de Indicadores de Seguimiento del PNIE			    	|
	|	Actualizado: 03/09/2023				    								|
	|__________________________________________________________________________*/

	/*_____________________________________________________________________
	|                                                                      |
	|                               PRÓLOGO                                |
	|_____________________________________________________________________*/ 
	
	clear 	all
	
	global 	Input  		=   "C:\CalcSPNIE2302\input"			// Carpeta Input
	global 	Final  		=   "C:\CalcSPNIE2302\"					// Carpeta Final
	cd 		"$Final"

	set 	more off 
	set 	varabbrev off
	set 	type double
	set 	seed 339487731
	set 	excelxlsxlargefile on
	
	/*_____________________________________________________________________
	|                                                                      |
	|            		CÁLCULO DE INDICADORES 2022		                   |
	|_____________________________________________________________________*/
	
	use 	"$Input\2022\LE_BasePr_2212", clear						
	
	* Indicadores de estrategia 1.1
	*-------------------------------
	gen 	ind_111 = int_st == 1 if brecha != .
	sum		ind_111
	display r(mean)
	
	gen 	ind_112 = int_sp == 1 if brecha != .
	sum		ind_112
	display r(mean)
	
	gen 	ind_113 = int_rc == 1 | int_ri == 1 if brecha != .
	sum		ind_113
	display r(mean)
	
	gen 	ind_114 = int_ic == 1 if brecha != .
	sum		ind_114
	display r(mean)

	gen 	ind_115 = int_st == 1 | int_sp == 1 | int_rc == 1 | int_ri == 1 | int_ic == 1 if brecha != .
	sum		ind_115
	display r(mean)
	
	gen 	ind_116 = ((cp_n > 0 & cp_n != .) | (cp_dr > 0 & cp_dr != .) | (cp_mm > 0 & cp_mm != .) | (cp_mb > 0 & cp_mb != .)) & intcb == 0 & zona == 2 if brecha != . & ((tramo > 0 & tramo != .) | intcb == 1 | zona == 1)
	sum		ind_116
	display r(mean)
	
	* Indicadores de estrategia 1.2
	*-------------------------------
	merge 	1:1 cod_local using "$Input\2022\LE_AccSSBB_2022", keepusing(acc_agua* acc_des* acc_ene*) keep(1 3) nogen
	
	gen 	ind_121 = (acc_agua != "1. Red pública (agua potable)" & acc_agua != "3. Camión cisterna u otro similar" & acc_agua != "4. Pozo") ///
					   if acc_agua != "" & acc_agua != "8. Sin información" & finfo != 12 & matricula != 0		
	sum		ind_121
	display r(mean)
					   
	gen 	ind_122 = (acc_des != "01. Red pública" & acc_des != "03. Tanque séptico" & acc_des != "04. Pozo percolador" & acc_des !=  "06. Biodigestor") ///
					   if acc_des != "" & acc_des != "10. Sin información" & finfo != 12 & matricula != 0		
	sum		ind_122
	display r(mean)
	
	gen 	ind_123 = ((cis_imp > 0 & cis_imp != .) | (te_imp > 0 & te_imp != .) | (te_mb > 0 & te_mb != .) | (te_mm > 0 & te_mm != .) | (te_sus > 0 & te_sus != .) | 	///
					   (ba_inoamp > 0 & ba_inoamp != .) | (ba_inoreh > 0 & ba_inoreh != .) | (ba_uriamp > 0 & ba_uriamp != .) | (ba_urireh > 0 & ba_urireh != .) | 		///
					   (b_beb > 0 & b_beb != .) | (ca_reh > 0 & ca_reh != .) | (ca_sus > 0 & ca_sus != .) | (ca_imp > 0 & ca_imp != .) | (bp_reh > 0 & bp_reh != .) | 	///
					   (bp_sus > 0 & bp_sus != .) | (bp_imp > 0 & bp_imp != .)) & intcb == 0 if brecha != . & ((ca_reh !=. & ba_inoamp != . & b_beb != . & te_imp != .) | intcb == 1) 
	sum		ind_123
	display r(mean)
	
	gen 	ind_124 = acc_ene != "1. Red pública (de una empresa distribuidora de energía eléctrica)" if acc_ene != "" & acc_ene != "9. Sin información" & finfo != 12 & matricula != 0		
	sum		ind_124
	display r(mean)
	
	gen 	ind_125 = ((area_ce1 > 0 & area_ce1 != .) | (area_ce2 > 0 & area_ce2 != .) | (area_ce3 > 0 & area_ce3 != .) | (area_ce4 > 0 & area_ce4 != .) | (area_ce5 > 0 & area_ce5 != .)) ///
						& intcb == 0 if brecha != . & finfo != 9 & (int_st == 0 | intcb == 1) 						// Sacar condición int_st == 0 si area_ce se define tomando en cuenta LL.EE. con sustitución total.
	sum		ind_125
	display r(mean)
	
	gen 	ind_126 = ((acc_ir > 0 & acc_ir != .) | (acc_rr1 > 0 & acc_rr1 != .) | (acc_rr2_5 > 0 & acc_rr2_5 != .) | (acc_ar > 0 & acc_ar != .)) & intcb == 0 		///
						if brecha != . & ((alumtot > 0 & alumtot != .) | intcb == 1 | finfo_ini == 4) 				// Condición finfo_ini == 4 es temporal hasta arreglar alumtot que viene de SRI.
	sum		ind_126
	display r(mean)
	
	* Indicadores de estrategia 1.3
	*-------------------------------
	drop	alum1* alum2* alum3* alum4* alum5* alum6* alum7* alum8* alum9*
	rename	cod_local id_local
	merge 	1:1 id_local using "$Input\2022\LE_Amp_CIE_2212", keepusing(areaamp* alum*) keep(1 3) nogen
	forvalues i = 1/9 {
		gen aux_areaamp`i' = areaamp`i' 					if finfo_ini == 5 | finfo_ini == 6  | finfo_ini == 7 | finfo_ini == 10 
		gen aux_alum`i' = alum`i' 							if finfo_ini == 5 | finfo_ini == 6  | finfo_ini == 7 | finfo_ini == 10 
		if 	(`i' != 3) gen aux_alum`i'tot = alum`i'tot 		if finfo_ini == 5 | finfo_ini == 6  | finfo_ini == 7 | finfo_ini == 10 
		if 	(`i' == 3) gen aux_alum`i'tot = alum`i' 		if finfo_ini == 5 | finfo_ini == 6  | finfo_ini == 7 | finfo_ini == 10 
		drop areaamp`i' alum`i' 
		cap drop alum`i'tot 
		cap drop alum`i'max
	}
	drop alum alummax areaamp1_dz areaamp2_dz areaamp3_dz

	rename	id_local cod_local 
	merge 	1:1 cod_local using "$Input\2022\LE_Amp_SRI_2212", keepusing(areaamp* alum*) keep(1 3) nogen
	forvalues i = 1/9 {
		replace aux_areaamp`i' = areaamp`i' 				if finfo_ini == 4 | finfo_ini == 8 
		replace aux_alum`i' = alum`i' 						if finfo_ini == 4 | finfo_ini == 8 
		if 	(`i' != 3) replace aux_alum`i'tot = alum`i'tot 	if finfo_ini == 4 | finfo_ini == 8 
		if 	(`i' == 3) replace aux_alum`i'tot = alum`i' 	if finfo_ini == 4 | finfo_ini == 8 
		drop areaamp`i' alum`i' 
		cap drop alum`i'tot
		cap drop alum`i'max
	}
	drop alum alummax areaamp1_dz areaamp2_dz areaamp3_dz
	
	merge 	1:1 cod_local using "$Input\2022\LE_Amp_FUIE21_2212", keepusing(areaamp* alum*) keep(1 3) nogen
	forvalues i = 1/9 {
		replace aux_areaamp`i' = areaamp`i' 				if finfo_ini == 3
		replace aux_alum`i' = alum`i' 						if finfo_ini == 3
		replace aux_alum`i'tot = alum`i'tot 				if finfo_ini == 3
		drop areaamp`i' alum`i' alum`i'tot alum`i'max
	}
	drop alum alummax areaamp1_dz areaamp2_dz areaamp3_dz
	
	merge 	1:1 cod_local using "$Input\2022\LE_Amp_FUIE22_2212", keepusing(areaamp* alum*) keep(1 3) nogen
	forvalues i = 1/9 {
		replace aux_areaamp`i' = areaamp`i' 				if finfo_ini == 2
		replace aux_alum`i' = alum`i' 						if finfo_ini == 2
		replace aux_alum`i'tot = alum`i'tot 				if finfo_ini == 2
		drop areaamp`i' alum`i' alum`i'tot alum`i'max
		rename (aux_areaamp`i' aux_alum`i' aux_alum`i'tot) (areaamp`i' alum`i' alum`i'tot)
	}
	drop alum alummax areaamp1_dz areaamp2_dz areaamp3_dz
	
	gen 	ind_131 = (areaamp1 > 0 & areaamp1 != .) & intcb == 0 if brecha != . & ((alum1 > 0 & alum1 != .) | (intcb == 1 & m_Inicial > 0 & m_Inicial != .)) 					
	sum		ind_131
	display r(mean)
	
	gen 	ind_132 = (areaamp2 > 0 & areaamp2 != .) & intcb == 0 if brecha != . & ((alum2 > 0 & alum2 != .) | (intcb == 1 & m_Primaria > 0 & m_Primaria != .)) 						
	sum		ind_132
	display r(mean)
	
	gen 	ind_133 = (areaamp3 > 0 & areaamp3 != .) & intcb == 0 if brecha != . & ((alum3 > 0 & alum3 != .) | (intcb == 1 & m_Secundaria > 0 & m_Secundaria != .)) 					
	sum		ind_133
	display r(mean)
	
	gen 	ind_135 = (areaamp5 > 0 & areaamp5 != .) & intcb == 0 if brecha != . & ((alum5 > 0 & alum5 != .) | (intcb == 1 & m_EBE > 0 & m_EBE != .)) 					
	sum		ind_135
	display r(mean)	
	
	gen 	ind_136 = (areaamp4 > 0 & areaamp4 != .) & intcb == 0 if brecha != . & ((alum4 > 0 & alum4 != .) | (intcb == 1 & m_EBA > 0 & m_EBA != .)) 					
	sum		ind_136
	display r(mean)	

	gen 	ind_137 = (areaamp9 > 0 & areaamp9 != .) & intcb == 0 if brecha != . & ((alum9 > 0 & alum9 != .) | (intcb == 1 & m_CETPRO > 0 & m_CETPRO != .)) 					
	sum		ind_137
	display r(mean)	
	
	gen 	ind_138 = ((areaamp6 > 0 & areaamp6 != .) | (areaamp7 > 0 & areaamp7 != .) | (areaamp8 > 0 & areaamp8 != .)) & intcb == 0 if brecha != . & ///
					  ((alum6 > 0 & alum6 != .) | (alum7 > 0 & alum7 != .) | (alum8 > 0 & alum8 != .) | (intcb == 1 & ((m_ESFA > 0 & m_ESFA != .) | (m_IST > 0 & m_IST != .) | (m_ISP > 0 & m_ISP != .)))) 					
	sum		ind_138
	display r(mean)	
	
	* Indicadores de estrategia 1.4
	*-------------------------------
	gen 	ind_141 = disafil == 2 | (disafil != 1 & disafil != 2 & intcb == 0) if finfo != 12 & matricula != 0		
	sum		ind_141
	display r(mean)	
	
	* Indicadores de estrategia 1.5
	*-------------------------------
	egen	alum = rowtotal(alum1 alum2 alum3 alum4 alum5 alum6 alum7 alum8 alum9), missing
	gen 	ind_151 = (int_st == 1 | int_sp == 1 | int_rc == 1 | int_ri == 1 | int_ic == 1 | (areaamp > 0 & areaamp != .)) & intcb == 0 & finfo != 9 if brecha != . & ((alum > 0 & alum != .) | intcb == 1)
	sum		ind_151
	display r(mean)
	
	* Indicadores de estrategia 3.1
	*-------------------------------
	gen 	ind_311 = ac_infadec != 1 if finfo != 12 & matricula != 0	
	sum		ind_311
	display r(mean)
	
	* Indicadores de estrategia 4.1
	*-------------------------------
	gen 	ind_411 = ((area_ene1 > 0 & area_ene1 != .) | (area_ene2 > 0 & area_ene2 != .) | (area_ene3 > 0 & area_ene3 != .) | (area_ene4 > 0 & area_ene4 != .)) & intcb == 0 	///
						if brecha != . & finfo != 9 & ((area_ene1 != . | area_ene2 != . | area_ene3 != . | area_ene4 != .) | intcb == 1) 
	sum		ind_411
	display r(mean)
	
	* Indicadores de estrategia 4.2
	*-------------------------------
	merge 	1:1 cod_local using "$Input\LE_MantReg_22", keepusing(cod_local total_declarado_mp) keep(1 3)
	gen 	mant_reg = (total_declarado_mp > 0 & total_declarado_mp != .) if _merge == 3
	drop	_merge
	
	gen 	año_int = 2022
	merge 	1:1 cod_local año_int using "$Input\LE_Acond_17-22", keepusing(año_int interv) keep(1 3)
	gen		acond = (interv == "ACONDICIONAMIENTO CUARTO DE BOMBAS" | interv == "ACONDICIONAMIENTO DE AULAS" | interv == "ACONDICIONAMIENTO DE PREFABRICADOS" | ///
			interv == "ACONDICIONAMIENTO ESPACIOS ABIERTOS" | interv == "ACONDICIONAMIENTO INTEGRAL") if _merge == 3
	drop	_merge
	
	gen 	ind_412 = mant_reg != 1 & acond != 1 if finfo!= 12 & matricula != 0	
	sum		ind_412
	display r(mean)	
	
	compress
	save 	"LE_SPNIE_2022.dta", replace
	
	gen		EBR = Inicial != 0 | Primaria != 0 | Secundaria != 0
	egen 	m_EBR = rowtotal (m_Inicial m_Primaria m_Secundaria)
	gen 	EB = Inicial != 0 | Primaria != 0 | Secundaria != 0 | EBA != 0 | EBE!= 0 
	egen 	m_EB = rowtotal (m_Inicial m_Primaria m_Secundaria m_EBA m_EBE)
	keep 	cod_local region prov dist codgeo nom_local urbano rural Inicial Primaria Secundaria EBR EBA EBE EB ESFA IST ISP CETPRO matricula m_Inicial m_Primaria m_Secundaria m_EBR m_EBA m_EBE m_EB m_ESFA m_IST m_ISP m_CETPRO ind_*
	order 	cod_local region prov dist codgeo nom_local urbano rural Inicial Primaria Secundaria EBR EBA EBE EB ESFA IST ISP CETPRO matricula m_Inicial m_Primaria m_Secundaria m_EBR m_EBA m_EBE m_EB m_ESFA m_IST m_ISP m_CETPRO ind_*
	
	export 	excel using "LE_SPNIE_2022.xlsx", firstrow(var) sheetmodify 
	
	/*_____________________________________________________________________
	|                                                                      |
	|            		CÁLCULO DE INDICADORES 2021	                   	   |
	|_____________________________________________________________________*/
	
	use 	"$Input\2021\LE_BasePr_2112", clear	
	
	* Ajustes en información de matrícula
	*-------------------------------------
	replace	m_ESFA = matricula if cod_local == 288718
	replace	m_Inicial = matricula if cod_local == 600281
	
	* Indicadores de estrategia 1.1
	*-------------------------------
	gen 	ind_111 = int_st == 1 if brecha != .
	sum		ind_111
	display r(mean)
	
	gen 	ind_112 = int_sp == 1 if brecha != .
	sum		ind_112
	display r(mean)
	
	gen 	ind_113 = int_rc == 1 | int_ri == 1 if brecha != .
	sum		ind_113
	display r(mean)
	
	gen 	ind_114 = int_ic == 1 if brecha != .
	sum		ind_114
	display r(mean)

	gen 	ind_115 = int_st == 1 | int_sp == 1 | int_rc == 1 | int_ri == 1 | int_ic == 1 if brecha != .
	sum		ind_115
	display r(mean)
	
	gen 	ind_116 = ((cp_n > 0 & cp_n != .) | (cp_dr > 0 & cp_dr != .) | (cp_mm > 0 & cp_mm != .) | (cp_mb > 0 & cp_mb != .)) & intcb == 0 & zona == 2 if brecha != . & ((tramo > 0 & tramo != .) | intcb == 1 | zona == 1)
	sum		ind_116
	display r(mean)
	
	* Indicadores de estrategia 1.2
	*-------------------------------
	merge 	1:1 cod_local using "$Input\2021\LE_AccSSBB_2021", keepusing(acc_agua* acc_des* acc_ene*) keep(1 3) nogen
	
	gen 	ind_121 = (acc_agua != "1. Red pública (agua potable)" & acc_agua != "3. Camión cisterna u otro similar" & acc_agua != "4. Pozo") ///
					   if acc_agua != "" & acc_agua != "8. Sin información" & finfo != 10 & matricula != 0		
	sum		ind_121
	display r(mean)
					   
	gen 	ind_122 = (acc_des != "1. Red pública" & acc_des != "3. Tanque séptico" & acc_des != "4. Pozo percolador" & acc_des !=  "6. Biodigestor") ///
					   if acc_des != "" & acc_des != "9. Sin información" & finfo != 10 & matricula != 0		
	sum		ind_122
	display r(mean)
	
	gen 	ind_123 = ((cis_imp > 0 & cis_imp != .) | (te_imp > 0 & te_imp != .) | (te_mb > 0 & te_mb != .) | (te_mm > 0 & te_mm != .) | (te_sus > 0 & te_sus != .) | 	///
					   (ba_inoamp > 0 & ba_inoamp != .) | (ba_inoreh > 0 & ba_inoreh != .) | (ba_uriamp > 0 & ba_uriamp != .) | (ba_urireh > 0 & ba_urireh != .) | 		///
					   (b_beb > 0 & b_beb != .) | (ca_reh > 0 & ca_reh != .) | (ca_sus > 0 & ca_sus != .) | (ca_imp > 0 & ca_imp != .) | (bp_reh > 0 & bp_reh != .) | 	///
					   (bp_sus > 0 & bp_sus != .) | (bp_imp > 0 & bp_imp != .)) & intcb == 0 if brecha != . & ((ca_reh !=. & ba_inoamp != . & b_beb != . & te_imp != .) | intcb == 1) 
	sum		ind_123
	display r(mean)
	
	gen 	ind_124 = acc_ene != "1. Red pública (De una empresa distribuidora de energía eléctrica)" if acc_ene != "" & acc_ene != "9. Sin información" & finfo != 10 & matricula != 0		
	sum		ind_124
	display r(mean)
	
	gen 	ind_125 = ((area_ce1 > 0 & area_ce1 != .) | (area_ce2 > 0 & area_ce2 != .) | (area_ce3 > 0 & area_ce3 != .) | (area_ce4 > 0 & area_ce4 != .) | (area_ce5 > 0 & area_ce5 != .)) ///
						& intcb == 0 if brecha != . & finfo != 8 & (int_st == 0 | intcb == 1) 						// Sacar condición int_st == 0 si area_ce se define tomando en cuenta LL.EE. con sustitución total.
	sum		ind_125
	display r(mean)
	
	gen 	ind_126 = ((acc_ir > 0 & acc_ir != .) | (acc_rr1 > 0 & acc_rr1 != .) | (acc_rr2_5 > 0 & acc_rr2_5 != .) | (acc_ar > 0 & acc_ar != .)) & intcb == 0 		///
						if brecha != . & ((alumtot > 0 & alumtot != .) | intcb == 1 | finfo_ini == 5) 				// Condición finfo_ini == 5 es temporal hasta arreglar alumtot que viene de SRI.
	sum		ind_126
	display r(mean)
	
	* Indicadores de estrategia 1.3
	*-------------------------------
	rename	cod_local id_local
	merge 	1:1 id_local using "$Input\2021\LE_Amp_CIE_2112", keepusing(areaamp* alum*) keep(1 3) nogen
	forvalues i = 1/9 {
		gen aux_areaamp`i' = areaamp`i' 					if finfo_ini == 2 | finfo_ini == 3  | finfo_ini == 4 | finfo_ini == 6
		gen aux_alum`i' = alum`i' 							if finfo_ini == 2 | finfo_ini == 3  | finfo_ini == 4 | finfo_ini == 6
		if 	(`i' != 3) gen aux_alum`i'tot = alum`i'tot 		if finfo_ini == 2 | finfo_ini == 3  | finfo_ini == 4 | finfo_ini == 6
		if 	(`i' == 3) gen aux_alum`i'tot = alum`i' 		if finfo_ini == 2 | finfo_ini == 3  | finfo_ini == 4 | finfo_ini == 6
		drop areaamp`i' alum`i' 
		cap drop alum`i'tot 
		cap drop alum`i'max
	}
	drop alum alummax areaamp1_dz areaamp2_dz areaamp3_dz

	rename	id_local cod_local 
	merge 	1:1 cod_local using "$Input\2021\LE_Amp_SRI_2112", keepusing(areaamp* alum*) keep(1 3) nogen
	forvalues i = 1/9 {
		replace aux_areaamp`i' = areaamp`i' 				if finfo_ini == 5 | finfo_ini == 7 
		replace aux_alum`i' = alum`i' 						if finfo_ini == 5 | finfo_ini == 7 
		if 	(`i' != 3) replace aux_alum`i'tot = alum`i'tot 	if finfo_ini == 5 | finfo_ini == 7  
		if 	(`i' == 3) replace aux_alum`i'tot = alum`i' 	if finfo_ini == 5 | finfo_ini == 7  
		drop areaamp`i' alum`i' 
		cap drop alum`i'tot
		cap drop alum`i'max
		rename (aux_areaamp`i' aux_alum`i' aux_alum`i'tot) (areaamp`i' alum`i' alum`i'tot)
	}
	drop alum alummax areaamp1_dz areaamp2_dz areaamp3_dz
	
	gen 	ind_131 = (areaamp1 > 0 & areaamp1 != .) & intcb == 0 if brecha != . & ((alum1 > 0 & alum1 != .) | (intcb == 1 & m_Inicial > 0 & m_Inicial != .)) 					
	sum		ind_131
	display r(mean)
	
	gen 	ind_132 = (areaamp2 > 0 & areaamp2 != .) & intcb == 0 if brecha != . & ((alum2 > 0 & alum2 != .) | (intcb == 1 & m_Primaria > 0 & m_Primaria != .)) 				
	sum		ind_132
	display r(mean)
	
	gen 	ind_133 = (areaamp3 > 0 & areaamp3 != .) & intcb == 0 if brecha != . & ((alum3 > 0 & alum3 != .) | (intcb == 1 & m_Secundaria > 0 & m_Secundaria != .)) 					
	sum		ind_133
	display r(mean)
	
	gen 	ind_135 = (areaamp5 > 0 & areaamp5 != .) & intcb == 0 if brecha != . & ((alum5 > 0 & alum5 != .) | (intcb == 1 & m_EBE > 0 & m_EBE != .)) 					
	sum		ind_135
	display r(mean)	
	
	gen 	ind_136 = (areaamp4 > 0 & areaamp4 != .) & intcb == 0 if brecha != . & ((alum4 > 0 & alum4 != .) | (intcb == 1 & m_EBA > 0 & m_EBA != .)) 					
	sum		ind_136
	display r(mean)	

	gen 	ind_137 = (areaamp9 > 0 & areaamp9 != .) & intcb == 0 if brecha != . & ((alum9 > 0 & alum9 != .) | (intcb == 1 & m_CETPRO > 0 & m_CETPRO != .)) 					
	sum		ind_137
	display r(mean)	
	
	gen 	ind_138 = ((areaamp6 > 0 & areaamp6 != .) | (areaamp7 > 0 & areaamp7 != .) | (areaamp8 > 0 & areaamp8 != .)) & intcb == 0 if brecha != . & ///
					  ((alum6 > 0 & alum6 != .) | (alum7 > 0 & alum7 != .) | (alum8 > 0 & alum8 != .) | (intcb == 1 & ((m_ESFA > 0 & m_ESFA != .) | (m_IST > 0 & m_IST != .) | (m_ISP > 0 & m_ISP != .)))) 					
	sum		ind_138
	display r(mean)	
	
	* Indicadores de estrategia 1.4
	*-------------------------------
	drop  	disafil
	merge 	1:1 cod_local using "$Input\2021\Disafil_211203", keep(1 3) nogen
	
	gen 	ind_141 = disafil == 3 | (disafil != 1 & disafil != 3 & intcb == 0) if finfo != 10 & matricula != 0					
	sum		ind_141
	display r(mean)	
	
	* Indicadores de estrategia 1.5
	*-------------------------------
	egen	alum = rowtotal(alum1 alum2 alum3 alum4 alum5 alum6 alum7 alum8 alum9), missing
	gen 	ind_151 = (int_st == 1 | int_sp == 1 | int_rc == 1 | int_ri == 1 | int_ic == 1 | (areaamp > 0 & areaamp != .)) & intcb == 0 & finfo != 8 if brecha != . & ((alum > 0 & alum != .) | intcb == 1)
	sum		ind_151
	display r(mean)
	
	* Indicadores de estrategia 3.1
	*-------------------------------
	merge 	1:1 cod_local using "$Input\2021\LE_InfAdec_2021", keepusing(ac_infadec) keep(1 3) nogen
	gen 	ind_311 = ac_infadec != 1 if finfo != 10 & matricula != 0
	sum		ind_311
	display r(mean)
	
	* Indicadores de estrategia 4.1
	*-------------------------------
	gen 	ind_411 = ((area_ene1 > 0 & area_ene1 != .) | (area_ene2 > 0 & area_ene2 != .) | (area_ene3 > 0 & area_ene3 != .) | (area_ene4 > 0 & area_ene4 != .)) & intcb == 0 	///
						if brecha != . & finfo != 8 & ((area_ene1 != . | area_ene2 != . | area_ene3 != . | area_ene4 != .) | intcb == 1) 
	sum		ind_411
	display r(mean)
	
	* Indicadores de estrategia 4.2
	*-------------------------------
	gen 	año = 2021
	merge 	1:1 cod_local año using "$Input\LE_MantReg_18,20-21", keepusing(cod_local año Total_declarado) keep(1 3)
	gen 	mant_reg = (Total_declarado > 0 & Total_declarado != .) if _merge == 3
	drop	_merge
	
	rename 	año año_int
	merge 	1:1 cod_local año_int using "$Input\LE_Acond_17-22", keepusing(año_int interv) keep(1 3)
	gen		acond = (interv == "ACONDICIONAMIENTO CUARTO DE BOMBAS" | interv == "ACONDICIONAMIENTO DE AULAS" | interv == "ACONDICIONAMIENTO DE PREFABRICADOS" | ///
			interv == "ACONDICIONAMIENTO ESPACIOS ABIERTOS" | interv == "ACONDICIONAMIENTO INTEGRAL") if _merge == 3
	drop	_merge
	
	gen 	ind_412 = mant_reg != 1 & acond != 1 if finfo!= 10 & matricula != 0	
	sum		ind_412
	display r(mean)	
	
	compress
	save 	"LE_SPNIE_2021.dta", replace
	
	gen		EBR = Inicial != 0 | Primaria != 0 | Secundaria != 0
	egen 	m_EBR = rowtotal (m_Inicial m_Primaria m_Secundaria)
	gen 	EB = Inicial != 0 | Primaria != 0 | Secundaria != 0 | EBA != 0 | EBE!= 0 
	egen 	m_EB = rowtotal (m_Inicial m_Primaria m_Secundaria m_EBA m_EBE)
	keep 	cod_local region prov dist codgeo nom_local urbano rural Inicial Primaria Secundaria EBR EBA EBE EB ESFA IST ISP CETPRO matricula m_Inicial m_Primaria m_Secundaria m_EBR m_EBA m_EBE m_EB m_ESFA m_IST m_ISP m_CETPRO ind_*
	order 	cod_local region prov dist codgeo nom_local urbano rural Inicial Primaria Secundaria EBR EBA EBE EB ESFA IST ISP CETPRO matricula m_Inicial m_Primaria m_Secundaria m_EBR m_EBA m_EBE m_EB m_ESFA m_IST m_ISP m_CETPRO ind_*
	
	export 	excel using "LE_SPNIE_2021.xlsx", firstrow(var) sheetmodify 
	
	/*_____________________________________________________________________
	|                                                                      |
	|            		CÁLCULO DE INDICADORES 2020		                   |
	|_____________________________________________________________________*/
	
	use 	"$Input\2020\LE_BasePr_2012", clear	
	
	* Ajustes en información de matrícula
	*-------------------------------------
	replace	m_ESFA = matricula if cod_local == 288718
	
	* Indicadores de estrategia 1.1
	*-------------------------------
	gen 	ind_111 = int_st == 1 if brecha != .
	sum		ind_111
	display r(mean)
	
	gen 	ind_112 = int_sp == 1 if brecha != .
	sum		ind_112
	display r(mean)
	
	gen 	ind_113 = int_rc == 1 | int_ri == 1 if brecha != .
	sum		ind_113
	display r(mean)
	
	gen 	ind_114 = int_ic == 1 if brecha != .
	sum		ind_114
	display r(mean)

	gen 	ind_115 = int_st == 1 | int_sp == 1 | int_rc == 1 | int_ri == 1 | int_ic == 1 if brecha != .
	sum		ind_115
	display r(mean)
	
	gen 	ind_116 = ((cp_n > 0 & cp_n != .) | (cp_dr > 0 & cp_dr != .) | (cp_mm > 0 & cp_mm != .) | (cp_mb > 0 & cp_mb != .)) & intcb == 0 & zona == 2 if brecha != . & ((tramo > 0 & tramo != .) | intcb == 1 | zona == 1)
	sum		ind_116
	display r(mean)
	
	* Indicadores de estrategia 1.2
	*-------------------------------
	merge 	1:1 cod_local using "$Input\2020\LE_AccSSBB_2020", keepusing(acc_agua* acc_des* acc_ene*) keep(1 3) nogen
	
	gen 	ind_121 = (acc_agua != "1. Red pública" & acc_agua != "3. Camión-cisterna u otro similar" & acc_agua != "4. Pozo") 			///
					   if acc_agua != "" & acc_agua != "8. Sin información" & finfo!= 10 & matricula != 0 & acc_agua_finfo != "SE2019"	//  No se usa SE2019 pues tiene muchas observaciones con Red pública y en otros años no hay esa fuente.		
	sum		ind_121
	display r(mean)
					   
	gen 	ind_122 = (acc_des != "1. Red pública" & acc_des != "3. Pozo séptico")  if acc_des != "" & acc_des != "7. Sin información" & finfo!= 10 & matricula != 0 & acc_des_finfo != "SE2019"			
	sum		ind_122																														//  No se usa SE2019 pues tiene muchas observaciones con Red pública y en otros años no hay esa fuente.
	display r(mean)
	
	gen 	ind_123 = ((cis_imp > 0 & cis_imp != .) | (te_imp > 0 & te_imp != .) | (te_mb > 0 & te_mb != .) | (te_mm > 0 & te_mm != .) | (te_sus > 0 & te_sus != .) | 	///
					   (ba_inoamp > 0 & ba_inoamp != .) | (ba_inoreh > 0 & ba_inoreh != .) | (ba_uriamp > 0 & ba_uriamp != .) | (ba_urireh > 0 & ba_urireh != .) | 		///
					   (b_beb > 0 & b_beb != .) | (ca_reh > 0 & ca_reh != .) | (ca_sus > 0 & ca_sus != .) | (ca_imp > 0 & ca_imp != .) | (bp_reh > 0 & bp_reh != .) | 	///
					   (bp_sus > 0 & bp_sus != .) | (bp_imp > 0 & bp_imp != .)) & intcb == 0 if brecha != . & ((ca_reh !=. & ba_inoamp != . & b_beb != . & te_imp != .) | intcb == 1) 
	sum		ind_123
	display r(mean)
	
	gen 	ind_124 = acc_ene != "1. Red pública" if acc_ene != "" & acc_ene != "6. Sin información" & finfo!= 10 & matricula != 0		
	sum		ind_124
	display r(mean)
	
	gen 	ind_125 = ((area_ce1 > 0 & area_ce1 != .) | (area_ce2 > 0 & area_ce2 != .) | (area_ce3 > 0 & area_ce3 != .) | (area_ce4 > 0 & area_ce4 != .) | (area_ce5 > 0 & area_ce5 != .)) ///
						& intcb == 0 if brecha != . & finfo != 8 & (int_st == 0 | intcb == 1) 						// Sacar condición int_st == 0 si area_ce se define tomando en cuenta LL.EE. con sustitución total.
	sum		ind_125
	display r(mean)
	
	gen 	ind_126 = ((acc_ir > 0 & acc_ir != .) | (acc_rr1 > 0 & acc_rr1 != .) | (acc_rr2_5 > 0 & acc_rr2_5 != .) | (acc_ar > 0 & acc_ar != .)) & intcb == 0 		///
						if brecha != . & ((alumtot > 0 & alumtot != .) | intcb == 1 | finfo_ini == 5) 				// Condición finfo_ini == 5 es temporal hasta arreglar alumtot que viene de SRI.
	sum		ind_126
	display r(mean)
	
	* Indicadores de estrategia 1.4
	*-------------------------------
	gen 	ind_141 = disafil == 3 | (disafil != 1 & disafil != 3 & intcb == 0) if finfo!= 10 & matricula != 0			
	sum		ind_141
	display r(mean)	
	
	* Indicadores de estrategia 1.5
	*-------------------------------
	gen 	ind_151 = (int_st == 1 | int_sp == 1 | int_rc == 1 | int_ri == 1 | int_ic == 1 | (areaamp > 0 & areaamp != .)) & intcb == 0 & finfo != 8 if brecha != . & ((alumtot > 0 & alumtot != .) | intcb == 1)
	sum		ind_151
	display r(mean)
	
	* Indicadores de estrategia 4.1
	*-------------------------------
	gen 	ind_411 = ((area_ene1 > 0 & area_ene1 != .) | (area_ene2 > 0 & area_ene2 != .) | (area_ene3 > 0 & area_ene3 != .) | (area_ene4 > 0 & area_ene4 != .)) & intcb == 0 	///
						if brecha != . & finfo != 8 & ((area_ene1 != . | area_ene2 != . | area_ene3 != . | area_ene4 != .) | intcb == 1) 
	sum		ind_411
	display r(mean)
	
	* Indicadores de estrategia 4.2
	*-------------------------------
	gen 	año = 2020
	merge 	1:1 cod_local año using "$Input\LE_MantReg_18,20-21", keepusing(cod_local año Total_declarado) keep(1 3)
	gen 	mant_reg = (Total_declarado > 0 & Total_declarado != .) if _merge == 3
	drop	_merge
	
	rename 	año año_int
	merge 	1:1 cod_local año_int using "$Input\LE_Acond_17-22", keepusing(año_int interv) keep(1 3)
	gen		acond = (interv == "ACONDICIONAMIENTO CUARTO DE BOMBAS" | interv == "ACONDICIONAMIENTO DE AULAS" | interv == "ACONDICIONAMIENTO DE PREFABRICADOS" | ///
			interv == "ACONDICIONAMIENTO ESPACIOS ABIERTOS" | interv == "ACONDICIONAMIENTO INTEGRAL") if _merge == 3
	drop	_merge
	
	gen 	ind_412 = mant_reg != 1 & acond != 1 if finfo!= 10 & matricula != 0	
	sum		ind_412
	display r(mean)	
	
	compress
	save 	"LE_SPNIE_2020.dta", replace
	
	gen		EBR = Inicial != 0 | Primaria != 0 | Secundaria != 0
	egen 	m_EBR = rowtotal (m_Inicial m_Primaria m_Secundaria)
	gen 	EB = Inicial != 0 | Primaria != 0 | Secundaria != 0 | EBA != 0 | EBE!= 0 
	egen 	m_EB = rowtotal (m_Inicial m_Primaria m_Secundaria m_EBA m_EBE)
	keep 	cod_local region prov dist codgeo nom_local urbano rural Inicial Primaria Secundaria EBR EBA EBE EB ESFA IST ISP CETPRO matricula m_Inicial m_Primaria m_Secundaria m_EBR m_EBA m_EBE m_EB m_ESFA m_IST m_ISP m_CETPRO ind_*
	order 	cod_local region prov dist codgeo nom_local urbano rural Inicial Primaria Secundaria EBR EBA EBE EB ESFA IST ISP CETPRO matricula m_Inicial m_Primaria m_Secundaria m_EBR m_EBA m_EBE m_EB m_ESFA m_IST m_ISP m_CETPRO ind_*
	
	export 	excel using "LE_SPNIE_2020.xlsx", firstrow(var) sheetmodify 
	
	/*_____________________________________________________________________
	|                                                                      |
	|            		CÁLCULO DE INDICADORES 2019		                   |
	|_____________________________________________________________________*/
	
	use     "$Input\2019\LE_BasePr_1912", clear
	
	* Ajustes en información de matrícula
	*-------------------------------------
	replace	m_Primaria = matricula if cod_local == 520500
	
	* Indicadores de estrategia 1.1
	*-------------------------------
	gen 	ind_111 = int_st == 1 if brecha != .
	sum		ind_111
	display r(mean)
		
	gen 	ind_112 = int_sp == 1 if brecha != .
	sum		ind_112
	display r(mean)
	
	gen 	ind_113 = int_rc == 1 | int_ri == 1 if brecha != .
	sum		ind_113
	display r(mean)
	
	gen 	ind_114 = int_ic == 1 if brecha != .
	sum		ind_114
	display r(mean)

	gen 	ind_115 = int_st == 1 | int_sp == 1 | int_rc == 1 | int_ri == 1 | int_ic == 1 if brecha != .
	sum		ind_115
	display r(mean)
	
	gen 	ind_116 = ((cp_n > 0 & cp_n != .) | (cp_dr > 0 & cp_dr != .) | (cp_mm > 0 & cp_mm != .) | (cp_mb > 0 & cp_mb != .)) & intcb == 0 & zona == 2 if brecha != . & ((tramo > 0 & tramo != .) | intcb == 1 | zona == 1)
	sum		ind_116
	display r(mean)
	
	* Indicadores de estrategia 1.2
	*-------------------------------
	merge 	1:1 cod_local using "$Input\2019\LE_AccSSBB_2019", keepusing(acc_agua* acc_des* acc_ene*) keep(1 3) nogen
	
	gen 	ind_121 = (acc_agua != "1. Red pública" & acc_agua != "3. Camión-cisterna u otro similar" & acc_agua != "4. Pozo") 		///
					   if acc_agua != "" & acc_agua != "8. Sin información" & matricula != 0 & acc_agua_finfo != "SE2019"			//  No se usa SE2019 pues tiene muchas observaciones con Red pública y en otros años no hay esa fuente.
	sum		ind_121
	display r(mean)
					   
	gen 	ind_122 = (acc_des != "1. Red pública" & acc_des != "3. Pozo séptico")  if acc_des != "" & acc_des != "7. Sin información" & matricula != 0	& acc_des_finfo != "SE2019"	 	
	sum		ind_122																													//  No se usa SE2019 pues tiene muchas observaciones con Red pública y en otros años no hay esa fuente.
	display r(mean)
	
	gen 	ind_123 = ((cis_imp > 0 & cis_imp != .) | (te_imp > 0 & te_imp != .) | (te_mb > 0 & te_mb != .) | (te_mm > 0 & te_mm != .) | (te_sus > 0 & te_sus != .) | 	///
					   (ba_inoamp > 0 & ba_inoamp != .) | (ba_inoreh > 0 & ba_inoreh != .) | (ba_uriamp > 0 & ba_uriamp != .) | (ba_urireh > 0 & ba_urireh != .) | 		///
					   (b_beb > 0 & b_beb != .) | (ca_reh > 0 & ca_reh != .) | (ca_sus > 0 & ca_sus != .) | (ca_imp > 0 & ca_imp != .) | (bp_reh > 0 & bp_reh != .) | 	///
					   (bp_sus > 0 & bp_sus != .) | (bp_imp > 0 & bp_imp != .)) & intcb == 0 if brecha != . & ((ca_reh !=. & ba_inoamp != . & b_beb != . & te_imp != .) | intcb == 1)		   
	tab 	ind_123
	sum		ind_123
	display r(mean)
	
	gen 	ind_124 = acc_ene != "1. Red pública" if acc_ene != "" & acc_ene != "6. Sin información" & matricula != 0		
	sum		ind_124
	display r(mean)
	
	gen 	ind_125 = ((area_ce1 > 0 & area_ce1 != .) | (area_ce2 > 0 & area_ce2 != .) | (area_ce3 > 0 & area_ce3 != .) | (area_ce4 > 0 & area_ce4 != .) | (area_ce5 > 0 & area_ce5 != .)) ///
						& intcb == 0 if brecha != . & finfo != 8 & ((int_st == 0 & int_sp == 0) | intcb == 1) 		// Sacar condición int_st == 0 & int_sp == 0 si area_ce se define tomando en cuenta LL.EE. con sustitución total y parcial.
																													// Hasta el año 2019, area_ce no se calculó para int_sp == 1.
	tab		ind_125
	sum		ind_125
	display r(mean)
	
	gen 	ind_126 = ((acc_ir > 0 & acc_ir != .) | (acc_rr1 > 0 & acc_rr1 != .) | (acc_rr2_5 > 0 & acc_rr2_5 != .) | (acc_ar > 0 & acc_ar != .)) & intcb == 0 		///
						if brecha != . & ((alumtot > 0 & alumtot != .) | intcb == 1 | finfo_ini == 5) 				// Condición finfo_ini == 5 es temporal hasta arreglar alumtot que viene de SRI.
	tab		ind_126
	sum		ind_126
	display r(mean)
	
	* Indicadores de estrategia 1.3
	*-------------------------------
	rename	cod_local id_local
	merge 	1:1 id_local using "$Input\2019\LE_Amp_CIE_1912", keepusing(areaamp* alum*) keep(1 3) nogen
	forvalues i = 1/9 {
		gen aux_areaamp`i' = areaamp`i' 					if finfo_ini == 2 | finfo_ini == 3  | finfo_ini == 4 | finfo_ini == 6 
		gen aux_alum`i' = alum`i' 							if finfo_ini == 2 | finfo_ini == 3  | finfo_ini == 4 | finfo_ini == 6
		if 	(`i' != 3) gen aux_alum`i'tot = alum`i'tot 		if finfo_ini == 2 | finfo_ini == 3  | finfo_ini == 4 | finfo_ini == 6
		if 	(`i' == 3) gen aux_alum`i'tot = alum`i' 		if finfo_ini == 2 | finfo_ini == 3  | finfo_ini == 4 | finfo_ini == 6
		drop areaamp`i' alum`i' 
		cap drop alum`i'tot 
		cap drop alum`i'max
	}
	drop alum alummax areaamp1_dz areaamp2_dz areaamp3_dz

	rename	id_local cod_local 
	merge 	1:1 cod_local using "$Input\2019\LE_Amp_SRI_1912", keepusing(areaamp* alum*) keep(1 3) nogen
	forvalues i = 1/9 {
		replace aux_areaamp`i' = areaamp`i' 				if finfo_ini == 5 | finfo_ini == 7  
		replace aux_alum`i' = alum`i' 						if finfo_ini == 5 | finfo_ini == 7 
		if 	(`i' != 3) replace aux_alum`i'tot = alum`i'tot 	if finfo_ini == 5 | finfo_ini == 7  
		if 	(`i' == 3) replace aux_alum`i'tot = alum`i' 	if finfo_ini == 5 | finfo_ini == 7  
		drop areaamp`i' alum`i' 
		cap drop alum`i'tot
		cap drop alum`i'max
		rename (aux_areaamp`i' aux_alum`i' aux_alum`i'tot) (areaamp`i' alum`i' alum`i'tot)
	}
	drop alum alummax areaamp1_dz areaamp2_dz areaamp3_dz
	
	gen 	ind_131 = (areaamp1 > 0 & areaamp1 != .) & intcb == 0 if brecha != . & ((alum1 > 0 & alum1 != .) | (intcb == 1 & m_Inicial > 0 & m_Inicial != .)) 					
	sum		ind_131
	display r(mean)
	
	gen 	ind_132 = (areaamp2 > 0 & areaamp2 != .) & intcb == 0 if brecha != . & ((alum2 > 0 & alum2 != .) | (intcb == 1 & m_Primaria > 0 & m_Primaria != .)) 				
	sum		ind_132
	display r(mean)
	
	gen 	ind_133 = (areaamp3 > 0 & areaamp3 != .) & intcb == 0 if brecha != . & ((alum3 > 0 & alum3 != .) | (intcb == 1 & m_Secundaria > 0 & m_Secundaria != .)) 					
	sum		ind_133
	display r(mean)
	
	gen 	ind_135 = (areaamp5 > 0 & areaamp5 != .) & intcb == 0 if brecha != . & ((alum5 > 0 & alum5 != .) | (intcb == 1 & m_EBE > 0 & m_EBE != .)) 					
	sum		ind_135
	display r(mean)	
	
	gen 	ind_136 = (areaamp4 > 0 & areaamp4 != .) & intcb == 0 if brecha != . & ((alum4 > 0 & alum4 != .) | (intcb == 1 & m_EBA > 0 & m_EBA != .)) 					
	sum		ind_136
	display r(mean)	

	gen 	ind_137 = (areaamp9 > 0 & areaamp9 != .) & intcb == 0 if brecha != . & ((alum9 > 0 & alum9 != .) | (intcb == 1 & m_CETPRO > 0 & m_CETPRO != .)) 					
	sum		ind_137
	display r(mean)	
	
	gen 	ind_138 = ((areaamp6 > 0 & areaamp6 != .) | (areaamp7 > 0 & areaamp7 != .) | (areaamp8 > 0 & areaamp8 != .)) & intcb == 0 if brecha != . & ///
					  ((alum6 > 0 & alum6 != .) | (alum7 > 0 & alum7 != .) | (alum8 > 0 & alum8 != .) | (intcb == 1 & ((m_ESFA > 0 & m_ESFA != .) | (m_IST > 0 & m_IST != .) | (m_ISP > 0 & m_ISP != .)))) 					
	sum		ind_138
	display r(mean)	
	
	* Indicadores de estrategia 1.4
	*-------------------------------
	gen 	ind_141 = disafil == 3 | (disafil != 1 & disafil != 3 & intcb == 0) if matricula != 0			
	sum		ind_141
	display r(mean)	
	
	* Indicadores de estrategia 1.5
	*-------------------------------
	egen	alum = rowtotal(alum1 alum2 alum3 alum4 alum5 alum6 alum7 alum8 alum9), missing
	gen 	ind_151 = (int_st == 1 | int_sp == 1 | int_rc == 1 | int_ri == 1 | int_ic == 1 | (areaamp > 0 & areaamp != .)) & intcb == 0 & finfo != 8 if brecha != . & ((alum > 0 & alum != .) | intcb == 1)
	sum		ind_151
	display r(mean)
	
	* Indicadores de estrategia 4.1
	*-------------------------------
	gen 	ind_411 = ((area_ene1 > 0 & area_ene1 != .) | (area_ene2 > 0 & area_ene2 != .) | (area_ene3 > 0 & area_ene3 != .) | (area_ene4 > 0 & area_ene4 != .)) & intcb == 0 	///
						if brecha != . & finfo != 8 & ((area_ene1 != . | area_ene2 != . | area_ene3 != . | area_ene4 != .) | intcb == 1) 
	sum		ind_411
	display r(mean)
	
	compress
	save 	"LE_SPNIE_2019.dta", replace
	
	gen		EBR = Inicial != 0 | Primaria != 0 | Secundaria != 0
	egen 	m_EBR = rowtotal (m_Inicial m_Primaria m_Secundaria)
	gen 	EB = Inicial != 0 | Primaria != 0 | Secundaria != 0 | EBA != 0 | EBE!= 0 
	egen 	m_EB = rowtotal (m_Inicial m_Primaria m_Secundaria m_EBA m_EBE)
	keep 	cod_local region prov dist codgeo nom_local urbano rural Inicial Primaria Secundaria EBR EBA EBE EB ESFA IST ISP CETPRO matricula m_Inicial m_Primaria m_Secundaria m_EBR m_EBA m_EBE m_EB m_ESFA m_IST m_ISP m_CETPRO ind_*
	order 	cod_local region prov dist codgeo nom_local urbano rural Inicial Primaria Secundaria EBR EBA EBE EB ESFA IST ISP CETPRO matricula m_Inicial m_Primaria m_Secundaria m_EBR m_EBA m_EBE m_EB m_ESFA m_IST m_ISP m_CETPRO ind_*
	
	export 	excel using "LE_SPNIE_2019.xlsx", firstrow(var) sheetmodify 
	
	/*_____________________________________________________________________
	|                                                                      |
	|            		CÁLCULO DE INDICADORES 2018		                   |
	|_____________________________________________________________________*/
	
	use 	"$Input\2018\LE_BasePr_1904", clear	
	
	* Ajustes en información de matrícula
	*-------------------------------------
	replace	m_Primaria = matricula if cod_local == 110334 | cod_local == 247011
	
	* Indicadores de estrategia 1.1
	*-------------------------------
	gen 	ind_111 = int_st == 1 if brecha != .
	sum		ind_111
	display r(mean)
	
	gen 	ind_112 = int_sp == 1 if brecha != .
	sum		ind_112
	display r(mean)
	
	gen 	ind_113 = int_rc == 1 | int_ri == 1 if brecha != .
	sum		ind_113
	display r(mean)
	
	gen 	ind_114 = int_ic == 1 if brecha != .
	sum		ind_114
	display r(mean)

	gen 	ind_115 = int_st == 1 | int_sp == 1 | int_rc == 1 | int_ri == 1 | int_ic == 1 if brecha != .
	sum		ind_115
	display r(mean)
	
	gen 	ind_116 = ((cp_n > 0 & cp_n != .) | (cp_dr > 0 & cp_dr != .) | (cp_mm > 0 & cp_mm != .) | (cp_mb > 0 & cp_mb != .)) & intcb == 0 & zona == 2 if brecha != . & ((tramo > 0 & tramo != .) | intcb == 1 | zona == 1)
	sum		ind_116
	display r(mean)
	
	* Indicadores de estrategia 1.2
	*-------------------------------
	gen 	ind_123 = ((cis_imp > 0 & cis_imp != .) | (te_imp > 0 & te_imp != .) | (te_mb > 0 & te_mb != .) | (te_mm > 0 & te_mm != .) | (te_sus > 0 & te_sus != .) | 	///
					   (ba_inoamp > 0 & ba_inoamp != .) | (ba_inoreh > 0 & ba_inoreh != .) | (ba_uriamp > 0 & ba_uriamp != .) | (ba_urireh > 0 & ba_urireh != .) | 		///
					   (b_beb > 0 & b_beb != .) | (ca_reh > 0 & ca_reh != .) | (ca_sus > 0 & ca_sus != .) | (ca_imp > 0 & ca_imp != .) | (bp_reh > 0 & bp_reh != .) | 	///
					   (bp_sus > 0 & bp_sus != .) | (bp_imp > 0 & bp_imp != .)) & intcb == 0 if brecha != . & ((ca_reh !=. & ba_inoamp != . & b_beb != . & te_imp != .) | intcb == 1) 
	sum		ind_123
	display r(mean)
	
	gen 	ind_125 = ((area_ce1 > 0 & area_ce1 != .) | (area_ce2 > 0 & area_ce2 != .) | (area_ce3 > 0 & area_ce3 != .) | (area_ce4 > 0 & area_ce4 != .) | (area_ce5 > 0 & area_ce5 != .)) ///
						& intcb == 0 if brecha != . & finfo != 8 & ((int_st == 0  & int_sp == 0)| intcb == 1) 		// Sacar condición int_st == 0 & int_sp == 0 si area_ce se define tomando en cuenta LL.EE. con sustitución total y parcial.
																													// Hasta el año 2019, area_ce no se calculó para int_sp == 1.
	sum		ind_125
	display r(mean)
	
	gen 	ind_126 = ((acc_ir > 0 & acc_ir != .) | (acc_rr1 > 0 & acc_rr1 != .) | (acc_rr2_5 > 0 & acc_rr2_5 != .) | (acc_ar > 0 & acc_ar != .)) & intcb == 0 		///
						if brecha != . & ((alumtot > 0 & alumtot != .) | intcb == 1 | finfo_ini == 5) 				// Condición finfo_ini == 5 es temporal hasta arreglar alumtot que viene de SRI.
	sum		ind_126
	display r(mean)
	
	* Indicadores de estrategia 1.3
	*-------------------------------
	rename	cod_local id_local
	merge 	1:1 id_local using "$Input\2018\LE_Amp_CIE_1904", keepusing(areaamp* alum*) keep(1 3) nogen
	forvalues i = 1/9 {
		gen aux_areaamp`i' = areaamp`i' 					if finfo_ini == 2 | finfo_ini == 3  | finfo_ini == 4 | finfo_ini == 6 
		gen aux_alum`i' = alum`i' 							if finfo_ini == 2 | finfo_ini == 3  | finfo_ini == 4 | finfo_ini == 6
		if 	(`i' != 3) gen aux_alum`i'tot = alum`i'tot 		if finfo_ini == 2 | finfo_ini == 3  | finfo_ini == 4 | finfo_ini == 6
		if 	(`i' == 3) gen aux_alum`i'tot = alum`i' 		if finfo_ini == 2 | finfo_ini == 3  | finfo_ini == 4 | finfo_ini == 6
		drop areaamp`i' alum`i' 
		cap drop alum`i'tot 
		cap drop alum`i'max
	}
	drop alum alummax areaamp1_dz areaamp2_dz areaamp3_dz

	rename	id_local cod_local 
	merge 	1:1 cod_local using "$Input\2018\LE_Amp_SRI_1904", keepusing(areaamp* alum*) keep(1 3) nogen
	forvalues i = 1/9 {
		replace aux_areaamp`i' = areaamp`i' 				if finfo_ini == 5 | finfo_ini == 7  
		replace aux_alum`i' = alum`i' 						if finfo_ini == 5 | finfo_ini == 7 
		if 	(`i' != 3) replace aux_alum`i'tot = alum`i'tot 	if finfo_ini == 5 | finfo_ini == 7  
		if 	(`i' == 3) replace aux_alum`i'tot = alum`i' 	if finfo_ini == 5 | finfo_ini == 7  
		drop areaamp`i' alum`i' 
		cap drop alum`i'tot
		cap drop alum`i'max
		rename (aux_areaamp`i' aux_alum`i' aux_alum`i'tot) (areaamp`i' alum`i' alum`i'tot)
	}
	drop alum alummax areaamp1_dz areaamp2_dz areaamp3_dz
	
	gen 	ind_131 = (areaamp1 > 0 & areaamp1 != .) & intcb == 0 if brecha != . & ((alum1 > 0 & alum1 != .) | (intcb == 1 & m_Inicial > 0 & m_Inicial != .)) 					
	sum		ind_131
	display r(mean)
	
	gen 	ind_132 = (areaamp2 > 0 & areaamp2 != .) & intcb == 0 if brecha != . & ((alum2 > 0 & alum2 != .) | (intcb == 1 & m_Primaria > 0 & m_Primaria != .)) 				
	sum		ind_132
	display r(mean)
	
	gen 	ind_133 = (areaamp3 > 0 & areaamp3 != .) & intcb == 0 if brecha != . & ((alum3 > 0 & alum3 != .) | (intcb == 1 & m_Secundaria > 0 & m_Secundaria != .)) 					
	sum		ind_133
	display r(mean)
	
	gen 	ind_135 = (areaamp5 > 0 & areaamp5 != .) & intcb == 0 if brecha != . & ((alum5 > 0 & alum5 != .) | (intcb == 1 & m_EBE > 0 & m_EBE != .)) 					
	sum		ind_135
	display r(mean)	
	
	gen 	ind_136 = (areaamp4 > 0 & areaamp4 != .) & intcb == 0 if brecha != . & ((alum4 > 0 & alum4 != .) | (intcb == 1 & m_EBA > 0 & m_EBA != .)) 					
	sum		ind_136
	display r(mean)	

	gen 	ind_137 = (areaamp9 > 0 & areaamp9 != .) & intcb == 0 if brecha != . & ((alum9 > 0 & alum9 != .) | (intcb == 1 & m_CETPRO > 0 & m_CETPRO != .)) 					
	sum		ind_137
	display r(mean)	
	
	gen 	ind_138 = ((areaamp6 > 0 & areaamp6 != .) | (areaamp7 > 0 & areaamp7 != .) | (areaamp8 > 0 & areaamp8 != .)) & intcb == 0 if brecha != . & ///
					  ((alum6 > 0 & alum6 != .) | (alum7 > 0 & alum7 != .) | (alum8 > 0 & alum8 != .) | (intcb == 1 & ((m_ESFA > 0 & m_ESFA != .) | (m_IST > 0 & m_IST != .) | (m_ISP > 0 & m_ISP != .)))) 					
	sum		ind_138
	display r(mean)	
	
	* Indicadores de estrategia 1.4
	*-------------------------------
	gen 	ind_141 = disafil == 3 | (disafil != 1 & disafil != 3 & intcb == 0) if matricula != 0			
	sum		ind_141
	display r(mean)	
	
	* Indicadores de estrategia 1.5
	*-------------------------------
	egen	alum = rowtotal(alum1 alum2 alum3 alum4 alum5 alum6 alum7 alum8 alum9), missing
	gen 	ind_151 = (int_st == 1 | int_sp == 1 | int_rc == 1 | int_ri == 1 | int_ic == 1 | (areaamp > 0 & areaamp != .)) & intcb == 0 & finfo != 8 if brecha != . & ((alum > 0 & alum != .) | intcb == 1)
	sum		ind_151
	display r(mean)
	
	* Indicadores de estrategia 4.1
	*-------------------------------
	gen 	ind_411 = ((area_ene1 > 0 & area_ene1 != .) | (area_ene2 > 0 & area_ene2 != .) | (area_ene3 > 0 & area_ene3 != .) | (area_ene4 > 0 & area_ene4 != .)) & intcb == 0 	///
						if brecha != . & finfo != 8 & ((area_ene1 != . | area_ene2 != . | area_ene3 != . | area_ene4 != .) | intcb == 1) 
	sum		ind_411
	display r(mean)
	
	* Indicadores de estrategia 4.2
	*-------------------------------
	gen 	año = 2018
	merge 	1:1 cod_local año using "$Input\LE_MantReg_18,20-21", keepusing(cod_local año Total_declarado) keep(1 3)
	gen 	mant_reg = (Total_declarado > 0 & Total_declarado != .) if _merge == 3
	drop	_merge
	
	rename 	año año_int
	merge 	1:1 cod_local año_int using "$Input\LE_Acond_17-22", keepusing(año_int interv) keep(1 3)
	gen		acond = (interv == "ACONDICIONAMIENTO CUARTO DE BOMBAS" | interv == "ACONDICIONAMIENTO DE AULAS" | interv == "ACONDICIONAMIENTO DE PREFABRICADOS" | ///
			interv == "ACONDICIONAMIENTO ESPACIOS ABIERTOS" | interv == "ACONDICIONAMIENTO INTEGRAL") if _merge == 3
	drop	_merge
	
	gen 	ind_412 = mant_reg != 1 & acond != 1 if matricula != 0	
	sum		ind_412
	display r(mean)	
	
	compress
	save 	"LE_SPNIE_2018.dta", replace
	
	gen		EBR = Inicial != 0 | Primaria != 0 | Secundaria != 0
	egen 	m_EBR = rowtotal (m_Inicial m_Primaria m_Secundaria)
	gen 	EB = Inicial != 0 | Primaria != 0 | Secundaria != 0 | EBA != 0 | EBE!= 0 
	egen 	m_EB = rowtotal (m_Inicial m_Primaria m_Secundaria m_EBA m_EBE)
	keep 	cod_local region prov dist codgeo nom_local urbano rural Inicial Primaria Secundaria EBR EBA EBE EB ESFA IST ISP CETPRO matricula m_Inicial m_Primaria m_Secundaria m_EBR m_EBA m_EBE m_EB m_ESFA m_IST m_ISP m_CETPRO ind_*
	order 	cod_local region prov dist codgeo nom_local urbano rural Inicial Primaria Secundaria EBR EBA EBE EB ESFA IST ISP CETPRO matricula m_Inicial m_Primaria m_Secundaria m_EBR m_EBA m_EBE m_EB m_ESFA m_IST m_ISP m_CETPRO ind_*
	
	export 	excel using "LE_SPNIE_2018.xlsx", firstrow(var) sheetmodify 
	
	/*_____________________________________________________________________
	|                                                                      |
	|            	CÁLCULO DE INDICADORES PNIE (2015-2017)    	    	   |
	|_____________________________________________________________________*/

	use 	"$Input\PNIE\Data_Completa", clear	
	
		* Indicadores de estrategia 1.1
	*-------------------------------
	gen 	ind_111 = IDEM == 1 if IDEM != .
	sum		ind_111
	display r(mean)
	
	gen 	ind_112 = IEDEM == 1 if IEDEM != .
	sum		ind_112
	display r(mean)
	
	gen 	ind_113 = RI == 1 | RC == 1 if RI != . | RC != .
	sum		ind_113
	display r(mean)
	
	gen 	ind_114 = ICONT if ICONT != .
	sum		ind_114
	display r(mean)

	gen 	ind_115 = IDEM == 1 | IEDEM == 1 | RI == 1 | RC == 1 | ICONT == 1 if IDEM != . | IEDEM != . | RI != . | RC != . | ICONT != .
	sum		ind_115
	display r(mean)
	
	gen 	ind_116 = ((Implementar_cerco == 1 & Implementar_cerco != .) | (Reponer_cerco == 1 & Reponer_cerco != .) | (Mante_medio == 1 & Mante_medio != .) | ///
					   (Mante_bajo == 1 & Mante_bajo != .)) & area_sig == "Urbana" if area_sig != ""
	sum		ind_116
	display r(mean)
	
	* Indicadores de estrategia 1.2
	*-------------------------------
	gen 	ind_121 = (ct_acc_agua > 0 & ct_acc_agua != .) if ct_acc_agua != .
	sum		ind_121
	display r(mean)
	
	gen 	ind_122 = (ct_acc_saneamiento > 0 & ct_acc_saneamiento != .) if ct_acc_saneamiento != .
	sum		ind_122
	display r(mean)
	
	gen 	ind_123 = (ctot_calidadays_aj > 0 & ctot_calidadays_aj != .)  if ctot_calidadays_aj != .
	sum		ind_123
	display r(mean)
	
	gen 	ind_124 = (costoAccE_mod > 0 & costoAccE_mod != .) if costoAccE_mod != .
	sum		ind_124
	display r(mean)	
		
	gen 	ind_125 = (CTot_elec1 > 0 & CTot_elec1 != .) | (CTot_elec2 > 0 & CTot_elec2 != .) | (CTot_elec3 > 0 & CTot_elec3 != .) | (CTot_elec4 > 0 & CTot_elec4 != .) | (CTot_elec5 > 0 & CTot_elec5 != .) ///
					  if (CTot_elec1 != . | CTot_elec2 != . | CTot_elec3 != . | CTot_elec4 != . | CTot_elec5 != .) & IDEM == 0	// Se establece condición IDEM == 0 para mantener coherencia con cálculo de otros años.
	sum		ind_125
	display r(mean)
	
	gen 	ind_126 = (Ctot_InodAccReq > 0 & Ctot_InodAccReq != .) | (Ctot_ramp > 0 & Ctot_ramp != .) | (Ctot_ascen > 0 & Ctot_ascen != .) 		///
					  if (Ctot_InodAccReq != . | Ctot_ramp != . | Ctot_ascen != .) & alumnos > 0 & alumnos != . 				
	sum		ind_126
	display r(mean)
	
	* Indicadores de estrategia 1.3
	*-------------------------------
	gen 	ind_131 = iampl_ini > 0 & iampl_ini != . & INICIAL == 1 if iampl_ini != . & INICIAL == 1 & Alum_INI > 0 & Alum_INI != .				
	sum		ind_131
	display r(mean)
	
	gen 	ind_132 = iampl_pri > 0 & iampl_pri != . & PRIMARIA == 1 if iampl_pri != . & PRIMARIA == 1 & Alum_PRI > 0 & Alum_PRI != .	
	sum		ind_132
	display r(mean)
	
	gen 	ind_133 = iampl_sec > 0 & iampl_sec != . & SECUNDARIA == 1 if iampl_sec != . & SECUNDARIA == 1 & Alum_SEC > 0 & Alum_SEC != .					
	sum		ind_133
	display r(mean)
	
	gen 	ind_135 = iampl_EBE > 0 & iampl_EBE != . & EBE == 1 if iampl_EBE != . & EBE == 1 & Alum_EBE > 0 & Alum_EBE != . 				
	sum		ind_135
	display r(mean)	
	
	gen 	ind_136 = iampl_EBA > 0 & iampl_EBA != . & EBA == 1 if iampl_EBA != . & EBA == 1 & Alum_EBA > 0 & Alum_EBA != . 					
	sum		ind_136
	display r(mean)	

	gen 	ind_137 = iampl_cetpro > 0 & iampl_cetpro != . & CETPRO == 1 if iampl_cetpro != . & CETPRO == 1 & Alum_CETPRO > 0 & Alum_CETPRO != . 					
	sum		ind_137
	display r(mean)	
	
	gen 	ind_138 = (iampl_ist > 0 & iampl_ist != . & EST == 1) | (iampl_isp > 0 & iampl_isp != . & ESP == 1) | ///
					  (iampl_ESFA > 0 & iampl_ESFA != . & ESFA == 1) if (iampl_ist != . & EST == 1 & Alum_IST > 0 & Alum_IST != .) | ///
					  (iampl_isp != . & ESP == 1 & Alum_ISP > 0 & Alum_ISP != .) | (iampl_ESFA != . & ESFA == 1 & Alum_ESFA > 0 & Alum_ESFA != .)					
	sum		ind_138
	display r(mean)	
	
	* Indicadores de estrategia 1.5
	*-------------------------------
	gen 	ind_151 = (IDEM == 1 | IEDEM == 1 | RI == 1 | RC == 1 | ICONT == 1 | (area_Sp43 > 0 & area_Sp43 != .)) if (IDEM != . | IEDEM != . | RI != . | RC != . | ICONT != .) & area_Sp43 != . & alumnos > 0 & alumnos != . 
	sum		ind_151
	display r(mean)

	* Indicadores de estrategia 4.1
	*-------------------------------
	gen 	ind_411 = (CTot_ENE > 0 & CTot_ENE != .) if CTot_ENE != . & IDEM == 0	// Se establece condición IDEM == 0 para mantener coherencia con cálculo de otros años.
	sum		ind_411
	display r(mean)
	
	gen 	_d_dpto1 = departamento
	replace _d_dpto1 = "Ancash" if _d_dpto1 == "Áncash"
	replace _d_dpto1 = "Lima Provincias" if departamento == "Lima" & provincia != "Lima"
	replace _d_dpto1 = "Lima Metropolitana" if departamento == "Lima" & provincia == "Lima"
	encode 	_d_dpto1, gen (region)
	drop 	_d_dpto1		// Codificar variable de región y crear nueva variable d_dpto1 con distinción LIMA METROPOLITANA Y LIMA PROVINCIAS.
	
	compress
	save 	"LE_SPNIE_PNIE.dta", replace
	
	egen 	_aux = rowtotal(ind_*), missing
	drop 	if _aux == .
	drop 	_aux
	
	keep 	id_local region provincia distrito UBIGEO_2015 area_sig ind_*
	order 	id_local region provincia distrito UBIGEO_2015 area_sig ind_*
	
	export 	excel using "LE_SPNIE_PNIE.xlsx", firstrow(var) sheetmodify 	