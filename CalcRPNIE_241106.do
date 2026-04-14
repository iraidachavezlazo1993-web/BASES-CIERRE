	/*__________________________________________________________________________
	|	                                                      					|
	|	MINEDU - Cálculo de Indicadores de Resultados del PNIE			    	|
	|	Actualizado: 06/11/2024				    								|
	|__________________________________________________________________________*/

	/*_____________________________________________________________________
	|                                                                      |
	|                               PRÓLOGO                                |
	|_____________________________________________________________________*/ 
	
	clear 	all
	
	global 	Input  		=   "C:\CalcRPNIE2411\input"			// Carpeta Input
	global 	Input2 		=   "C:\CalcSPNIE2302\"					// Carpeta SPNIE2302
	global 	Final  		=   "C:\CalcRPNIE2411\"					// Carpeta Final
	cd 		"$Final"

	set 	more off 
	set 	varabbrev off
	set 	type double
	set 	seed 339487731
	set 	excelxlsxlargefile on
	
	/*_____________________________________________________________________
	|                                                                      |
	|            		CÁLCULO DE INDICADORES 2023	                   	   |
	|_____________________________________________________________________*/
	
	use 	"$Input\2023\LE_BasePr_2312", clear	
	
	gen 	ind_6 = ac_cd_infadec == 1 if finfo != 13 & matricula != 0	
	sum		ind_6
	display r(mean)
	
	gen 	ind_9 = disafil == 1 | (disafil != 1 & disafil != 2 & intcb == 1) if finfo != 13 & matricula != 0		
	sum		ind_9
	display r(mean)	
	
	gen 	ind_10 = (int_st == 0 & int_sp == 0 & int_ri == 0 & int_rc == 0 & int_ic == 0) | intcb == 1 if brecha != .
	sum		ind_10
	display r(mean)
	
	merge 	1:1 cod_local using "$Input\2023\LE_AccSSBB_2023", keepusing(acc_agua* acc_des* acc_ene*) keep(1 3) nogen
	
	gen 	ind_11 = (acc_agua == "1. Red pública (agua potable)" | acc_agua == "2. Pilón de uso público (agua potable)" | acc_agua == "3. Camión cisterna u otro similar") ///
			if acc_agua != "" & acc_agua != "9. Sin información" & finfo != 13 & matricula != 0		
	sum		ind_11
	display r(mean)
	
	gen 	ind_12 = (acc_des == "1. Red pública" | acc_des == "3. Tanque séptico" | acc_des == "4. Biodigestor" | acc_des == "6. Unidades Básicas de Saneamiento de compostera (U.B.S -C)") 	///
			if acc_des != "" & acc_des != "9. Sin información" & finfo != 13 & matricula != 0		
	sum		ind_12
	display r(mean)
	
	gen 	ind_13 = (acc_ene == "1. Red pública (De una empresa distribuidora de energía eléctrica)" | acc_ene == "5. Panel solar" | acc_ene == "6. Energía Eólica")		///
			if acc_ene != "" & acc_ene != "9. Sin información" & finfo != 13 & matricula != 0		
	sum		ind_13
	display r(mean)
	
	gen 	ind_14 = (ind_11 == 1 & ind_12 == 1 & ind_13 == 1) if ind_11 != . & ind_12 != . & ind_13 != . & finfo != 13 & matricula != 0		
	sum		ind_14
	display r(mean)		
	
	gen 	ind_15 = (areaamp == 0 | areaamp == .) | intcb == 1 if brecha != . & finfo != 10 & (((finfo_ini != 5 & alumtot > 0 & alumtot != .) | (finfo == 5 & alumtot != .)) | intcb == 1) 					
	sum		ind_15
	display r(mean)	
	
	gen 	ind_16 = ((acc_ir == 0 | acc_ir == .) & (acc_rr1 == 0 | acc_rr1 == .) & (acc_rr2_5 == 0 | acc_rr2_5 == .) & (acc_ar == 0 | acc_ar == .)) | intcb == 1		///
			if brecha != . & finfo != 10 & (((finfo_ini != 5 & alumtot > 0 & alumtot != .) | (finfo == 5 & alumtot != .)) | intcb == 1) 					
	sum		ind_16
	display r(mean)	
	
	gen 	ind_17 = ((area_ene1 == 0 | area_ene1 == .) & (area_ene2 == 0 | area_ene2 == .) & (area_ene3 == 0 | area_ene3 == .) & (area_ene4 == 0 | area_ene4 == .)) | intcb == 1		///
			if brecha != . & finfo != 10 & ((area_ene1 != . | area_ene2 != . | area_ene3 != . | area_ene4 != .) | intcb == 1) 					
	sum		ind_17
	display r(mean)		

	
	/*_____________________________________________________________________
	|                                                                      |
	|            		CÁLCULO DE INDICADORES 2022		                   |
	|_____________________________________________________________________*/
	
	use 	"$Input2\LE_SPNIE_2022.dta", clear
	
	gen 	ind_11 = (acc_agua == "1. Red pública (agua potable)" | acc_agua == "2. Pilón de uso público (agua potable)" | acc_agua == "3. Camión cisterna u otro similar") ///
			if acc_agua != "" & acc_agua != "8. Sin información" & finfo != 12 & matricula != 0		
	sum		ind_11
	display r(mean)
	
	gen 	ind_12 = (acc_des == "01. Red pública" | acc_des == "03. Tanque séptico" | acc_des == "04. Pozo percolador" | acc_des ==  "06. Biodigestor") 					///
			if acc_des != "" & acc_des != "10. Sin información" & finfo != 12 & matricula != 0		
	sum		ind_12
	display r(mean)
	
	gen 	ind_13 = (acc_ene == "1. Red pública (de una empresa distribuidora de energía eléctrica)" | acc_ene == "5. Panel solar" | acc_ene == "6. Energía eólica")		///
			if acc_ene != "" & acc_ene != "9. Sin información" & finfo != 12 & matricula != 0		
	sum		ind_13
	display r(mean)
	
	gen 	ind_14 = (ind_11 == 1 & ind_12 == 1 & ind_13 == 1) if ind_11 != . & ind_12 != . & ind_13 != . & finfo != 12 & matricula != 0		
	sum		ind_14
	display r(mean)	
	
	gen 	ind_15 = (areaamp == 0 | areaamp == .) | intcb == 1 if brecha != . & finfo != 9 & (((finfo_ini != 4 & alumtot > 0 & alumtot != .) | (finfo == 4 & alumtot != .)) | intcb == 1) 					
	sum		ind_15
	display r(mean)
	
	
	/*_____________________________________________________________________
	|                                                                      |
	|            		CÁLCULO DE INDICADORES 2021		                   |
	|_____________________________________________________________________*/
	
	use 	"$Input2\LE_SPNIE_2021.dta", clear
	
	gen 	ind_11 = (acc_agua == "1. Red pública (agua potable)" | acc_agua == "2. Pilón de uso público (agua potable)" | acc_agua == "3. Camión cisterna u otro similar") ///
			if acc_agua != "" & acc_agua != "8. Sin información" & finfo != 10 & matricula != 0		
	sum		ind_11
	display r(mean)
					   
	gen 	ind_12 = (acc_des == "1. Red pública" | acc_des == "3. Tanque séptico" | acc_des == "4. Pozo percolador" | acc_des ==  "6. Biodigestor") 						///
			if acc_des != "" & acc_des != "9. Sin información" & finfo != 10 & matricula != 0		
	sum		ind_12
	display r(mean)

	gen 	ind_13 = (acc_ene == "1. Red pública (De una empresa distribuidora de energía eléctrica)" | acc_ene == "5. Panel solar" | acc_ene == "6. Energía eólica") 		///
			if acc_ene != "" & acc_ene != "9. Sin información" & finfo != 10 & matricula != 0		
	sum		ind_13
	display r(mean)
	
	gen 	ind_14 = (ind_11 == 1 & ind_12 == 1 & ind_13 == 1) if ind_11 != . & ind_12 != . & ind_13 != . & finfo != 10 & matricula != 0		
	sum		ind_14
	display r(mean)		
	
	gen 	ind_15 = (areaamp == 0 | areaamp == .) | intcb == 1 if brecha != . & finfo != 8 & ((alumtot > 0 & alumtot != .) | intcb == 1) 					
	sum		ind_15
	display r(mean)
	
	/*_____________________________________________________________________
	|                                                                      |
	|            		CÁLCULO DE INDICADORES 2020		                   |
	|_____________________________________________________________________*/
	
	use 	"$Input2\LE_SPNIE_2020.dta", clear
	
	gen 	ind_11 = (acc_agua == "1. Red pública" | acc_agua == "2. Pilón de uso público" | acc_agua == "3. Camión-cisterna u otro similar") 			///
			if acc_agua != "" & acc_agua != "8. Sin información" & finfo != 10 & matricula != 0 & acc_agua_finfo != "SE2019"				//  No se usa SE2019 pues tiene muchas observaciones con Red pública y en otros años no hay esa fuente.		
	sum		ind_11
	display r(mean)
					   
	gen 	ind_12 = (acc_des == "1. Red pública" | acc_des == "3. Pozo séptico")  if acc_des != "" & acc_des != "7. Sin información" & finfo != 10 & matricula != 0 & acc_des_finfo != "SE2019"			
	sum		ind_12																														//  No se usa SE2019 pues tiene muchas observaciones con Red pública y en otros años no hay esa fuente.
	display r(mean)
	
	gen 	ind_13 = (acc_ene == "1. Red pública" | acc_ene == "3. Panel solar") if acc_ene != "" & acc_ene != "6. Sin información" & finfo != 10 & matricula != 0		
	sum		ind_13
	display r(mean)
	
	gen 	ind_14 = (ind_11 == 1 & ind_12 == 1 & ind_13 == 1) if ind_11 != . & ind_12 != . & ind_13 != . & finfo != 10 & matricula != 0		
	sum		ind_14
	display r(mean)	
	
	gen 	ind_15 = (areaamp == 0 | areaamp == .) | intcb == 1 if brecha != . & finfo != 8 & ((alumtot > 0 & alumtot != .) | intcb == 1) 					
	sum		ind_15
	display r(mean)
	
	/*_____________________________________________________________________
	|                                                                      |
	|            		CÁLCULO DE INDICADORES 2019		                   |
	|_____________________________________________________________________*/
	
	use 	"$Input2\LE_SPNIE_2019.dta", clear
	
	gen 	ind_11 = (acc_agua == "1. Red pública" | acc_agua == "2. Pilón de uso público" | acc_agua == "3. Camión-cisterna u otro similar") 			///
			if acc_agua != "" & acc_agua != "8. Sin información" & matricula != 0 & acc_agua_finfo != "SE2019"			//  No se usa SE2019 pues tiene muchas observaciones con Red pública y en otros años no hay esa fuente.
	sum		ind_11
	display r(mean)
					   
	gen 	ind_12 = (acc_des == "1. Red pública" | acc_des == "3. Pozo séptico") if acc_des != "" & acc_des != "7. Sin información" & matricula != 0	& acc_des_finfo != "SE2019"	 	
	sum		ind_12																													//  No se usa SE2019 pues tiene muchas observaciones con Red pública y en otros años no hay esa fuente.
	display r(mean)
	
	gen 	ind_13 = (acc_ene == "1. Red pública" | acc_ene == "3. Panel Solar") if acc_ene != "" & acc_ene != "6. Sin información" & matricula != 0		
	sum		ind_13
	display r(mean)
	
	gen 	ind_14 = (ind_11 == 1 & ind_12 == 1 & ind_13 == 1) if ind_11 != . & ind_12 != . & ind_13 != . & finfo != 10 & matricula != 0		
	sum		ind_14
	display r(mean)	
	
	gen 	ind_15 = (areaamp == 0 | areaamp == .) | intcb == 1 if brecha != . & finfo != 8 & ((alumtot > 0 & alumtot != .) | intcb == 1) 					
	sum		ind_15
	display r(mean)
	
	/*_____________________________________________________________________
	|                                                                      |
	|            		CÁLCULO DE INDICADORES 2018		                   |
	|_____________________________________________________________________*/
	
	use 	"$Input2\LE_SPNIE_2018.dta", clear
	
	gen 	ind_15 = (areaamp == 0 | areaamp == .) | intcb == 1 if brecha != . & finfo != 8 & ((alumtot > 0 & alumtot != .) | intcb == 1) 					
	sum		ind_15
	display r(mean)
	
	/*_____________________________________________________________________
	|                                                                      |
	|           	 CÁLCULO DE INDICADORES PNIE (2015-2017) 	           |
	|_____________________________________________________________________*/
	
	use 	"$Input2\LE_SPNIE_PNIE.dta", clear
	
	gen 	ind_14 = (ind_121 == 0 & ind_122 == 0 & ind_124 == 0) if ind_121 != . & ind_122 != . & ind_124 != .		
	sum		ind_14
	display r(mean)	
	
	gen 	ind_15 = (area_Sp43 == 0 | area_Sp43 == .) if area_Sp43 != . & alumnos > 0 & alumnos != . 				
	sum		ind_15
	display r(mean)