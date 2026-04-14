	/*___________________________________________________________
	|	                                                      	|
	|	MINEDU - Cálculo de Brecha CIE (optimizado) 			|
	|	Actualizado: 29/07/2024		           					|
	|___________________________________________________________*/

	/*_____________________________________________________________________
	|                                                                      |
	|                               PRÓLOGO                                |
	|_____________________________________________________________________*/
	
	clear all
	
	global 	Input	=   "C:\CalcBrPr2211\raw\PNIE_DATA" 					// Carpeta Bases de Trabajo
	global	NoCens	=	"C:\CalcBrPr2211\raw\No Censados"					// Carpeta No Censados
	global	Raw		= 	"C:\CalcBrPr2211\raw"								// Carpeta Raw (2022-11)
	global	Raw2	= 	"C:\CalcBrPr2412\raw"								// Carpeta Raw (2024-12)
	global 	Final  	=   "C:\CalcBrPr2507\input"								// Carpeta Final
	cd 		"$Final"

	set 	more off 
	set 	varabbrev off
	set 	type double
	set 	seed 339487731
	set 	excelxlsxlargefile on
	
	/*_____________________________________________________________________
	|                                                                      |
	|            		PREPARACIÓN INICIAL - NO CENSADOS                  |
	|_____________________________________________________________________*/
	
	use 	"$NoCens\NoCensados_ficha", clear
	
	rename 	(numpredios numedificaciones areatechadadelprimerpiso numpisos 	///
			ejecutorobra antigedad sistemaestpred) 	///
			(NmeroPredios Nmerototaledificaciones readetecho Nmerodepisos 	///
			EntidadConstructora Fecha SistemaEstructuralCIE)
	
	replace topografia = 4 if topografia == .	// Caso más conservador. Si no tiene registro de topografía, se registra la topografía más accidentada.
	gen 	pendiente = 1 if topografia == 1 | topografia == 2 
    replace pendiente = 2 if topografia == 3 | topografia == 4
	
	replace Nmerodepisos = 1 if Nmerodepisos == 0	// Caso particular (1).
	recast 	double readetecho
	replace readetecho = 10 if readetecho < 10
	replace readetecho = 2500 if readetecho > 2500 & readetecho != .
	replace readetecho = round(readetecho,0.01)
	gen 	Areatotalconstruida = readetecho * Nmerodepisos
	drop 	if Areatotalconstruida == .
	replace NmeroPredios = 1 if NmeroPredios == . & EntidadConstructora != . & SistemaEstructuralCIE != . & Fecha != . 
	replace Nmerototaledificaciones = 1 if Nmerototaledificaciones == . & EntidadConstructora != . & SistemaEstructuralCIE != . & Fecha != . 
	
	gen 	SistemaEstructural = "780-POST" if (SistemaEstructuralCIE == 1 | SistemaEstructuralCIE == 2) ///
			& Fecha == 3 & (EntidadConstructora == 1 | EntidadConstructora == 2)
	replace SistemaEstructural = "780-PRE" if SistemaEstructural == "" & (SistemaEstructuralCIE == 1 | SistemaEstructuralCIE == 2) ///
			& Fecha == 2 & (EntidadConstructora == 1 | EntidadConstructora == 2)
	replace	SistemaEstructural = "PCM" if SistemaEstructural == "" & (SistemaEstructuralCIE == 1 | SistemaEstructuralCIE == 2) ///
			& (((EntidadConstructora == 1 | EntidadConstructora == 2) & Fecha == 1) | (EntidadConstructora == 3 | EntidadConstructora == 4 | EntidadConstructora == 5))
	replace	SistemaEstructural = "EA" if SistemaEstructural == "" & SistemaEstructuralCIE == 3
	replace	SistemaEstructural = "M" if SistemaEstructural == "" & SistemaEstructuralCIE == 4
	replace SistemaEstructural = "A" if SistemaEstructural == "" & SistemaEstructuralCIE == 5
	replace SistemaEstructural = "ASC" if SistemaEstructural == "" & SistemaEstructuralCIE == 6
	replace SistemaEstructural = "P" if SistemaEstructural == "" & (SistemaEstructuralCIE == 7 | (SistemaEstructuralCIE == 8 & (Fecha == 1 | Fecha == 2)))
	replace SistemaEstructural = "PROV" if SistemaEstructural == "" & SistemaEstructuralCIE == 8 & Fecha == 3
	
	merge 	m:1 id_local using "LE_Matri", keepusing(id_local dareacenso codgeo act)
	keep 	if _merge == 3
	drop 	if act == 0
	drop 	act _merge		// Mantener locales públicos en padrón actual.

	merge	m:1 id_local using "$Raw\Data_Completa", keepusing(id_local)
	keep	if _merge == 1
	drop 	_merge		// Mantener locales que no se repiten en el CIE.
	
	rename codgeo ubigeo
	merge 	m:1 ubigeo using "ZonaSis", keepusing(zonaamenaza)
	drop 	if _merge == 2
	drop 	_merge
	rename ubigeo codgeo
	
	merge	m:1 id_local using "$NoCens\CE15_escenarios"
	drop 	if _merge == 2
	drop	_merge
	recast double escenario
	
	rename	codgeo UBIGEO
	merge	m:1 UBIGEO using "$Raw\ZonasBioclimaticas", keepusing(ZONABIOCLIMTICA)
	drop 	if _merge == 2
	drop	_merge
	label 	define Zonabio 1 "Desértico marino" 2 "Desértico" 3 "Interandino bajo" ///
			4 "Mesoandino" 5 "Alto andino" 6 "Nevado" 7 "Ceja de montaña" ///
			8 "Subtropical húmedo" 9 "Tropical húmedo"
	encode 	ZONABIOCLIMTICA, gen(Clima) label(Zonabio)
	replace Clima = 5 if Clima == .								// Caso moderado
			
	rename 	(zonaamenaza escenario) (ZonaAmenazaSegnNorma Escenarios)
	gen		NC = 1
	
	encode 	SistemaEstructural, gen(se)
	egen 	_aux1 = count(se), by (id_local)
	egen 	_aux2 = count(id_local), by(id_local)
	drop 	if _aux1 != _aux2
	drop	_*
	
	sort	id_local NmeroPredios Nmerototaledificaciones
	compress
	save	"cie\Edif_NC_CIE.dta", replace

		
	/*_____________________________________________________________________
	|                                                                      |
	|            				CÁLCULO DE ÁREAS                           |
	|_____________________________________________________________________*/	
	
	use 	"$Input\ANEXO 1", clear
	
	* PARCHE: Actualización de información de edificaciones de 41 locales (APP) - Inspección DIPLAN 
	*------------------------------------------------------------------------------------------------
	merge 	1:1 id_local NmeroPredios Nmerototaledificaciones using "cie\Edif_ParcheAPP41", keepusing(Zona ZonaAmenazaSegnNorma readetecho ///
			Nmerodepisos EntidadConstructora Fecha SistemaEstructuralCIE SistemaEstructural act_SINAD act_fuente act_usuario IAPP41) update replace
	egen	_aux1 = max(IAPP41), by(id_local)
	drop	if _aux1 == 1 & IAPP41 == .
	drop	_aux1 _merge
	merge 	1:1 id_local NmeroPredios Nmerototaledificaciones using "cie\Edif_ParcheAPP41", keepusing(Departamento Provincia Escenarios Clima ///
			Longitud Latitud Altitud UsosValidos SE_SISMO) update
	drop	_merge
	
	* PARCHE: Actualización de información de edificaciones de 3 locales - Inspección PRONIED-Tacna
	*------------------------------------------------------------------------------------------------
	merge 	1:1 id_local NmeroPredios Nmerototaledificaciones using "cie\Edif_ParchePRONIED", keepusing(Zona ZonaAmenazaSegnNorma readetecho ///
			Nmerodepisos EntidadConstructora Fecha SistemaEstructuralCIE SistemaEstructural act_SINAD act_fuente act_usuario IPRONIED) update replace
	egen	_aux1 = max(IPRONIED), by(id_local)
	drop	if _aux1 == 1 & IPRONIED == .
	drop	_aux1 _merge
	merge 	1:1 id_local NmeroPredios Nmerototaledificaciones using "cie\Edif_ParchePRONIED", keepusing(Departamento Provincia Escenarios Clima ///
			Longitud Latitud Altitud UsosValidos SE_SISMO) update
	drop	_merge	
	
	* PARCHE: Actualización de información de edificaciones de 38 locales - Inspección DRELM
	*----------------------------------------------------------------------------------------
	merge 	1:1 id_local NmeroPredios Nmerototaledificaciones using "cie\Edif_ParcheDRELM", keepusing(Zona ZonaAmenazaSegnNorma readetecho ///
			Nmerodepisos EntidadConstructora Fecha SistemaEstructuralCIE SistemaEstructural act_SINAD act_fuente act_usuario IDRELM NC) update replace
	drop 	if NC == 1
	egen	_aux1 = max(IDRELM), by(id_local)
	drop	if _aux1 == 1 & IDRELM == .
	drop	_aux1 _merge NC
	merge 	1:1 id_local NmeroPredios Nmerototaledificaciones using "cie\Edif_ParcheDRELM", keepusing(Departamento Provincia Escenarios Clima ///
			Longitud Latitud Altitud UsosValidos SE_SISMO NC) update
	drop 	if NC == 1
	drop	_merge NC
	
	* Actualización de zonas y UBIGEO
	*---------------------------------
	merge 	m:1 id_local using "LE_Matri", keepusing(dareacenso codgeo act)
	keep 	if _merge == 3
	drop	if act == 0
	drop	act _merge
	replace dareacenso = Zona if dareacenso == ""
	rename	codgeo UBIGEO
		
	* Topografía
	*---------------
	merge 	m:1 id_local using "$Input\topografia.dta", keepusing(p2_b_1_topo)
	drop 	if _merge ==2
	drop	 _merge
	rename	p2_b_1_topo topografia
	replace topografia = 4 if topografia == .	// Caso más conservador. Si no tiene registro de topografía, se registra la topografía más accidentada.
	gen		pendiente = 1 if topografia == 1 | topografia == 2
	replace pendiente = 2 if topografia == 3 | topografia == 4

	label 	define EntidadConstructora 1 "gobierno nacional / proyecto especial" 2 "gobierno regional / local" ///
			3 "apafa / autoconstrucción" 4 "entidades cooperantes / ong’s" 5 "empresa privada"
	label	define SistemaEstructuralCIE 1 "pórticos de concreto armado y/o muros de albañilería (dual)" ///
			2 "albañilería confinada o armada" 3 "estructura de acero" 4 "madera (normalizada)" ///
			5 "adobe" 6 "albañilería sin confinar" 7 "construcciones precarías (triplay, quincha, tapial, similares)" ///
			8 "aulas provisionales"			
	encode 	EntidadConstructora, gen(EntidadConstructora1) label(EntidadConstructora)
	encode	SistemaEstructuralCIE, gen(SistemaEstructuralCIE1) label(SistemaEstructuralCIE)
	encode	Fecha, gen(Fecha1)
	drop 	EntidadConstructora Fecha SistemaEstructuralCIE
	rename 	(EntidadConstructora1 Fecha1 SistemaEstructuralCIE1) (EntidadConstructora Fecha SistemaEstructuralCIE)
	
	* Corregir áreas, acotar áreas a [10; 2,500] y redondear a 2 decimales máx.
	* --------------------------------------------------------------------------
	rename	(NmeroPredios Nmerototaledificaciones) (nro_pred nro_ed)
	merge 	1:1 id_local nro_pred nro_ed using "$Input\P6_1", ///
			keepusing(p6_1_3 p6_3_1 p6_3_1a p6_3_2 p6_3_2b p6_3_3 p6_3_3a p6_5_1 p6_5_1a)	// Se agregan variables de energía y drenaje pluvial.
	drop 	if _merge == 2
	drop	_merge
	
	gen 	aux1 = round(p6_1_3, 1)
	gen		aux2 = p6_1_3 != aux1
	recast 	double readetecho
	replace readetecho = p6_1_3 if aux2 == 1 & IAPP41 != 1 & IPRONIED != 1 & IDRELM != 1
	
	replace Nmerodepisos = 1 if Nmerodepisos < 1 | Nmerodepisos == .
	replace Nmerodepisos = 6 if Nmerodepisos > 6 & Nmerodepisos != .
	replace readetecho = 10 if readetecho < 10
	replace readetecho = 2500 if readetecho > 2500 & readetecho != .
	replace readetecho = round(readetecho,.01)
	drop	Areatotalconstruida
	gen 	Areatotalconstruida = readetecho*Nmerodepisos
	
	drop 	aux1 aux2 p6_1_3
	rename	(nro_pred nro_ed) (NmeroPredios Nmerototaledificaciones)
	
	* Anexar observaciones de No Censados
	*-------------------------------------
	append 	using "cie\Edif_NC_CIE"
	replace	NC = 0 if NC == .
	replace	IDRELM = 1 if id_local == 310743  // Actualización inspección DRELM en No Censados
	drop 	ZONABIOCLIMTICA
	
	* Definir variables iniciales
	* ---------------------------
	encode 	SistemaEstructural, gen(sistest)
	encode 	dareacenso, gen(zona)
	rename	(readetecho Areatotalconstruida UBIGEO) (areatechosuperior areatechadatotal ubigeo)
	
	merge 	m:1 ubigeo using "ZonaSis", keepusing(zonaamenaza)	// Obtener variable Zona de amenaza sísmica.
	drop 	if _merge == 2
	drop 	_merge
	label 	define zonaamenaza 1 "Amenaza baja" 2 "Amenaza intermedia" 3 "Amenaza alta" 4 "Amenaza muy alta"
	label 	values zonaamenaza zonaamenaza
	replace zonaamenaza = ZonaAmenazaSegnNorma if zonaamenaza == .
	drop	ZonaAmenazaSegnNorma
	
	rename 	ubigeo UBIGEO	
	merge	m:1 UBIGEO using "$Raw\ZonasBioclimaticas", keepusing(ZONABIOCLIMTICA)	// Obtener variable Clima.
	drop 	if _merge == 2
	drop	_merge
	label 	define clima 1 "Desértico marino" 2 "Desértico" 3 "Interandino bajo" ///
			4 "Mesoandino" 5 "Altoandino" 6 "Nevado" 7 "Ceja de montaña" ///
			8 "Subtropical húmedo" 9 "Tropical húmedo"
	encode 	ZONABIOCLIMTICA, gen(clima) label(clima)
	replace clima = Clima if clima == .
	replace clima = 5 if clima == .				// Caso moderado (?).
	drop	Clima
		
	rename 	id_local cod_local
	merge 	m:1 cod_local using "LE_InfRie"	// LL.EE. con informe de riesgo, se cambia el sistema estructural PNIE a precario (regla interna).
	drop 	if _merge == 2
	gen 	infriesgo = _merge == 3
	replace sistest = 8 if infriesgo == 1
	drop 	_merge
	tab 	sistest infriesgo
	rename 	cod_local id_local
	
	* Definir escenarios
	* -------------------
	merge 	m:1 id_local using "LE_Matri", keepusing(d_dpto d_prov d_dist cen_pob matri act)
	keep 	if _merge == 3
	drop	if act == 0
	drop	act _merge
	decode 	d_dpto, gen(d_dpto2)
	drop	d_dpto
	rename	d_dpto2 d_dpto
	
	merge	m:1 id_local using "$Raw\LE_TiempoUGEL"
	drop 	if _merge == 2
	drop	 _merge
	
	merge	m:1 d_dpto d_prov d_dist cen_pob using "$Raw\Cpobl"
	drop 	if _merge == 2
	drop	 _merge
	
	merge 	m:1 UBIGEO using "$Raw2\Pobciud", keepusing(ciudad_inei pob_ciud_cap_inei Pob_2024 superficie)
	drop 	if _merge == 2
	drop	 _merge	
	
	gen		densidad = Pob_2024 / superficie
	egen	matdisturb = sum(matri) if zona == 2, by(UBIGEO)
	egen	matcp = sum(matri), by(cen_pob)
	
	gen		escenario = 1 if zona == 2 & (ciudad_inei == "LIMA METROPOLITANA" | ciudad_inei == "AREQUIPA")
	replace	escenario = 2 if escenario == . & zona == 2 & pob_ciud_cap_inei != .
	replace	escenario = 3 if escenario == . & zona == 2 & (ciudad_inei != "" | capital == 2 | tmin_ugel < 60 | (matdisturb > 200 & matdisturb != .)) 
	replace	escenario = 4 if escenario == . & ((zona == 1 & (ciudad_inei != "" | capital == 2 | capital == 3)) | (tmin_ugel < 300 | (matri >= 100 & matri != .) | (matcp > 300 & matcp != .))) 
	replace	escenario = 5 if (escenario > 2 & (tmin_ugel > 300 & tmin_ugel != .)) | (escenario == . & (densidad < 100 | matri < 100))
	replace escenario = Escenarios if escenario == .
	drop	Escenarios	
	
	replace escenario = 3 if escenario == . & zona == 2		// Escenario con costos más altos de zona urbana.
	replace escenario = 5 if escenario == . & zona == 1		// Escenario con costos más altos de zona rural.
	
	label 	define escenario 1 "Grandes ciudades" 2 "Ciudades intermedias" 3 "Centros urbanos" 4 "Pueblos conectados" 5 "Comunidades dispersas"
	label 	values escenario escenario
	rename	UBIGEO codgeo
	
	* Cálculo de áreas según tipo de intervención
	*---------------------------------------------	
	gen 	areadem = areatechadatotal if (sistest == 3 | sistest == 4 | sistest == 5 | sistest == 7 | sistest == 8 | sistest == 10 | (sistest == 9 & zona == 1)) & zonaamenaza != 1
	egen	areadem_le = sum(areadem), by(id_local)
	egen	areatech_le = sum(areatechadatotal), by(id_local)
	gen		ratiodem = areadem_le / areatech_le
	
	gen		areasust = areadem if ratiodem >= 0.7 | (ratiodem < 0.7 & zonaamenaza != 2)
	gen		areari = areatechadatotal if areadem == . & (sistest == 2 | (sistest == 9 & zona == 2)) & zonaamenaza != 1
	gen 	arearc = areatechadatotal if areadem == . & areari == . & sistest == 6
	gen		areaic1 = areadem if ratiodem < 0.7 & zonaamenaza == 2
	gen		areaic2 = areatechadatotal if areadem == . & areari == . & arearc == . & sistest != 1 & zonaamenaza == 1
	
	* Cálculo de áreas para calidad de energía
	*------------------------------------------
	gen		area_ce1 = areatechadatotal if ratiodem < 0.7 & areasust == . & p6_3_1 != 1	& NC != 1														// Requiere instalaciones eléctricas interiores.
	gen		area_ce2 = areatechadatotal if ratiodem < 0.7 & areasust == . & p6_3_1 == 1 & p6_3_1a != 1 & p6_3_1a != 2 & NC != 1							// Requiere canalizar circuitos elécticos.
	gen		area_ce3 = areatechadatotal if ratiodem < 0.7 & areasust == . & p6_3_1 == 1 & (p6_3_2 != 1 | p6_3_2b != 1) & NC != 1						// Requiere nuevo tablero o gabinete.
	gen		area_ce4 = areatechadatotal if ratiodem < 0.7 & areasust == . & p6_3_1 == 1 & (p6_3_3 != 1)	& NC != 1										// Requiere sistema de puesta a tierra.
	gen		area_ce5 = areatechadatotal if ratiodem < 0.7 & areasust == . & p6_3_1 == 1 & (p6_3_3 == 1 & (p6_3_3a < 2013 | p6_3_3a == .)) & NC != 1		// Requiere nuevo sistema de puesta a tierra.
	
	* Cálculo de metros lineales de canaletas aéreas y bajadas pluviales 
	*--------------------------------------------------------------------
	gen		ca_reh = 2 * sqrt(areatechosuperior) if clima >= 2 & clima <= 9 & p6_5_1 == 1 & p6_5_1a == 2 & NC != 1						// Rehabilitación de canaletas aéreas.
	gen		ca_sus = 2 * sqrt(areatechosuperior) if clima >= 2 & clima <= 9 & p6_5_1 == 1 & (p6_5_1a == 3 | p6_5_1a == .) & NC != 1		// Sustitución de canaletas aéreas.
	gen		ca_imp = 2 * sqrt(areatechosuperior) if clima >= 2 & clima <= 9 & p6_5_1 != 1 & NC != 1										// Implementación de canaletas aéreas.
	
	gen		bp_reh = 2 * 2.8 * Nmerodepisos if clima >= 2 & clima <= 9 & p6_5_1 == 1 & p6_5_1a == 2 & NC != 1							// Rehabilitación de bajadas pluviales.
	gen		bp_sus = 2 * 2.8 * Nmerodepisos if clima >= 2 & clima <= 9 & p6_5_1 == 1 & (p6_5_1a == 3 | p6_5_1a == .) & NC != 1			// Sustitución de bajadas pluviales.
	gen		bp_imp = 2 * 2.8 * Nmerodepisos if clima >= 2 & clima <= 9 & p6_5_1 != 1 & NC != 1											// Implementación de bajadas pluviales.
	
	compress
	save 	"cie\Edif_Infra_CIE.dta", replace
	
	* Colapso de base a locales y cálculo de indicadores
	*----------------------------------------------------
	use 	"cie\Edif_Infra_CIE", clear
	
	collapse 	(sum) areadem areasust areari arearc areaic1 areaic2 areatechadatotal areatechosuperior area_ce* ca_* bp_* ///
				(max) ratiodem zonaamenaza clima escenario pendiente topografia NC zona IAPP41 IPRONIED IDRELM 	///
				(firstnm) codgeo act_* (count) Nmerototaledificaciones, by(id_local NmeroPredios)		
	collapse 	(sum) areadem areasust areari arearc areaic1 areaic2 areatechadatotal areatechosuperior area_ce* ca_* bp_* Nmerototaledificaciones ///
				(max) ratiodem zonaamenaza clima escenario pendiente topografia NC zona	IAPP41 IPRONIED IDRELM  ///
				(firstnm) codgeo act_* (count) NmeroPredios, by(id_local)
	
	foreach v of varlist area_ce* ca_* bp_* {
		replace `v' = . if NC == 1
	}
	
	gen		int_st = areasust > 0  & ratiodem >= 0.7
	gen		int_sp = areasust > 0 & ratiodem < 0.7  
	gen		int_ri = areari > 0 & ratiodem < 0.7
	gen		int_rc = arearc > 0 & ratiodem < 0.7
	gen		int_ic = (areaic1 > 0 | areaic2 > 0) & ratiodem < 0.7
	
	egen 	areariesgo = rowtotal(areasust areari arearc areaic1 areaic2), missing
	replace areariesgo = areadem if areariesgo < areadem					// No debería suceder.
	replace areariesgo = areatechadatotal if areatechadatotal < areariesgo	// No debería suceder.
	
	gen 	ratioriesgo = areariesgo / areatechadatotal
	replace ratioriesgo = 1 if ratioriesgo > 1 & ratioriesgo != .			// No debería suceder.
	replace ratiodem = ratioriesgo if ratioriesgo < ratiodem				// No debería suceder.
	
	label 	values zonaamenaza zonaamenaza
	label 	values clima clima
	label 	values escenario escenario
	label	values zona zona
	
	compress
	save 	"cie\LE_Infra_CIE.dta", replace
	
	/*_____________________________________________________________________
	|                                                                      |
	|            	INTERVENCIÓN DE CERCOS PERIMÉTRICOS 	               |
	|_____________________________________________________________________*/
	
	use 	"$Input\P4_2N", clear 
	
	merge 	m:1 id_local using "cie\LE_Infra_CIE", keepusing(id_local zona)
	keep 	if _merge == 3
	drop 	_merge		// Mantener locales que tienen información del estado de la infraestructura.
	
	replace p4_2_1e_estcons = 4 if p4_2_1e_estcons == .
	
	gen		cp = p4_2_1c_cerco == 1
	gen		cp_n = p4_2_1b_longtramo if cp == 0 & zona == 2 
	gen		cp_dr = p4_2_1b_longtramo if cp == 1 & zona == 2 ///
			& ((p4_2_1d_estruc == 1 & p4_2_1e_estcons == 4) | (p4_2_1d_estruc != 1))
	gen		cp_mm = p4_2_1b_longtramo if cp == 1 & p4_2_1d_estruc == 1 & p4_2_1e_estcons == 3
	gen		cp_mb = p4_2_1b_longtramo if cp == 1 & p4_2_1d_estruc == 1 & p4_2_1e_estcons == 2
	gen		cp_nv =	p4_2_1b_longtramo if cp == 0 & zona == 1 
	gen		cp_drv = p4_2_1b_longtramo if cp == 1 & zona == 1 ///
			& ((p4_2_1d_estruc == 1 & p4_2_1e_estcons == 4) | (p4_2_1d_estruc != 1))
	gen		cp_si = p4_2_1b_longtramo if cp == 1 & p4_2_1d_estruc == 1 & p4_2_1e_estcons == 1
	rename	p4_2_1b_longtramo tramo
	
	collapse 	(sum) cp_* tramo cp (max) zona (count) nro_pred, by(id_local)
	drop 	if tramo == 0
	
	compress
	save 	"cie\LE_Cerco_CIE.dta", replace
	
	/*_____________________________________________________________________
	|                                                                      |
	|            	CÁLCULO DE ÁREAS DE TERRENO,						   | 
	|				ALUMNOS POR TURNO Y POR NIVEL	           			   |
	|_____________________________________________________________________*/
		 	
	* Cálculo de áreas de terreno
	*----------------------------------------------------------------------------
	use 	"$Input\P1_B_3N", clear
	append	using "$Input\P1_C"
	
	collapse (sum) p1_b_3_9_at_local p1_c_17_at_local, by(id_local)
	
	merge 	1:1 id_local using "cie\LE_Infra_CIE", keepusing(id_local zona areatechosuperior)
	keep 	if _merge == 3
	drop 	_merge		// Mantener locales que tienen información del estado de la infraestructura.
	
	egen	areaterr = rowtotal(p1_b_3_9_at_local p1_c_17_at_local), missing
	replace areaterr = areatechosuperior if areatechosuperior > areaterr
	
	compress
	save	"cie\LE_Aterr_CIE.dta", replace
			
	* Fusión de bases de IIEE con Anexos y cálculo de alumnos por turno y por nivel
	*-------------------------------------------------------------------------------
	use 	"$Input\P1_A_2_9N", clear
	merge 	m:1	id_local p1_a_2_nroie p1_a_2_9_nrocmod using "$Input\P1_A_2_8N", keepusing(p1_a_2_9c_nivel)
	drop 	if	_merge == 2
	drop	_merge
	
	append	using "$Input\P1_A_2_8N"
	drop 	if id_local == 435272 &  p1_a_2_9c_nivel == 11 // Caso particular (1).
	
	egen	alummax = rowmax(p1_a_2_9g_t1_talu p1_a_2_9i_t2_talu p1_a_2_9k_t3_talu)
	egen	alumtot = rowtotal(p1_a_2_9g_t1_talu p1_a_2_9i_t2_talu p1_a_2_9k_t3_talu)
	gen 	alum1_t1 = p1_a_2_9g_t1_talu if p1_a_2_9c_nivel == 1 | p1_a_2_9c_nivel== 2 | p1_a_2_9c_nivel == 3
	gen 	alum1_t2 = p1_a_2_9i_t2_talu if p1_a_2_9c_nivel == 1 | p1_a_2_9c_nivel== 2 | p1_a_2_9c_nivel == 3
	gen 	alum1_t3 = p1_a_2_9k_t3_talu if p1_a_2_9c_nivel == 1 | p1_a_2_9c_nivel== 2 | p1_a_2_9c_nivel == 3
		
	forvalues i = 2/9 {
			gen alum`i'_t1 = p1_a_2_9g_t1_talu if p1_a_2_9c_nivel == `i' + 2
			gen alum`i'_t2 = p1_a_2_9i_t2_talu if p1_a_2_9c_nivel == `i' + 2
			gen alum`i'_t3 = p1_a_2_9k_t3_talu if p1_a_2_9c_nivel == `i' + 2 
	}
		
	forvalues i = 1/2 {
			gen alum`i' = max(alum`i'_t1, alum`i'_t2, alum`i'_t3)
			egen alum`i'tot = rowtotal(alum`i'_t1 alum`i'_t2 alum`i'_t3)
	}
	egen 	alum3 = rowtotal(alum3_t1 alum3_t2 alum3_t3)
	gen		alum3max = max(alum3_t1, alum3_t2, alum3_t3)
	forvalues i = 4/9 {
			gen alum`i' = max(alum`i'_t1, alum`i'_t2, alum`i'_t3)
			egen alum`i'tot = rowtotal(alum`i'_t1 alum`i'_t2 alum`i'_t3)
	}
	drop	alum1_*	alum2_* alum3_* alum4_* alum5_* alum6_* alum7_* alum8_* alum9_*
	collapse	(sum) alum*, by (id_local)
	egen	alum = rowtotal(alum1 alum2 alum3 alum4 alum5 alum6 alum7 alum8 alum9)
	* DISCUTIR SI CÁLCULO DE MÁXIMO DE ALUMNOS TOMA EN CUENTA DISTINTAS IIEE DEL MISMO NIVEL Y DEL MISMO LOCAL.
	
	merge 	1:1 id_local using "cie\LE_Infra_CIE", keepusing(zona)
	keep 	if _merge == 3
	drop 	_merge		// Mantener locales que tienen información del estado de la infraestructura.
			
	* Acotar número de alumnos máximo y total por nivel a matrícula registrada.
	*------------------------------------------------------------------------------
	merge 	1:1 id_local using "LE_Matri", keepusing(matri m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO)
	drop 	if _merge == 2
	drop 	_merge	
	
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local niv = `niv' + 1
		if 	(`niv' != 3) count if alum`niv'tot  > `v'
		count if alum`niv'  > `v'
		if 	(`niv' == 3) count if alum`niv'max  > `v'
		if 	(`niv' != 3) table zona, stat(sum alum`niv'tot)
		if 	(`niv' == 3) table zona, stat(sum alum`niv')
		table zona, stat(sum `v')
	}
	
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local niv = `niv' + 1
		if 	(`niv' != 3)  replace 	alum`niv'tot = `v' if alum`niv'tot  > `v'
		replace 	alum`niv' = `v' if alum`niv'  > `v'
		if 	(`niv' == 3)  replace 	alum`niv'max = `v' if alum`niv'max  > `v'
	}
	
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local niv = `niv' + 1
		if 	(`niv' != 3) count if alum`niv'tot  > `v'
		count if alum`niv'  > `v'
		if 	(`niv' == 3) count if alum`niv'max  > `v'
		if 	(`niv' != 3) table zona, stat(sum alum`niv'tot)
		if 	(`niv' == 3) table zona, stat(sum alum`niv')
		table zona, stat(sum `v')
	}
	
	* Si el nivel solo tiene un turno, usar matrícula registrada.
	*--------------------------------------------------------------
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local niv = `niv' + 1
		if 	(`niv' != 3) count if alum`niv'tot  < `v' & alum`niv'tot == alum`niv' & alum`niv'tot != 0
		if 	(`niv' != 3) gen turno`niv'uni = 1 if alum`niv'tot == alum`niv' & alum`niv'tot != 0
		if 	(`niv' != 3) replace turno`niv'uni = 0 if alum`niv'tot > alum`niv' & alum`niv'tot != 0
		if 	(`niv' != 3) replace turno`niv'uni = 2 if alum`niv'tot < alum`niv' & alum`niv'tot != 0				// Corregir si aparece esta categoría en las variables.
		if 	(`niv' == 3) count if alum`niv' < `v' & alum`niv'max == alum`niv' & alum`niv' != 0
		if 	(`niv' == 3) gen turno`niv'uni = 1 if alum`niv'max == alum`niv' & alum`niv' != 0
		if 	(`niv' == 3) replace turno`niv'uni = 0 if alum`niv'max < alum`niv' & alum`niv' != 0
		if 	(`niv' == 3) replace turno`niv'uni = 2 if alum`niv'max > alum`niv' & alum`niv' != 0					// Corregir si aparece esta categoría en las variables.
	}
	
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local niv = `niv' + 1
		if 	(`niv' != 3)  replace 	alum`niv'tot = `v' 	if turno`niv'uni == 1
		replace 	alum`niv' = `v' 					if turno`niv'uni == 1
		if 	(`niv' == 3)  replace 	alum`niv'max = `v'  if turno`niv'uni == 1
	}
	
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local niv = `niv' + 1
		if 	(`niv' != 3) count if alum`niv'tot  < `v' & alum`niv'tot == alum`niv' & alum`niv'tot != 0
		if 	(`niv' == 3) count if alum`niv' < `v' & alum`niv'max == alum`niv' & alum`niv' != 0
		if 	(`niv' != 3) table zona, stat(sum alum`niv'tot)
		if 	(`niv' == 3) table zona, stat(sum alum`niv')
		table zona, stat(sum `v')
	}
	
	* Aumento de información de matrícula (supuesto: 1 turno en todos los niveles)
	*------------------------------------------------------------------------------	
	local 	niv = 0
	foreach v of varlist m_Inicial m_Primaria m_Secundaria m_EBA m_EBE m_ESFA m_IST m_ISP m_CETPRO {
		local niv = `niv' + 1
		if 	(`niv' != 3) replace alum`niv'tot = `v' if alum`niv'tot == 0
		replace alum`niv' = `v' if alum`niv' == 0
		if 	(`niv' == 3) replace alum`niv'max = `v' if alum`niv'max == 0
	}

	drop	alum alumtot alummax
	egen	alum = rowtotal(alum1 alum2 alum3 alum4 alum5 alum6 alum7 alum8 alum9)
	egen	alumtot = rowtotal(alum1tot alum2tot alum3 alum4tot alum5tot alum6tot alum7tot alum8tot alum9tot)
	egen	alummax = rowtotal(alum1 alum2 alum3max alum4 alum5 alum6 alum7 alum8 alum9)

	drop 	if alumtot == 0

	compress
	save	"cie\LE_Alum_CIE.dta", replace
	
	/*_____________________________________________________________________
	|                                                                      |
	|            	CÁLCULO DE ÁREAS PARA AMPLIACIÓN	 	               |
	|_____________________________________________________________________*/
	
	use		"cie\LE_Alum_CIE.dta", replace
	
	* Cálculo de áreas techadas mínimas
	*-----------------------------------	
	merge 	1:1 id_local using "cie\LE_Infra_CIE", keepusing(id_local zona areatechosuperior areatechadatotal codgeo)
	keep 	if _merge == 3
	drop 	_merge		// Mantener locales que tienen información del estado de la infraestructura.
	
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
	
	scalar	FPS = 0.7 	// Factor que considera si un local tiene primaria y secundaria para el cálculo de área mínima.
	
	forvalues i = 1/9 {
			gen areamin`i' = alum`i' * areaminu`i'
	}
	
	replace areamin2 = areamin2 * FPS if areamin3 > areamin2 & areamin2 != . & areamin3 != .
	replace areamin3 = areamin3 * FPS if areamin2 > areamin3 & areamin2 != . & areamin3 != .
	
	* Cálculo de áreas techadas reales y ratios de ampliación
	*----------------------------------------------------------
	merge 	1:1 id_local using "cie\LE_Aterr_CIE", keepusing(areaterr)
	drop 	if _merge == 2
	drop	_merge
	replace areaterr = areatechosuperior if areaterr == .
	
	drop 	if areaterr + 10 < areatechosuperior	// Eliminar inconsistencias (> 10 m2 = inconsistencia). DISCUTIR SI REVISAR ESTE SUPUESTO!
		
	forvalues i = 1/9 {
			gen areatech`i' = (alum`i' / alum) * areatechadatotal
			gen ramp`i' = (areamin`i' - areatech`i') / areatechadatotal if alum`i' > 0
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
	save 	"cie\LE_Amp_CIE.dta", replace

	/*_____________________________________________________________________
	|                                                                      |
	|       			IDONEIDAD DE SSHH Y BEBEDEROS				       |
	|_____________________________________________________________________*/
	
	use 	"$Input\P6_2_4N", clear
	
	gen 	amb1 = p6_2_4mod == 1 & (p6_2_4id == 1 | p6_2_4id == 2 | p6_2_4id == 3)
		
	forvalues i = 2/9 {
			gen amb`i' = p6_2_4mod == 1 & p6_2_4id == `i' + 2
	}
	
	collapse	(max) amb*, by(id_local nro_pred p5_ed_nro p5_nropiso p6_2_1)
	
	recast 	double p6_2_1
	merge	1:1 id_local nro_pred p5_ed_nro p5_nropiso p6_2_1 using "$Input\p6_2", ///
			keepusing(p6_2_3 p6_2_5 p6_2_12 p6_2_13 p6_2_14*)
	drop	if _merge == 1		// Mantener ambientes con información principal. No afecta a SSHH.
	
	* Cálculo de baños e inodoros
	*-----------------------------
	gen 	sshh = p6_2_3 == 1 & (p6_2_12 == 8 | p6_2_13 == 1 | p6_2_13 == 2)	// Número de baños en uso.
	
	egen	ino = rowtotal(p6_2_14_3 p6_2_14_4 p6_2_14_5 p6_2_14_6) if sshh == 1	// Número de inodoros en uso.
	egen	inon = rowtotal(p6_2_14_5 p6_2_14_6) if sshh == 1
	gen		ino5 = ino if p5_nropiso == 1 & amb5 == 1
	gen 	ino1 = inon if p5_nropiso == 1 & amb5 == 0 & amb1 == 1	// Para Inicial y EBE, solo SSHH de primer piso. Para Inicial, solo inodoros para niños.
	gen		ino2 = ino if amb5 == 0 & amb1 == 0 & amb2 == 1
	gen		ino3 = ino if amb5 == 0 & amb1 == 0 & amb2 == 0 & amb3 == 1
	gen		ino8 = ino if amb5 == 0 & amb1 == 0 & amb2 == 0 & amb3 == 0 & amb8 == 1
	gen		ino9 = ino if amb5 == 0 & amb1 == 0 & amb2 == 0 & amb3 == 0 & amb8 == 0 & amb9 == 1
	gen		ino7 = ino if amb5 == 0 & amb1 == 0 & amb2 == 0 & amb3 == 0 & amb8 == 0 & amb9 == 0 & amb7 == 1
	gen		ino4 = ino if amb5 == 0 & amb1 == 0 & amb2 == 0 & amb3 == 0 & amb8 == 0 & amb9 == 0 & amb7 == 0 & amb4 == 1
	gen 	ino6 = ino if amb5 == 0 & amb1 == 0 & amb2 == 0 & amb3 == 0 & amb8 == 0 & amb9 == 0 & amb7 == 0 & amb4 == 0 & amb6 == 1	// Número de inodoros por nivel.
	
	egen	_aux = rowtotal(amb*)
	gen		amb_comp = _aux > 1
	egen	amb_comp_le = sum(amb_comp), by(id_local)
	gen		excl = amb_comp_le == 0			// Local solo contiene ambientes exclusivos a un nivel.
	drop	amb_comp* _aux ino
	
	gen 	inod = p6_2_14a if p6_2_3 == 1 & ((amb1 == 0 & amb5 == 0) | p5_nropiso == 1 | excl == 0)	// Número de inodoros con función de descarga.
																										// Se excluyen ambientes de Inicial y EBE que no estén en primer piso en locales con ambientes exclusivos.
	gen		acc_ino1 = p6_2_14_5 if p6_2_3 == 1 & p6_2_12 == 8
  	egen	acc_ino2_9 = rowtotal(p6_2_14_3 p6_2_14_5) if p6_2_3 == 1 & (p6_2_13 == 1 | p6_2_13 == 2)	// Números de inodoros accesibles en uso.
			
	collapse	(sum) sshh acc_ino1 acc_ino2_9 ino*, by(id_local nro_pred p5_ed_nro)
	
	* Cálculo de edificaciones con agua y saneamiento
	*-------------------------------------------------
	gen		sshh_edif = sshh > 0 & sshh != .
	rename	p5_ed_nro nro_ed
	merge 	1:1 id_local nro_pred nro_ed using "$Input\P6_1", ///
			keepusing(p6_4_1 p6_4_2)	// Se agregan variables de agua y desagüe.
	drop 	if _merge == 1
	drop	_merge
	
	gen		ad_edif = p6_4_1 == 1 & p6_4_2 == 1 & sshh_edif > 0
	
	collapse	(sum) sshh sshh_edif ad_edif acc_ino1 acc_ino2_9 ino*, by(id_local)

	* Cálculo de brecha de inodoros y urinarios por local
	*-----------------------------------------------------
	egen	ino = rowtotal(ino1 ino2 ino3 ino4 ino5 ino6 ino7 ino8 ino9)
	replace	ino = inod if inod > ino & inod != .

	merge	1:1 id_local using "cie\LE_Alum_CIE"
	drop	if _merge == 2
	drop	_merge alum3 alum
	rename	alum3max alum3
	egen	alum = rowtotal(alum1 alum2 alum3 alum4 alum5 alum6 alum7 alum8 alum9)

	forvalues i = 1/9 {
			replace alum`i' = 0 if alum`i' == .
			gen alum_inor`i' = ceil(max(alum`i' - ino`i' * 30, 0))
	} 
	egen	alum_inor = rowtotal(alum_inor1 alum_inor2 alum_inor3 alum_inor4 alum_inor5 alum_inor6 alum_inor7 alum_inor8 alum_inor9)
	
	gen		alum_ino = min(ino * 30, alum) if ino != .
	gen 	alum_inodr = ceil(alum_ino - (alum_ino * (inod / ino)))
		
	gen 	alum_urir2 = ceil(max(alum2 - ino2 * 30, 0) * 0.50)
	gen 	alum_urir3 = ceil(max(alum3 - ino3 * 30, 0) * 0.50)
	gen 	alum_urir4 = ceil(max(alum4 - ino4 * 30, 0) * 0.55)
	gen 	alum_urir5 = ceil(max(alum5 - ino5 * 30, 0) * 0.60)
	gen 	alum_urir6 = ceil(max(alum6 - ino6 * 30, 0) * 0.67)
	gen 	alum_urir7 = ceil(max(alum7 - ino7 * 30, 0) * 0.42)
	gen 	alum_urir8 = ceil(max(alum8 - ino8 * 30, 0) * 0.30)
	gen 	alum_urir9 = ceil(max(alum9 - ino9 * 30, 0) * 0.36)
	egen	alum_urir = rowtotal(alum_urir2 alum_urir3 alum_urir4 alum_urir5 alum_urir6 alum_urir7 alum_urir8 alum_urir9)
	
	gen		alum_uri2 = ceil(min(ino2 * 30, alum2) * 0.50) if ino2 != .
	gen		alum_uri3 = ceil(min(ino3 * 30, alum3) * 0.50) if ino3 != .
	gen		alum_uri4 = ceil(min(ino4 * 30, alum4) * 0.55) if ino4 != .
	gen		alum_uri5 = ceil(min(ino5 * 30, alum5) * 0.60) if ino5 != .
	gen		alum_uri6 = ceil(min(ino6 * 30, alum6) * 0.67) if ino6 != .
	gen		alum_uri7 = ceil(min(ino7 * 30, alum7) * 0.42) if ino7 != .
	gen		alum_uri8 = ceil(min(ino8 * 30, alum8) * 0.30) if ino8 != .
	gen		alum_uri9 = ceil(min(ino9 * 30, alum9) * 0.36) if ino9 != .
	
	forvalues i = 2/9 {
			gen alum_uridr`i' = ceil(alum_uri`i' - (alum_uri`i' * (inod / ino)))
	}
	egen	alum_uridr = rowtotal(alum_uridr2 alum_uridr3 alum_uridr4 alum_uridr5 alum_uridr6 alum_uridr7 alum_uridr8 alum_uridr9)

	gen		int_sshh = sshh == 0 | ad_edif < sshh_edif | ad_edif == 0	
	
	gen		ba_inoamp = alum_inor if int_sshh == 0	
	replace	ba_inoamp = alum if int_sshh == 1
	gen		ba_inoreh = alum_inodr if int_sshh == 0 & alum != . & alum != ba_inoamp
	
	gen		alumh = ceil(alum2 * 0.50) + ceil(alum3 * 0.50) + ceil(alum4 * 0.55) + ceil(alum5 * 0.60) ///
			+ ceil(alum6 * 0.67) + ceil(alum7 * 0.42) + ceil(alum8 * 0.30) + ceil(alum9 * 0.36)
	gen		ba_uriamp = alum_urir if int_sshh == 0
	replace ba_uriamp =	alumh if int_sshh == 1
	gen		ba_urireh = alum_uridr if int_sshh == 0 & alumh != . & alumh != ba_uriamp
	
	gen 	b_beb = max(1, ceil(alummax / 40))
	
	compress
	save 	"cie\LE_SSHH_CIE.dta", replace
	
	/*_____________________________________________________________________
	|                                                                      |
	|      				INTERVENCIÓN EN ACCESIBILIDAD	 	  		       |
	|_____________________________________________________________________*/
	
	use		"$Input\P6_1_8N", clear
	egen 	num_pisos = count(p6_1_8_accesibilidad), by(id_local nro_pred nro_ed)

	forvalues i = 1/5 {
			gen 	acc`i' = p6_1_8id == `i' & p6_1_8_accesibilidad == 1
	}
	
	collapse 	(max) acc* num_pisos, by(id_local nro_pred nro_ed)
	replace num_pisos = 1 if num_pisos == 0

	gen		acc_edif1 = num_pisos == 1 & acc1 == 1
	gen		acc_edif2 = num_pisos == 2 & acc1 == 1 & acc2 == 1
	gen		acc_edif3 = num_pisos == 3 & acc1 == 1 & acc2 == 1 & acc3 == 1
	gen		acc_edif4 = num_pisos == 4 & acc1 == 1 & acc2 == 1 & acc3 == 1 & acc4 == 1
	gen		acc_edif5 = num_pisos == 5 & acc1 == 1 & acc2 == 1 & acc3 == 1 & acc4 == 1 & acc5 == 1
	gen		acc_edif = acc_edif1 == 1 | acc_edif2 == 1 | acc_edif3 == 1 | acc_edif4 == 1 | acc_edif5 == 1
	
	gen		accno_edif1 = num_pisos == 1 & acc_edif1 == 0
	gen		accno_edif2 = num_pisos == 2 & acc_edif2 == 0
	gen		accno_edif3 = num_pisos == 3 & acc_edif3 == 0
	gen		accno_edif4 = num_pisos == 4 & acc_edif4 == 0
	gen		accno_edif5 = num_pisos == 5 & acc_edif5 == 0	
	gen		accno_edif2_5 = accno_edif2 == 1 | accno_edif3 == 1 | accno_edif4 == 1 | accno_edif5 == 1
	
	gen		acc_pisos = num_pisos if acc_edif == 1
		
	collapse	(sum) acc_edif* accno_edif1 accno_edif2_5 acc_pisos num_pisos (count) nro_ed, by(id_local)
	
	* Cálculo de áreas de accesibilidad mínimas
	*-------------------------------------------	
	merge	1:1 id_local using "cie\LE_Alum_CIE"
	keep	if _merge == 3
	drop	_merge

	merge 	1:1 id_local using "cie\LE_Infra_CIE", keepusing(id_local zona areatechosuperior areatechadatotal)
	keep 	if _merge == 3
	drop 	_merge		// Mantener locales que tienen información del estado de la infraestructura.
	
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
	
	scalar	FPS = 0.7 	// Factor que considera si un local tiene primaria y secundaria para el cálculo de área mínima.
	
	forvalues i = 1/9 {
			gen areamina`i' = alum`i' * areaminau`i'
	}
	
	replace areamina2 = areamina2 * FPS if areamina3 > areamina2 & areamina2 != . & areamina3 != .
	replace areamina3 = areamina3 * FPS if areamina2 > areamina3 & areamina2 != . & areamina3 != .
	
	egen	areamina = rowtotal(areamina1 areamina2 areamina3 areamina4 areamina5 ///
					   areamina6 areamina7 areamina8 areamina9)
	
	* Cálculo de requerimiento de ampliación de terrenos
	*----------------------------------------------------
	merge 	1:1 id_local using "cie\LE_Aterr_CIE", keepusing(areaterr)
	drop 	if _merge == 2
	drop	_merge
	replace areaterr = areatechosuperior if areaterr == .
	
	gen		calc_ampt = areaterr + 10 >= areatechosuperior	// No considerar inconsistencias (> 10 m2 = inconsistencia). DISCUTIR SI REVISAR ESTE SUPUESTO!
	gen		int_ampt = areamina > areaterr if calc_ampt == 1
	replace int_ampt = 0 if calc_ampt == 0
	
	* Cálculo de rampas y ascensores requeridos
	*-------------------------------------------
	gen 	num_niv = 0
	forvalues i = 1/9 {
			replace num_niv = num_niv + 1 if alum`i' > 0 & alum`i' != .
	}
	replace	num_niv = 1 if num_niv == 0
	
	gen		acc_rr = max(min(num_niv, nro_ed) - acc_edif, 0) if int_ampt == 0
	gen		acc_rr2_5 = min(acc_rr, accno_edif2_5) if int_ampt == 0
	gen		acc_rr1 = max(acc_rr-acc_rr2_5, 0) if int_ampt == 0
	gen		acc_ar = max(min(num_niv, nro_ed - (acc_edif + accno_edif1)), 0) if int_ampt == 1 & zona == 2	// DISCUTIR SI CONSIDERAR A ZONA RURAL CON INT. AMPLIACIÓN DE TERRENO.

	* Cálculo de inodoros accesibles requeridos
	*-------------------------------------------
	merge 	1:1 id_local using "cie\LE_SSHH_CIE", keepusing(acc_ino1 acc_ino2_9)
	drop 	if _merge == 2
	drop	_merge
	replace acc_ino1 = 0 if acc_ino1 == .
	replace acc_ino2_9 = 0 if acc_ino2_9 == .
	
	gen		num_niv2_9 = num_niv - 1 if alum1 > 0 & alum1 != .
	replace	num_niv2_9 = num_niv if num_niv2_9 == .
	
	gen		acc_ir1 = max(2 - acc_ino1, 0) if alum1 > 0 & alum1 != .
	gen		acc_ir2_9 = max(2 * num_niv2_9 - acc_ino2_9, 0)
	egen	acc_ir = rowtotal(acc_ir1 acc_ir2_9)
		
	compress
	save 	"cie\LE_Acc_CIE.dta", replace
	
	/*_____________________________________________________________________
	|                                                                      |
	|            	ESTADO DE ELEMENTOS NO ESTRUCTURALES               	   |
	|_____________________________________________________________________*/	
	
	use		"cie\Edif_Infra_CIE", clear
	drop	if NC == 1
	save	"cie\Edif_InfraC_CIE.dta", replace
	
	use 	"$Input\p6_2", clear
	
	rename 	p6_2_15a p6_2_15_est
	gen 	p6_2_15a = p6_2_15_est == 1
	gen 	p6_2_15b = p6_2_15_est == 2
	gen 	p6_2_15c = p6_2_15_est == 3
	gen 	aulas = p6_2_5 == 1 
		
	collapse	(sum) p6_2_15a p6_2_15b p6_2_15c	///
					  p6_2_16a_b p6_2_16a_r p6_2_16a_m p6_2_16b_b p6_2_16b_r p6_2_16b_m p6_2_16c_b p6_2_16c_r ///
					  p6_2_16c_m p6_2_16d_b p6_2_16d_r p6_2_16d_m p6_2_16e_b p6_2_16e_r p6_2_16e_m	///
					  p6_2_17a p6_2_17b p6_2_17c p6_2_17d	///
					  p6_2_18a_b p6_2_18a_r p6_2_18a_m p6_2_18b_b p6_2_18b_r p6_2_18b_m p6_2_18c_b p6_2_18c_r ///
					  p6_2_18c_m p6_2_18d_b p6_2_18d_r p6_2_18d_m p6_2_18e_b p6_2_18e_r p6_2_18e_m	///
					  p6_2_19a p6_2_19b p6_2_19c aulas, by(id_local nro_pred p5_ed_nro)
	
	rename	(p5_ed_nro nro_pred) (Nmerototaledificaciones NmeroPredios)
	compress
	save	"cie\Edif_Aulas.dta", replace
	
	merge	1:1 id_local NmeroPredios Nmerototaledificaciones using "cie\Edif_InfraC_CIE", keepusing(ratiodem areasust areari arearc areatechadatotal)
	drop 	if _merge == 2
	drop	_merge
	keep 	if areasust == . & areari == . & arearc == . & ratiodem < 0.7		// Solo mantener edificaciones sin intervención de sustitución o reforzamiento.
	
	gen 	num_ene2b = p6_2_16a_b + p6_2_16b_b + p6_2_16c_b + p6_2_16d_b + p6_2_16e_b + p6_2_18a_b + p6_2_18b_b + p6_2_18c_b + p6_2_18d_b + p6_2_18e_b
	gen 	num_ene2r = p6_2_16a_r + p6_2_16b_r + p6_2_16c_r + p6_2_16d_r + p6_2_16e_r + p6_2_18a_r + p6_2_18b_r + p6_2_18c_r + p6_2_18d_r + p6_2_18e_r
	gen 	num_ene2m = p6_2_16a_m + p6_2_16b_m + p6_2_16c_m + p6_2_16d_m + p6_2_16e_m + p6_2_18a_m + p6_2_18b_m + p6_2_18c_m + p6_2_18d_m + p6_2_18e_m
	
	gen		area_ene1 = areatechadatotal * (0 * p6_2_15a + 0.5 * p6_2_15b + 1 * p6_2_15c) / (p6_2_15a + p6_2_15b + p6_2_15c)
	gen		area_ene2 = areatechadatotal * (0 * num_ene2b + 0.5 * num_ene2r  + 1 * num_ene2m) / (num_ene2b + num_ene2r + num_ene2m)
	gen		area_ene3 = areatechadatotal * (0 * p6_2_17a + 0.25 * p6_2_17b + 0.75 * p6_2_17c + 1 * p6_2_17d) / (p6_2_17a + p6_2_17b + p6_2_17c + p6_2_17d)
	gen		area_ene4 = areatechadatotal * (0 * p6_2_19a + 1 * p6_2_19b + 1 * p6_2_19c) / (p6_2_19a + p6_2_19b + p6_2_19c)
	* DISCUTIR SI HACERLO POR EDIFICACION O POR LOCAL.
	
	keep	id_local Nmerototaledificaciones NmeroPredios areatechadatotal area_ene1 area_ene2 area_ene3 area_ene4
	compress
	save	"cie\Edif_Ene_CIE.dta", replace
	
	collapse	(sum) areatechadatotal area_ene1 area_ene2 area_ene3 area_ene4, by(id_local)
	
	compress
	save	"cie\LE_Ene_CIE.dta", replace
	erase	"cie\Edif_InfraC_CIE.dta"
	
	* Optimización de base de edificaciones con número de aulas.
	*-----------------------------------------------------------
	use 	"cie\Edif_Aulas.dta", clear
	keep	id_local Nmerototaledificaciones NmeroPredios aulas
	compress
	save	"cie\Edif_Aulas.dta", replace
	
	/*_____________________________________________________________________
	|                                                                      |
	|            CÁLCULO DE COSTO DE ACCESO DE AGUA Y DESAGÜE        	   |
	|_____________________________________________________________________*/	
	
	use 	"$Input\P2_D_5N", clear
	append 	using "$Input\P2_D_9N"
	
	merge	m:1	id_local nro_pred using "$Input\P2_C", keepusing(p2_c_1locl_2_agua p2_c_1locl_3_alc)
	drop	if _merge == 2
	drop	_merge
	
	* Corrección de información incompleta en P2_C
	*----------------------------------------------
	replace p2_c_1locl_2_agua = 1 if id_local == 672471
	replace p2_c_1locl_3_alc = 1 if id_local == 672471	
	replace	p2_c_1locl_2_agua = 2 if id_local == 108736
	replace p2_c_1locl_3_alc = 2 if id_local == 108736

	* Elaboración de indicadores
	*----------------------------
	gen		aa_rp = p2_d_5_cod == 1 & p2_d_5_cod_est == 1		// Acceso de agua: red pública.
	gen		aa_pp = p2_d_5_cod == 2 & p2_d_5_cod_est == 1		// Acceso de agua: pilón de uso público de agua potable.
	gen		aa_cc = p2_d_5_cod == 3 & p2_d_5_cod_est == 1		// Acceso de agua: camión cisterna u otro similar.
	gen		aa_po = p2_d_5_cod == 4 & p2_d_5_cod_est == 1		// Acceso de agua: pozo.
	gen		aa_ra = p2_d_5_cod == 5 & p2_d_5_cod_est == 1		// Acceso de agua: río, acequia, manantial o similar.
	gen		aa_ot = p2_d_5_cod == 6 & p2_d_5_cod_est == 1		// Acceso de agua: otra fuente.
	
	gen		ad_rp = p2_d_9_nro == 1 & p2_d_9_cod == 1			// Acceso de desagüe: red pública.
	gen		ad_pp = p2_d_9_nro == 2 & p2_d_9_cod == 1			// Acceso de desagüe: pozo percolador.
	gen		ad_pct = p2_d_9_nro == 3 & p2_d_9_cod == 1			// Acceso de desagüe: pozo con tratamiento.
	gen		ad_pst = p2_d_9_nro == 4 & p2_d_9_cod == 1			// Acceso de desagüe: pozo sin tratamiento.
	gen		ad_ra = p2_d_9_nro == 5 & p2_d_9_cod == 1			// Acceso de desagüe: río acequia.
	gen		ad_zf = p2_d_9_nro == 6 & p2_d_9_cod == 1			// Acceso de desagüe: zanja filtrante.
	gen		ad_no = p2_d_9_nro == 7 & p2_d_9_cod == 1			// Acceso de desagüe: no tiene.
	
	gen		aa_loc = p2_c_1locl_2_agua == 1
	gen		ad_loc = p2_c_1locl_3_alc == 1	
	
	collapse	(max) aa_* ad_*, by(id_local nro_pred)
	
	* Cálculo de costos de intervención
	*-----------------------------------	
	merge 	m:1 id_local using "cie\LE_Infra_CIE", keepusing(id_local clima)
	keep 	if _merge == 3
	drop 	_merge		// Mantener locales que tienen información del estado de la infraestructura.
	
	gen		int_aa_no = aa_rp == 1 | aa_cc == 1 | aa_po == 1	// Sin intervención.
	gen		int_aa_cnrp = (aa_rp != 1 & aa_cc != 1 & aa_po != 1) & aa_loc == 1	// Conexión a red pública + medidor.
	gen		int_aa_alpt = (aa_rp != 1 & aa_cc != 1 & aa_po != 1) & aa_loc != 1 & (clima >= 7 & clima <= 9)	// Agua de lluvia y planta de tratamiento.
	gen		int_aa_pasc = (aa_rp != 1 & aa_cc != 1 & aa_po != 1) & aa_loc != 1 & (clima >= 1 & clima <= 6) 	// Pozo de agua y sistema de cloración.
	
	gen		int_ad_no = ad_rp == 1 | ad_pp == 1		// Sin intervención.
	gen		int_ad_cnrp = (ad_rp != 1 & ad_pp != 1) & ad_loc == 1	// Conexión a red pública.
	gen		int_ad_zinu = (ad_rp != 1 & ad_pp != 1) & ad_loc != 1 & clima == 9	// En zona inundable.
	gen		int_ad_sinsitu = (ad_rp != 1 & ad_pp != 1) & ad_loc != 1 & (clima >= 1 & clima <= 8)	// Sistema in-situ.
	
	compress
	save	"cie\Pred_Aad_CIE.dta", replace
	
	collapse (sum) int_*, by(id_local)
	compress
	save	"cie\LE_Aad_CIE.dta", replace
	
	/*_____________________________________________________________________
	|                                                                      |
	|       	SISTEMA DE ALMACENAMIENTO DE IMPULSIÓN DE AGUA		       |
	|_____________________________________________________________________*/
	
	* Obtención de variable de abastecimiento de agua L-V
	*-----------------------------------------------------
	use 	"$Input\plocal2016 base actual de 11 - local escolar", clear
	merge 	1:1 codlocal using "$Input\11 - local escolar", keepusing(p216) 
	gen		agualv = p224 == "S"
	replace	agualv = p216 == "S" if p224 == ""
	destring 	codlocal, gen(id_local)
	keep	id_local agualv
	
	save	"cie\LE_AguaLV_CIE.dta", replace
	
	* Procesamiento de variables de tanques elevados y cisternas
	*------------------------------------------------------------
	use 	"$Input\p8", clear
	keep	if p8_2_tipo == "CTE" & p8_est_palo != .
	
	gen		cis = p8_est_palo == 5			// Predio tiene cisterna.
	gen		te_si = p8_est_palo == 1		// Tanque elevado sin intervención.
	gen		te_mb = p8_est_palo == 2		// Tanque elevado requiere mantenimiento bajo.
	gen		te_mm = p8_est_palo == 3		// Tanque elevado requiere mantenimiento moderado.
	gen		te_sus = p8_est_palo == 4		// Tanque elevado requiere sustitución.
	
	collapse	(sum) cis te_si te_mb te_mm te_sus, by(id_local nro_pred)
	
	merge	1:1	id_local nro_pred using "$Input\pcar", keepusing(pc_e_6_tcist)
	drop	if _merge == 1
	drop	_merge	// Mantener locales que tienen información de cisternas / tanques.

	merge	m:1	id_local using "cie\LE_AguaLV_CIE"
	drop	if _merge == 2
	drop	_merge
	replace agualv = 0 if agualv == .
	
	merge	1:1	id_local nro_pred using "cie\Pred_Aad_CIE", keepusing(aa_*)
	drop	if _merge == 2
	drop	_merge	
		
	* Implementación de cisterna / tanque elevado.
	gen		cis_imp = ((aa_rp == 1 | aa_cc == 1 | aa_po == 1) & agualv == 0 & (cis == 0 | cis == .)) | (aa_rp != 1 & aa_cc != 1 & aa_po != 1)
	gen		te_imp = ((aa_rp == 1 | aa_cc == 1 | aa_po == 1) & agualv == 0 & (te_si == 0 | te_si == .) ///
			& (te_mb == 0 | te_mb == .) & (te_mm == 0 | te_mm == .) & (te_sus == 0 | te_sus == .)) | (aa_rp != 1 & aa_cc != 1 & aa_po != 1)
			
	collapse	(sum) cis* te_* (max) agualv, by(id_local)
	
	* Cálculo de metros cúbicos de tanques y cisternas según alumnos
	*----------------------------------------------------------------
	merge	1:1 id_local using "cie\LE_Alum_CIE", keepusing(alumtot alum1tot alum2tot alum3 alum4tot alum5tot alum6tot alum7tot alum8tot alum9tot)
	drop	if _merge == 2
	drop	_merge
	
	merge 	1:1 id_local using "cie\LE_Infra_CIE", keepusing(id_local zona)
	keep 	if _merge == 3
	drop 	_merge		// Mantener locales que tienen información del estado de la infraestructura.
	
	gen		aguar = alumtot * 50 if zona == 2
	replace	aguar = (alum1tot + alum2tot) * 15 + (alum3 + alum4tot + alum5tot + alum6tot + alum7tot + alum8tot + alum9tot) * 20 if zona == 1
	
	gen		cisvol = aguar * (3 / 4) / 1000
	gen		tevol = aguar * (1 / 3) / 1000
	
	compress
	save	"cie\LE_Alma_CIE.dta", replace
	
	/*_____________________________________________________________________
	|                                                                      |
	|            	CÁLCULO DE COSTO DE ACCESO DE ENERGÍA               	   |
	|_____________________________________________________________________*/	
	
	use 	"$Input\P2_D_1N", clear
	
	merge	m:1	id_local nro_pred using "$Input\P2_C", keepusing(p2_c_1locl_1_energ p2_c_2loce_1_energ)
	drop	if _merge == 2
	drop	_merge
	
	gen		ae_rp = p2_d_1_cod == 1 & p2_d_1_cod_est == 1
	gen		ae_gm = p2_d_1_cod == 2 & p2_d_1_cod_est == 1
	gen		ae_ps = p2_d_1_cod == 3 & p2_d_1_cod_est == 1
	gen		ae_ot = p2_d_1_cod == 4 & p2_d_1_cod_est == 1
	gen		ae_loc = p2_c_1locl_1_energ == 1
	gen		ae_le = p2_c_2loce_1_energ == 1
	
	collapse	(max) ae_*, by(id_local)
	
	* Cálculo de costos de intervención
	*-----------------------------------	
	merge 	1:1 id_local using "cie\LE_Infra_CIE", keepusing(id_local zona areatechadatotal int_st)
	keep 	if _merge == 3
	drop 	_merge		// Mantener locales que tienen información del estado de la infraestructura.
	
	gen		int_ae_no = ((ae_loc == 1 & ae_le == 1) | (ae_loc == 0 & ae_le == 1)) & (ae_rp == 1 & ae_gm == 0 & ae_ps == 0 & ae_ot == 0)		// Sin intervención.
	gen		int_ae_adec = ((ae_loc == 1 & ae_le == 1) & (ae_gm == 1 | ae_ps == 1 | ae_ot == 1)) | 	///
						  ((ae_loc == 0 & ae_le == 1) & (ae_rp == 1 & (ae_gm == 1 | ae_ps == 1 | ae_ot == 1))) 	// Adecuar conexión de energía.
	gen		int_ae_prloc = (ae_loc == 0 & ae_le == 0) | ((ae_loc == 0 & ae_le == 1) & (ae_rp == 0 & (ae_gm == 1 | ae_ps == 1 | ae_ot == 1)))	// Proveer conexión a localidad.
	gen		int_ae_prle = ae_loc == 1 & ae_le == 0	// Proveer conexión a local educativo.
	replace	int_ae_adec = 1 if ae_le == 1 & (ae_rp == 0 & ae_gm == 0 & ae_ps == 0 & ae_ot == 0)		// Adecuar conexión de energía (caso de inconsistencia).
	
	merge 	1:1 id_local using "cie\LE_Amp_CIE", keepusing(areatech1 areatech2 areatech3 areatech4 areatech5 areatech6 areatech7 areatech8 areatech9)
	drop	if _merge == 2
	drop	_merge
	
	egen	areatechmax = rowmax(areatech1 areatech2 areatech3 areatech4 areatech5 areatech6 areatech7 areatech8 areatech9)
	replace areatechmax = areatechadatotal if areatechmax == .		// Los locales sin información de áreas techadas por nivel se incorporan con el área techada total.
	
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
	replace	ct_ae = 0 if int_st == 1 	// Locales que requieren sustitución total se registran por separado.
	
	compress
	save	"cie\LE_Ae_CIE.dta", replace	
	
	/*_____________________________________________________________________
	|                                                                      |
	|            			CÁLCULO DE BRECHA 	                    	   |
	|_____________________________________________________________________*/
	
	use 	"cie\LE_Infra_CIE", clear
	
	merge	1:1 id_local using "cie\LE_Cerco_CIE", keepusing(cp cp_* tramo)
	drop 	if _merge == 2
	drop	_merge
	
	merge	1:1 id_local using "cie\LE_Amp_CIE", keepusing(areaamp)
	drop 	if _merge == 2
	drop	_merge
	replace	areaamp = 0 if areaamp == .
	
	merge	1:1 id_local using "cie\LE_Acc_CIE", keepusing(acc_ir acc_rr1 acc_rr2_5 acc_ar)
	drop 	if _merge == 2
	drop	_merge
	
	merge	1:1 id_local using "cie\LE_Ene_CIE", keepusing(area_ene1 area_ene2 area_ene3 area_ene4)
	drop 	if _merge == 2
	drop	_merge
	
	merge	1:1 id_local using "cie\LE_SSHH_CIE", keepusing(ba_* b_* int_sshh)
	drop 	if _merge == 2
	drop	_merge
	
	merge	1:1 id_local using "cie\LE_Aad_CIE", keepusing(int_*)
	drop 	if _merge == 2
	drop	_merge
	
	merge	1:1 id_local using "cie\LE_Alma_CIE", keepusing(cis* te*)
	drop 	if _merge == 2
	drop	_merge
	
	merge	1:1 id_local using "cie\LE_Ae_CIE", keepusing(ct_ae ct_st_ae)
	drop 	if _merge == 2
	drop	_merge
	
	merge	1:1 id_local using "cie\LE_Alum_CIE", keepusing(alumtot)
	drop	if _merge == 2
	drop	_merge
	
	merge 	1:1 id_local using "$Raw\BrSAFIL_PNIE", keepusing(b_safil)
	drop 	if _merge == 2
	drop 	_merge
	
	merge	m:1 codgeo zona using "$Raw\BrSAFIL_Dist", keepusing(b_safil) update
	drop	if _merge == 2
	drop	_merge	
	
	gen 	codgeor = substr(codgeo, 1,2)
	merge	m:1 codgeor zona using "$Raw\BrSAFIL_Reg", keepusing(b_safil) update
	drop	if _merge == 2
	drop	_merge
	
	rename	id_local cod_local
	merge 	1:1 cod_local using "LE_BaseAdic.dta", keepusing(disafil)
	drop	if _merge == 2
	drop	_merge
	replace b_safil = 0 if disafil == 1
	rename	cod_local id_local
		
	* Proyección de elementos faltantes para NO CENSADOS
	*----------------------------------------------------	
	* ELEMENTOS PROYECTADOS PARA CÁLCULO DE BRECHA DE NO CENSADOS:
	* - en b_1: cp_n, cp_dr, cp_mm, cp_mb.
	* - en b_3: area_ce, area_ene.
	* - en b_4: areaamp, acc_ir, acc_rr1, acc_rr2_5, acc_asc (NO).
	* SUBCOMPONENTES DE COSTOS PROYECTADOS PARA CÁLCULO DE BRECHA DE NO CENSADOS (luego de calcular el subcomponente):
	* - en b_2: ct_aad, ct_cis_te, ct_sshh_beb, ct_ca_bp.
	* - en b_4: ct_ae.
	* - en b_5: ct_st_aad, ct_st_ae.

	gen		atechc =		areatechadatotal if NC == 0
	replace atechc = 		0 if NC == 1
	egen	dz_atechc = 	sum(atechc), by(codgeo zona)
	egen	rz_atechc = 	sum(atechc), by(codgeor zona)
	gen		fp_at_dz =		areatechadatotal / dz_atechc if NC == 1
	gen		fp_at_rz =		areatechadatotal / rz_atechc if NC == 1
	
	foreach v of varlist cp_n cp_dr cp_mm cp_mb areaamp {
		recast	double `v'
		egen 	dz_`v' = 	sum(`v'), by(codgeo zona)
		egen	rz_`v' = 	sum(`v'), by(codgeor zona)
		replace	`v' = 		fp_at_dz * dz_`v' if NC == 1
		replace	`v' = 		fp_at_rz * rz_`v' if NC == 1 & `v' == .
		replace	`v' = 		0 if `v' == .
	}
	foreach v of varlist area_ce* ct_ae {
		recast	double `v'
		egen 	dz_`v' = 	sum(`v'), by(codgeo zona)
		egen	rz_`v' = 	sum(`v'), by(codgeor zona)
		replace	`v' = 		fp_at_dz * dz_`v' if int_st != 1 & NC == 1
		replace	`v' = 		fp_at_rz * rz_`v' if int_st != 1 & NC == 1 & `v' == .
	}
	foreach v of varlist area_ene* {
		egen 	dz_`v' = 	sum(`v'), by(codgeo zona)
		egen	rz_`v' = 	sum(`v'), by(codgeor zona)
		replace	`v' = 		fp_at_dz * dz_`v' if int_st != 1 & (areasust + areari + arearc + 0.01 < areatechadatotal) & NC == 1
		replace	`v' = 		fp_at_rz * rz_`v' if int_st != 1 & (areasust + areari + arearc + 0.01 < areatechadatotal) & NC == 1 & `v' == .
	}	
	foreach v of varlist acc_ir acc_rr1 acc_rr2_5 {
		egen 	dz_`v' = 	sum(`v'), by(codgeo zona)
		egen	rz_`v' = 	sum(`v'), by(codgeor zona)
		replace	`v' = 		ceil(fp_at_dz * dz_`v') if NC == 1
		replace	`v' = 		ceil(fp_at_rz * rz_`v') if NC == 1 & `v' == .
	}	
	egen	acc_rr =		rowtotal(acc_rr1 acc_rr2_5) if NC == 1
	replace acc_rr1 = 		max(min(Nmerototaledificaciones-acc_rr2_5, acc_rr1), 0) if acc_rr1 != . & NC == 1
	replace	acc_rr2_5 = 	max(min(Nmerototaledificaciones,acc_rr2_5),0) if acc_rr2_5 != . & NC == 1

	egen 	dz_ct_st_ae = 	sum(ct_st_ae), by(codgeo zona)
	egen 	rz_ct_st_ae = 	sum(ct_st_ae), by(codgeor zona)
	replace	ct_st_ae = 		fp_at_dz * dz_ct_st_ae if int_st == 1 & NC == 1 
	replace	ct_st_ae = 		fp_at_rz * rz_ct_st_ae if int_st == 1 & NC == 1 & ct_st_ae == .
	
	drop	atechc dz_* rz_* acc_rr
	
	* Obtener costos unitarios, aplicar actualización por inflación y definir factores
	*----------------------------------------------------------------------------------
	merge	m:1 escenario clima pendiente using "Cunit_Atech"	// Usar Cunit_Atech2018 para costos 2018.
	drop 	if _merge == 2
	drop	_merge
	
	merge	m:1 zona escenario using "Cunit_Cp"					// Usar Cunit_Cp2018 para costos 2018.
	drop 	if _merge == 2
	drop	_merge			// Se ha proyectado el costo unitario de cerco perimétrico para escenarios 4 y 5
							// según los costos unitarios promedio de área techada (4.28% y 29.09% respectivamente sobre costo promedio de escenario 3).
	
	merge	m:1 escenario topografia using "Cunit_Acc"
	drop 	if _merge == 2
	drop	_merge			// Se ha proyectado el costo unitario de ascensores para escenarios 4 y 5
							// según los costos unitarios promedio de área techada (4.28% y 29.09% respectivamente sobre costo promedio de escenario 3).

	merge	m:1 escenario clima using "Cunit_AgSn"
	drop 	if _merge == 2
	drop	_merge
	gen		cu_ad_zinu = 20000
	
	merge 	m:1 clima using "Cprop_Ce"
	drop 	if _merge == 2
	drop	_merge
	
	merge 	m:1 escenario clima using "Cprop_Ene"
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
	gen		ct_st_d = 	(areatechadatotal * cu_at * 0.35) * FSCX * FGGU * FIGV if int_st == 1
	gen		ct_sp_d = 	(areasust * cu_at * 0.35) * FSCX * FGGU * FIGV if int_sp == 1
	gen		ct_ri = 	(areari * cu_at * 0.3) * FSC * FGGU * FIGV if int_ri == 1
	gen		ct_ic = 	((areaic1 + areaic2) * cu_at * 0.15) * FSC * FGGU * FIGV if int_ic == 1
	gen		ct_cp = 	(cp_n + cp_dr * 1.33 + cp_mm * 0.66 + cp_mb * 0.33) * cu_cp * FSCX * FGGU* FIGV if int_st != 1
		
	egen	b_1 = 		rowtotal(ct_st_d ct_sp_d ct_ri ct_ic ct_cp), missing
	
	* Cálculo del Grupo 2: Servicios Básicos de Agua y Saneamiento
	*--------------------------------------------------------------
	gen		f_sp =			areasust / areatechadatotal if int_sp == 1	// Factor con proporción de área techada que requiere sustitución en intervención de sustitución parcial.
	replace	f_sp = 			1							if int_st == 1
	replace	f_sp = 			0							if f_sp == .
	
	gen		ct_aa_cnrp = 	int_aa_cnrp * cu_aa_cnrp_m * FSCX * FGGU * FIGV
	gen		ct_aa_alpt = 	int_aa_alpt * cu_aa_alpt * FSCX * FGGU * FIGV
	gen		ct_aa_pasc = 	int_aa_pasc * cu_aa_pasc * FSCX * FGGU * FIGV
	gen		ct_ad_cnrp = 	int_ad_cnrp * cu_ad_cnrp_m * FSCX * FGGU * FIGV
	gen		ct_ad_zinu = 	cu_ad_zinu if int_ad_zinu > 0 & int_ad_zinu != .
	gen		ct_ad_sinsitu = int_ad_sinsitu * (cu_ad_sinsitu_ts + cu_ad_sinsitu_pp + cu_ad_sinsitu_bc) * FSCX * FGGU * FIGV
	egen	ct_aad = 		rowtotal(ct_aa_cnrp ct_aa_alpt ct_aa_pasc ct_ad_cnrp ct_ad_zinu ct_ad_sinsitu), missing
	gen	 	ct_st_aad = 	ct_aad if int_st == 1
	replace ct_st_aad = 	0 if int_st != 1
	replace	ct_aad = 		0 if int_st == 1 	// Locales que requieren sustitución total se registran por separado.
	
	gen		ct_cis_imp = 	(cu_cis_cb + cu_cis_eb + cisvol * cu_cis_m3) * FSCX * FGGU * FIGV if cis_imp > 0 & cis_imp != . & cisvol > 0 & cisvol != . & int_st != 1
	replace ct_cis_imp = 	(cu_cis_cb + cu_cis_eb) * FSCX * FGGU * FIGV if ct_cis_imp == . & cis_imp > 0 & cis_imp != . & int_st != 1
	gen		ct_te_sus = 	tevol * cu_te_m3 * 1.20 * FSCX * FGGU * FIGV if te_sus > 0 & te_sus != . & int_st != 1	// REVISAR SI CONSIDERAR CADA TANQUE O SOLO M3 NECESARIOS.
	gen		ct_te_imp = 	tevol * cu_te_m3 * FSCX * FGGU * FIGV if te_imp > 0 & te_imp != . & (ct_te_sus == . | ct_te_sus == 0) & int_st != 1
	gen		ct_te_mm = 		tevol * cu_te_m3 * 0.30 * FSCX * FGGU * FIGV if te_mm > 0 & te_mm != . & (ct_te_sus == . | ct_te_sus == 0) & (ct_te_imp == . | ct_te_imp == 0) & int_st != 1
	gen		ct_te_mb = 		tevol * cu_te_m3 * 0.20 * FSCX * FGGU * FIGV if te_mb > 0 & te_mb != . & (ct_te_sus == . | ct_te_sus == 0) & (ct_te_imp == . | ct_te_imp == 0) & (ct_te_mm == . | ct_te_mm == 0) & int_st != 1
	egen	ct_cis_te = 	rowtotal(ct_cis_imp ct_te_imp ct_te_mb ct_te_mm ct_te_sus), missing
	
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
	
	foreach v of varlist ct_aad ct_cis_te ct_sshh_beb ct_ca_bp {		// Proyección de subcomponentes de costos para NO CENSADOS.
		egen 	dz_`v' = 	sum(`v'), by(codgeo zona)
		egen	rz_`v' = 	sum(`v'), by(codgeor zona)
		replace	`v' = 		fp_at_dz * dz_`v' if int_st != 1 & NC == 1
		replace	`v' = 		fp_at_rz * rz_`v' if int_st != 1 & NC == 1 & `v' == .
	}
	egen 	dz_ct_st_aad = 	sum(ct_st_aad), by(codgeo zona)
	egen 	rz_ct_st_aad = 	sum(ct_st_aad), by(codgeor zona)
	replace	ct_st_aad = 	fp_at_dz * dz_ct_st_aad if int_st == 1 & NC == 1
	replace	ct_st_aad = 	fp_at_rz * rz_ct_st_aad if int_st == 1 & NC == 1 & ct_st_aad == .  
	
	egen	ct_cad =		rowtotal(ct_cis_te ct_sshh_beb ct_ca_bp), missing
	egen	b_2 =			rowtotal(ct_aad ct_cad), missing
	
	* Cálculo del Grupo 4: Mejoramiento y Ampliación de Locales
	*-----------------------------------------------------------
	gen		ct_sp_r = 	(areasust * cu_at * FSEAC) * FSCX * FGGU * FIGV if int_sp == 1
	gen		ct_rc = 	(arearc * cu_at * 0.5) * FSCX * FGGU * FIGV if int_rc == 1
	gen		ct_ic_r = 	(areaic1 * cu_at * 1.35) * FSC * FGGU * FIGV if int_ic == 1
	gen		ct_amp = 	(areaamp * cu_atyoe) * FSCX * FGGU * FIGV if int_st != 1		// ALERTA! ¿Por qué no considerar FSEAC?
	
	gen		ct_acc1 = 	(acc_ir * cu_ino) * FSCX * FGGU * FIGV if int_st != 1
	gen		ct_acc2 =	(acc_rr1 * cu_ram1) * FSCX * FGGU * FIGV if int_st != 1
	gen		ct_acc3 = 	(acc_rr2_5 * cu_ram2_5) * FSCX * FGGU * FIGV if int_st != 1
	gen 	ct_acc4 = 	(acc_ar * cu_asc) * FSCX * FGGU * FIGV if int_st != 1
	egen	ct_acc = 	rowtotal(ct_acc1 ct_acc2 ct_acc3 ct_acc4), missing
	
	egen	b_4 = 		rowtotal(ct_sp_r ct_rc ct_ic_r ct_amp ct_ae ct_acc), missing
	
	* Cálculo del Grupo 5: Nueva Infraestructura
	*--------------------------------------------
	scalar 	FCR = 0.94 		// Factor que reduce el costo para no considerar cercos perimétricos en la zona rural.
	
	gen		ct_st_ra = 	((areatechadatotal + areaamp) * cu_atyoe * FSEAC) * FSCX * FGGU * FIGV if int_st == 1 & zona == 2
	replace	ct_st_ra = 	((areatechadatotal + areaamp) * cu_atyoe * FSEAC * FCR) * FSCX * FGGU * FIGV if int_st == 1 & zona == 1
	
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
	
	* Cálculo final de brecha y cierre de base de datos
	*-----------------------------------------------------------------
	egen	brecha = 	rowtotal(b_1 b_2 b_3 b_4 b_5 b_safil), missing
	
	rename (id_local areatechadatotal areatechosuperior Nmerototaledificaciones NmeroPredios) (cod_local areatech areatechp1 cod_edif cod_pred)
	replace IAPP41 = 0 if IAPP41 == .
	replace IDRELM = 0 if IDRELM == .
	replace IPRONIED = 0 if IPRONIED == .
	gen		CIE = NC == 0
	drop	NC codgeo codgeor dz_* rz_* fp_at_dz fp_at_rz
	order	cod_local b_1 b_2 b_3 b_4 b_5 b_safil brecha areadem ratiodem areariesgo ratioriesgo int_st int_sp int_ri int_rc int_ic
	sort	cod_local
	
	compress
	save	"cie\LE_Brecha_CIE.dta", replace
	
	table CIE, stat(sum b_1 b_2 b_3 b_4 b_5 b_safil brecha) nformat(%18.2fc)
	table CIE, stat(sum brecha) stat(count brecha) nformat(%18.2fc)

	* BRECHA DE NO CENSADOS NO INCLUYE CÁLCULO DIRECTO DE:
	* - en b_1: ct_cp
	* - en b_2: TODO
	* - en b_3: ct_amp_me, ct_ce, ct_ene
	* - en b_4: ct_amp, ct_ae, ct_acc
	* - en b_5: ct_st_rt (parte de areaamp), ct_st_ae
