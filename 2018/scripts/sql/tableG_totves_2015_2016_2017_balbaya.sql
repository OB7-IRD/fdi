SELECT 
	sq.country
	,sq.year
	,sq.quarter
	,sq.vessel_length
	,sq.fishing_tech
	,sq.gear_type
	,sq.mesh_size_range
	,sq.metier
	,sq.supra_region
	,sq.sub_region
	--,sq.eez_indicator
	,sq.geo_indicator
	,sq.specon_tech
	,sq.target_assemblage
	,sq.deep
	,count(DISTINCT sq.c_bat)::integer AS totves
FROM (
	SELECT 
	--Declaring country 
	p.l3c_pays_d::text AS country

	--Year
	,extract(YEAR FROM act.d_act)::integer AS year
	
	--Quarter
	,extract(QUARTER FROM act.d_act)::integer as quarter
	
	--Id vessel
	,act.c_bat::integer
	
	--Vessel length
	,CASE
		WHEN b.v_l_ht < 10 THEN 'VL0010'
		WHEN b.v_l_ht >= 10 AND b.v_l_ht < 12 THEN 'VL1012'
		WHEN b.v_l_ht >= 12 AND b.v_l_ht < 18 THEN 'VL1218'
		WHEN b.v_l_ht >= 18 AND b.v_l_ht < 24 THEN 'VL1824'
		WHEN b.v_l_ht >= 24 AND b.v_l_ht < 40 THEN 'VL2440'
		WHEN b.v_l_ht >= 40 THEN 'VL40XX'
		ELSE 'NK'
	END::text AS vessel_length

	--Fishing technique
	,CASE
		WHEN b.c_typ_b IN (4, 5, 6) THEN 'PS'
		WHEN b.c_typ_b IN (1, 2) THEN 'HOK'
	END::text AS fishing_tech
		
	--Gear type
	,CASE
		WHEN en.l_engin = 'Canneur' THEN 'LHP'
		WHEN en.l_engin = 'Senneur' THEN 'PS'
		ELSE 'NK'
	END::text AS gear_type

	--Mesh size range
	,CASE 
		WHEN en.l_engin = 'Canneur' THEN 'NA'
		WHEN en.l_engin = 'Senneur' THEN 'NK'
		ELSE 'NK'
	END::text AS mesh_size_range

	--Metier
	,CASE
		WHEN en.l_engin = 'Canneur' THEN 'LHP_LPF_0_0_0'
		WHEN en.l_engin = 'Senneur' THEN 'PS_LPF_0_0_0'
	END::text AS metier

	--Supra region
	,CASE
		WHEN zf.c_zfao='27' THEN 'AREA27'
		WHEN zf.c_zfao='37' THEN 'AREA37'
		ELSE 'OFR'
	END::text AS supra_region

	--Sub region
	,zf.c_dfao::text AS sub_region
--	,CASE
--		WHEN zf.c_dfao='34.1.3' THEN '34.1.3'
--		WHEN zf.c_dfao='34.2' THEN '34.2.0'
--		WHEN zf.c_dfao='34.3.1' THEN '34.3.1'
--		WHEN zf.c_dfao='34.3.2' THEN '34.3.2'
--		WHEN zf.c_dfao='34.3.3' THEN '34.3.3'
--		WHEN zf.c_dfao='34.3.4' THEN '34.3.4'
--		WHEN zf.c_dfao='34.3.5' THEN '34.3.5'
--		WHEN zf.c_dfao='34.3.6' THEN '34.3.6'
--		WHEN zf.c_dfao='34.4.1' THEN '34.4.1'
--		WHEN zf.c_dfao='34.4.2' THEN '34.4.2'
--		WHEN zf.c_dfao='41.1.4' THEN '41.1'
--		WHEN zf.c_dfao='47.1.1' THEN '47.1'
--		WHEN zf.c_dfao='47.1.2' THEN '47.1'
--		WHEN zf.c_dfao='47.A' THEN '47.A'
--		WHEN zf.c_dfao='47.B' THEN '47.B'
--		WHEN zf.c_dfao='51.3' THEN '51.3'
--		WHEN zf.c_dfao='51.4' THEN '51.4'
--		WHEN zf.c_dfao='51.5' THEN '51.5'
--		WHEN zf.c_dfao='51.6' THEN '51.6'
--		WHEN zf.c_dfao='51.7' THEN '51.7'
--		WHEN zf.c_dfao='57.1' THEN '57.1'
--		WHEN zf.c_dfao='57.2' THEN '57.2'
--		ELSE 'ATTENTION PROB'
--	END::text AS sub_region
	
	--EEZ indicator
--	,CASE
--		WHEN zf.c_dfao='34.1.3' THEN 'RFMO'
--		WHEN zf.c_dfao='34.2' THEN 'RFMO'
--		WHEN zf.c_dfao='34.3.1' THEN 'NA'
--		WHEN zf.c_dfao='34.3.2' THEN 'NA'
--		WHEN zf.c_dfao='34.3.3' THEN 'NA'
--		WHEN zf.c_dfao='34.3.4' THEN 'NA'
--		WHEN zf.c_dfao='34.3.5' THEN 'NA'
--		WHEN zf.c_dfao='34.3.6' THEN 'NA'
--		WHEN zf.c_dfao='34.4.1' THEN 'NA'
--		WHEN zf.c_dfao='34.4.2' THEN 'NA'
--		WHEN zf.c_dfao='41.1.4' THEN 'NA'
--		WHEN zf.c_dfao='47.1.1' THEN 'NA'
--		WHEN zf.c_dfao='47.1.2' THEN 'NA'
--		WHEN zf.c_dfao='47.A' THEN 'NA'
--		WHEN zf.c_dfao='47.B' THEN 'NA'
--		WHEN zf.c_dfao='51.3' THEN 'NA'
--		WHEN zf.c_dfao='51.4' THEN 'NA'
--		WHEN zf.c_dfao='51.5' THEN 'NA'
--		WHEN zf.c_dfao='51.6' THEN 'NA'
--		WHEN zf.c_dfao='51.7' THEN 'NA'
--		WHEN zf.c_dfao='57.1' THEN 'NA'
--		WHEN zf.c_dfao='57.2' THEN 'NA'
--		ELSE 'ATTENTION PROB'
--	END::text AS eez_indicator

	--Geo indicator
	,'IWE'::text AS geo_indicator
	
	--Specon tech
	,'NA'::text AS specon_tech
	
	--Target essemblage
	,'LPF'::text AS target_assemblage
	
	--Deep sea regulations
	,'NA'::text AS deep
	
FROM 
	maree m
	JOIN bateau b 
		ON (m.c_bat=b.c_bat)
	JOIN public.activite act
		ON (act.c_bat=m.c_bat AND act.d_dbq=m.d_dbq)
	JOIN public.zfao zf
		USING (id_zfao)
	JOIN public.a_pays_d p 
		ON (act.c_pays_d=p.c_pays_d) 
	JOIN public.engin en
		ON (act.c_engin=en.c_engin)
WHERE
	extract(YEAR FROM act.d_act) IN (2015, 2016, 2017)
	AND b.c_typ_b IN (1,2,4,5,6,7)
) AS sq
GROUP BY
	sq.country
	,sq.year
	,sq.quarter
	,sq.vessel_length
	,sq.fishing_tech
	,sq.gear_type
	,sq.mesh_size_range
	,sq.metier
	,sq.supra_region
	,sq.sub_region
	--,sq.eez_indicator
	,sq.geo_indicator
	,sq.specon_tech
	,sq.target_assemblage
	,sq.deep
;
