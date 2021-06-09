WITH query1 AS (
SELECT
	--Declaring country 
	p.l3c_pays_d::text AS country
	--Year
	,extract(YEAR FROM act.d_act)::integer AS year
	--Quarter
	,extract(QUARTER FROM act.d_act)::integer as quarter
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
	--Ocean
	,CASE
		WHEN o.l_ocea='Atlantique' THEN 'ATL'
		WHEN o.l_ocea='Indien' THEN 'IND'
		ELSE 'NK'
	END::text AS ocean
	--School type
	,CASE
		WHEN tb.l4c_tban='BL' THEN 'FSC'
		WHEN tb.l4c_tban='BO' THEN 'FOB'
		WHEN tb.l4c_tban='IND' THEN 'UNK'
		ELSE 'UNKN'
	END::text AS schooltype
	--Supra region
	,CASE
		WHEN zf.c_zfao='27' THEN 'AREA27'
		WHEN zf.c_zfao='37' THEN 'AREA37'
		ELSE 'OFR'
	END::text AS supra_region
	--Sub region
	--,zf.c_dfao::text AS sub_region
	,CASE
		WHEN zf.c_dfao='34.1.3' THEN '34.1.3'
		WHEN zf.c_dfao='34.2' THEN '34.2.0'
		WHEN zf.c_dfao='34.3.1' THEN '34.3.1'
		WHEN zf.c_dfao='34.3.2' THEN '34.3.2'
		WHEN zf.c_dfao='34.3.3' THEN '34.3.3'
		WHEN zf.c_dfao='34.3.4' THEN '34.3.4'
		WHEN zf.c_dfao='34.3.5' THEN '34.3.5'
		WHEN zf.c_dfao='34.3.6' THEN '34.3.6'
		WHEN zf.c_dfao='34.4.1' THEN '34.4.1'
		WHEN zf.c_dfao='34.4.2' THEN '34.4.2'
		WHEN zf.c_dfao='41.1.4' THEN '41.1'
		WHEN zf.c_dfao='47.1.1' THEN '47.1'
		WHEN zf.c_dfao='47.1.2' THEN '47.1'
		WHEN zf.c_dfao='47.A' THEN '47.A'
		WHEN zf.c_dfao='47.B' THEN '47.B'
		WHEN zf.c_dfao='51.3' THEN '51.3'
		WHEN zf.c_dfao='51.4' THEN '51.4'
		WHEN zf.c_dfao='51.5' THEN '51.5'
		WHEN zf.c_dfao='51.6' THEN '51.6'
		WHEN zf.c_dfao='51.7' THEN '51.7'
		WHEN zf.c_dfao='57.1' THEN '57.1'
		WHEN zf.c_dfao='57.2' THEN '57.2'
		ELSE 'ATTENTION PROB'
	END::text AS sub_region
	--EEZ indicator
	,CASE
		WHEN zf.c_dfao='34.1.3' THEN 'RFMO'
		WHEN zf.c_dfao='34.2' THEN 'RFMO'
		WHEN zf.c_dfao='34.3.1' THEN 'NA'
		WHEN zf.c_dfao='34.3.2' THEN 'NA'
		WHEN zf.c_dfao='34.3.3' THEN 'NA'
		WHEN zf.c_dfao='34.3.4' THEN 'NA'
		WHEN zf.c_dfao='34.3.5' THEN 'NA'
		WHEN zf.c_dfao='34.3.6' THEN 'NA'
		WHEN zf.c_dfao='34.4.1' THEN 'NA'
		WHEN zf.c_dfao='34.4.2' THEN 'NA'
		WHEN zf.c_dfao='41.1.4' THEN 'NA'
		WHEN zf.c_dfao='47.1.1' THEN 'NA'
		WHEN zf.c_dfao='47.1.2' THEN 'NA'
		WHEN zf.c_dfao='47.A' THEN 'NA'
		WHEN zf.c_dfao='47.B' THEN 'NA'
		WHEN zf.c_dfao='51.3' THEN 'NA'
		WHEN zf.c_dfao='51.4' THEN 'NA'
		WHEN zf.c_dfao='51.5' THEN 'NA'
		WHEN zf.c_dfao='51.6' THEN 'NA'
		WHEN zf.c_dfao='51.7' THEN 'NA'
		WHEN zf.c_dfao='57.1' THEN 'NA'
		WHEN zf.c_dfao='57.2' THEN 'NA'
		ELSE 'ATTENTION PROB'
	END::text AS eez_indicator
	--Geo indicator
	,'IWE'::text AS geo_indicator
	--Specon tech
	,'NA'::text AS specon_tech
	--Target essemblage
	,'LPF'::text AS target_assemblage
	--Deep sea regulations
	,'NA'::text AS deep
	--Species code
	,e.c_esp_3l::text AS species
	--Estimated landings in tonnes
	,c.v_poids_capt::numeric AS totwghtlandg
	--Estimated total value of the landings
	,0::integer AS totvallandg
	--Data confidential
	,'N'::text AS confidential
FROM
	public.activite act
	JOIN public.capture c
		USING (c_bat, d_act, n_act)
	JOIN public.espece e 
		USING (c_esp)
	JOIN public.a_pays_d p
		USING (c_pays_d)
	JOIN public.bateau b
		USING (c_bat)
	JOIN public.engin en
		USING (c_engin)
	JOIN public.ocean o
		USING (c_ocea)
	JOIN public.type_banc tb
		USING (c_tban)
	JOIN public.zfao zf
		USING (id_zfao)
WHERE
	--p.l3c_pays_d='FRA' AND
	b.c_pav_b IN (1,41) AND
	(act.d_act BETWEEN '2015-01-01' AND '2017-12-31') AND
	--c.c_esp != 8 AND
	--(c.c_esp NOT BETWEEN 800 AND 899) AND
	c.c_esp IN (1, 2, 3)
)
SELECT
	q1.country
	,q1.ocean
	,q1.year
	,q1.quarter
	,q1.vessel_length
	,q1.fishing_tech
	,q1.gear_type
	,q1.mesh_size_range
	,q1.metier
	--Domain
	,q1.country || '_' || q1.quarter || '_' || q1.sub_region || '_' || q1.gear_type || '_LPF_' || q1.mesh_size_range || '_0_' || 'NA_' || 'all_' || 'all_' ||  q1.schooltype::text AS domain
	,q1.supra_region
	,q1.sub_region
	,q1.eez_indicator
	,q1.geo_indicator
	,q1.specon_tech
	,q1.target_assemblage
	,q1.deep
	,q1.species
	,ROUND(sum(q1.totwghtlandg), 3)::numeric AS totwghtlandg
	,q1.totvallandg
	,q1.confidential
FROM
	query1 q1
GROUP BY 
	q1.country
	,q1.ocean
	,q1.year
	,q1.quarter
	,q1.vessel_length
	,q1.fishing_tech
	,q1.gear_type
	,q1.mesh_size_range
	,q1.metier
	,q1.country || '_' || q1.quarter || '_' || q1.sub_region || '_' || q1.gear_type || '_LPF_' || q1.mesh_size_range || '_0_' || 'NA_' || 'all_' || 'all_' || q1.schooltype
	,q1.supra_region
	,q1.sub_region
	,q1.eez_indicator
	,q1.geo_indicator
	,q1.specon_tech
	,q1.target_assemblage
	,q1.deep
	,q1.species
	,q1.totvallandg
	,q1.confidential
;
