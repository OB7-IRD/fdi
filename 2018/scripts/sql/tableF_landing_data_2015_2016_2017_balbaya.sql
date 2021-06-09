WITH query1 AS (
SELECT
	--Declaring country 
	p.l3c_pays_d::text AS country
	--Year
	,EXTRACT(YEAR FROM act.d_act)::integer AS YEAR
	--Quarter
	--,EXTRACT(QUARTER FROM act.d_act)::integer as quarter
	--Gear type
	,CASE
		WHEN en.l_engin = 'Canneur' THEN 'LHP'
		WHEN en.l_engin = 'Senneur' THEN 'PS'
		ELSE 'NK'
	END::text AS gear_type
	--Metier
	,CASE
		WHEN en.l_engin = 'Canneur' THEN 'LHP_LPF_0_0_0'
		WHEN en.l_engin = 'Senneur' THEN 'PS_LPF_0_0_0'
	END::text AS metier
	--Mesh size range
	,CASE 
		WHEN en.l_engin = 'Canneur' THEN 'NA'
		WHEN en.l_engin = 'Senneur' THEN 'NK'
		ELSE 'NK'
	END::text AS mesh_size_range
	--Ocean
	,CASE
		WHEN o.l_ocea='Atlantique' THEN 'ATL'
		WHEN o.l_ocea='Indien' THEN 'IND'
		ELSE 'NK'
	END::text AS ocean
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
	--School type
	,CASE
		WHEN tb.l4c_tban='BL' THEN 'FSC'
		WHEN tb.l4c_tban='BO' THEN 'FOB'
		WHEN tb.l4c_tban='IND' THEN 'UNK'
		ELSE 'UNKN'
	END::text AS schooltype
	,e.c_esp_3l::text AS species
	,c.v_poids_capt::numeric AS totwghtlandg
	--,ROUND(sum(c.v_poids_capt), 3)::numeric AS totwghtlandg
	,count(DISTINCT (act.c_bat, act.d_act, act.n_act))::integer AS act_number
FROM
	public.activite act
	JOIN public.capture c
		USING (c_bat, d_act, n_act)
	JOIN public.espece e 
		USING (c_esp)
	JOIN public.a_pays_d p
		USING (c_pays_d)
	JOIN public.engin en
		USING (c_engin)
	JOIN public.ocean o
		USING (c_ocea)	
	JOIN public.type_banc tb
		USING (c_tban)
	JOIN public.zfao zf
		USING (id_zfao)
	JOIN public.bateau b
		USING (c_bat)
WHERE
	--p.l3c_pays_d='FRA' 
	b.c_pav_b IN (1,41)
	AND	EXTRACT(YEAR FROM act.d_act) IN (2015, 2016, 2017)
	AND e.c_esp_3l IN ('BET', 'SKJ', 'YFT')
GROUP BY
	zf.c_dfao
	,p.l3c_pays_d
	,EXTRACT(YEAR FROM act.d_act)
	--,EXTRACT(QUARTER FROM act.d_act)
	,en.l_engin
	,o.l_ocea
	,tb.l4c_tban
	,e.c_esp_3l
	,en.l_engin
	,c.v_poids_capt
ORDER BY
	zf.c_dfao
	,p.l3c_pays_d
	,EXTRACT(YEAR FROM act.d_act)
	--,EXTRACT(QUARTER FROM act.d_act)
	,en.l_engin
	,o.l_ocea
	,tb.l4c_tban
	,e.c_esp_3l
)
SELECT
	q1.country
	,q1.year
	,q1.metier
	,q1.ocean
	--Domain
	,q1.country || '_all_' || q1.sub_region || '_' || q1.gear_type || '_LPF_' || q1.mesh_size_range || '_0_' || 'NA_' || 'all_' || 'all_' || q1.schooltype::text AS domain_discards
	,q1.schooltype
	,q1.species
	,ROUND(sum(q1.totwghtlandg), 3)::numeric AS totwghtlandg
	,sum(q1.act_number)::integer AS act_number
FROM query1 q1
GROUP BY
	q1.country
	,q1.year
	,q1.metier
	,q1.ocean
	,q1.country || '_all_' || q1.sub_region || '_' || q1.gear_type || '_LPF_' || q1.mesh_size_range || '_0_' || 'NA_' || 'all_' || 'all_' || q1.schooltype
	,q1.schooltype
	,q1.species
;
