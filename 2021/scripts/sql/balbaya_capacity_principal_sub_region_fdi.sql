SELECT
	f.c_pays_fao::text AS country
	,extract(year from act.d_dbq)::integer AS year
	,act.c_bat::integer AS vessel_id
	,CASE
		WHEN v.v_l_ht < 10 THEN 'VL0010'
		WHEN (v.v_l_ht >= 10 AND v.v_l_ht < 12) THEN 'VL1012'
		WHEN (v.v_l_ht >= 12 AND v.v_l_ht < 18) THEN 'VL1218'
		WHEN (v.v_l_ht >= 18 AND v.v_l_ht < 24) THEN 'VL1824'
		WHEN (v.v_l_ht >= 24 AND v.v_l_ht < 40) THEN 'VL2440'
		WHEN (v.v_l_ht >= 40) THEN 'VL40XX'
		ELSE 'NK'
	END::text AS vessel_length
	,act.v_la_act::numeric AS latitude
	,act.v_lo_act::numeric AS longitude
	,act.c_ocea::integer AS ocean
	,act.v_tpec::numeric AS fishing_time
FROM
	public.activite act
	INNER JOIN public.bateau v ON (act.c_bat=v.c_bat)
	INNER JOIN public.pavillon f ON (f.c_pav_b=v.c_pav_b)
	INNER JOIN public.type_bateau vt ON (v.c_typ_b=vt.c_typ_b)
	INNER JOIN public.engin g ON (vt.c_engin=g.c_engin)
WHERE
	EXTRACT(YEAR FROM act.d_dbq) IN (?periode)
	AND v.c_pav_b IN (?flag)
	AND g.c_engin IN (?gear)
