SELECT
	f.c_pays_fao::text AS country
	,EXTRACT(YEAR FROM act.d_act)::integer AS year
	,EXTRACT(quarter FROM act.d_act)::integer AS quarter
	,v.v_l_ht::numeric AS vessel_length
	,g.c_engin::integer AS gear
	,act.v_la_act::numeric AS latitude
	,act.v_lo_act::numeric AS longitude
	,act.c_bat::text AS vessel_id
	,act.v_tmer::numeric AS hrsea
	,v.v_p_cv::numeric AS engine_power
	,act.c_ocea::integer AS ocean
	,v.v_ct_m3::numeric AS vessel_volume_m3
	,act.v_tpec::numeric AS fishing_time
FROM
	public.activite act
	INNER JOIN public.bateau v ON (act.c_bat=v.c_bat)
	INNER JOIN public.pavillon f ON (f.c_pav_b=v.c_pav_b)
	INNER JOIN public.type_bateau vt ON (v.c_typ_b=vt.c_typ_b)
	INNER JOIN public.engin g ON (vt.c_engin=g.c_engin)
WHERE
	EXTRACT(YEAR FROM act.d_act) IN (?periode)
	AND v.c_pav_b IN (?flag)
	AND g.c_engin IN (?gear)
;
