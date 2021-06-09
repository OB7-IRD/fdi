SELECT
	f.c_pays_fao::text AS country
	,EXTRACT(YEAR FROM c.d_act)::integer AS year
	,EXTRACT(quarter FROM c.d_act)::integer AS quarter
	,v.v_l_ht::numeric AS vessel_length
	,g.c_engin::text AS engin
	,act.c_tban::integer AS fishing_mode
	,act.cwp55_act AS cwp
	,sp.c_esp_3l::text AS species
	,round(c.v_poids_capt, 3)::numeric AS totwghtlandg
FROM
	public.capture c
	INNER JOIN public.bateau v ON (c.c_bat=v.c_bat)
	INNER JOIN public.pavillon f ON (f.c_pav_b=v.c_pav_b)
	INNER JOIN public.type_bateau vt ON (v.c_typ_b=vt.c_typ_b)
	INNER JOIN public.engin g ON (vt.c_engin=g.c_engin)
	INNER JOIN public.activite act ON (act.c_bat=c.c_bat AND act.d_act=c.d_act AND act.n_act=c.n_act)
	INNER JOIN public.espece sp ON (c.c_esp=sp.c_esp)
	INNER JOIN public.type_banc fm ON (act.c_tban=fm.c_tban)
WHERE
	EXTRACT(YEAR FROM c.d_act) IN (?periode)
	AND v.c_pav_b IN (?flag)
	AND g.c_engin IN (?gear)
	-- Without discards
	AND c.c_esp != 8
	AND (c.c_esp NOT BETWEEN 800 AND 899)
	-- Without sharks (extract from observe database)
	AND c.c_esp != 7
;
