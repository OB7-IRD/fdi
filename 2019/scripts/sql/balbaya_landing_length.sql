WITH balbaya_landing AS 
(SELECT
	f.c_pays_fao::text AS country
	,EXTRACT(YEAR FROM c.d_act)::integer AS year
	,EXTRACT(quarter FROM c.d_act)::integer AS quarter
	,act.v_la_act::numeric AS latitude
	,act.v_lo_act::numeric AS longitude
	,CASE
		WHEN g.c_engin = 1 THEN 'PS_LPF_0_0_0'
		WHEN g.c_engin = 2 THEN 'LHP_LPF_0_0_0'
		WHEN g.c_engin = 3 THEN 'LLD_LPF_0_0_0'
	END::text AS metier
	,fm.l4c_tban::text AS fishing_mode
	,CASE
		WHEN v.v_l_ht < 10 THEN 'VL0010'
		WHEN (v.v_l_ht >= 10 AND v.v_l_ht < 12) THEN 'VL1012'
		WHEN (v.v_l_ht >= 12 AND v.v_l_ht < 18) THEN 'VL1218'
		WHEN (v.v_l_ht >= 18 AND v.v_l_ht < 24) THEN 'VL1824'
		WHEN (v.v_l_ht >= 24 AND v.v_l_ht < 40) THEN 'VL2440'
		WHEN (v.v_l_ht >= 40) THEN 'VL40XX'
		ELSE 'NK'
	END::text AS vessel_length
	,sp.c_esp_3l::text AS species
	,round(c.v_poids_capt, 3)::numeric AS totwghtlandg
FROM
	public.capture c
	INNER JOIN public.bateau v ON (c.c_bat=v.c_bat)
	INNER JOIN public.pavillon f ON (f.c_pav_b=v.c_flotte)
	INNER JOIN public.type_bateau vt ON (v.c_typ_b=vt.c_typ_b)
	INNER JOIN public.engin g ON (vt.c_engin=g.c_engin)
	INNER JOIN public.activite act ON (act.c_bat=c.c_bat AND act.d_act=c.d_act AND act.n_act=c.n_act)
	INNER JOIN public.espece sp ON (c.c_esp=sp.c_esp)
	INNER JOIN public.type_banc fm ON (act.c_tban=fm.c_tban)
WHERE
	EXTRACT(YEAR FROM c.d_act) IN (2015, 2016, 2017, 2018)
	-- For the French fleet, 1 = France & 41 = Mayotte
	AND v.c_flotte IN (1, 41)
	-- 1 = PS, 2 = BB and 3 = LL
	AND g.c_engin IN (1, 2, 3)),
balbaya_nbtrip AS 
(SELECT
	f.c_pays_fao::text AS country
	,EXTRACT(YEAR FROM t.d_dbq)::integer AS year
	,EXTRACT(quarter FROM t.d_dbq)::integer AS quarter
	,act.v_la_act::numeric AS latitude
	,act.v_lo_act::numeric AS longitude
	,CASE
		WHEN g.c_engin = 1 THEN 'PS_LPF_0_0_0'
		WHEN g.c_engin = 2 THEN 'LHP_LPF_0_0_0'
		WHEN g.c_engin = 3 THEN 'LLD_LPF_0_0_0'
	END::text AS metier
	,fm.l4c_tban::text AS fishing_mode
	,CASE
		WHEN v.v_l_ht < 10 THEN 'VL0010'
		WHEN (v.v_l_ht >= 10 AND v.v_l_ht < 12) THEN 'VL1012'
		WHEN (v.v_l_ht >= 12 AND v.v_l_ht < 18) THEN 'VL1218'
		WHEN (v.v_l_ht >= 18 AND v.v_l_ht < 24) THEN 'VL1824'
		WHEN (v.v_l_ht >= 24 AND v.v_l_ht < 40) THEN 'VL2440'
		WHEN (v.v_l_ht >= 40) THEN 'VL40XX'
		ELSE 'NK'
	END::text AS vessel_length
	,*
FROM
	public.maree t
	INNER JOIN public.bateau v ON (t.c_bat=v.c_bat)
	INNER JOIN public.pavillon f ON (f.c_pav_b=v.c_pav_b)
	INNER JOIN public.type_bateau vt ON (v.c_typ_b=vt.c_typ_b)
	INNER JOIN public.engin g ON (vt.c_engin=g.c_engin)
WHERE
	EXTRACT(YEAR FROM t.d_dbq) IN (2015, 2016, 2017, 2018)
	-- For the French fleet, 1 = France & 41 = Mayotte
	AND v.c_pav_b IN (1, 41)
	-- 1 = PS, 2 = BB and 3 = LL
	AND g.c_engin IN (1, 2, 3)

)