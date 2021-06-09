SELECT
	p.c_pav_iso::text AS country
	,t.an::integer AS YEAR
	,t.n_trim::integer AS quarter
	,CASE
		WHEN m.c_g_engin = 1 THEN 'VL2440'
		WHEN m.c_g_engin = 2 THEN 'VL40XX'
		ELSE 'Prob'
	END::text AS vessel_length
	,loc.c_cwp5::integer AS cwp
	,CASE
		WHEN m.c_g_engin = 1 THEN 'LHP_LPF_0_0_0'
		WHEN m.c_g_engin = 2 THEN 'PS_LPF_0_0_0'
		WHEN m.c_g_engin = 3 THEN 'LLD_LPF_0_0_0'
	END::text AS metier
	,CASE
		WHEN m.c_banc = 1 THEN 'FOB'
		WHEN m.c_banc = 2 THEN 'FSC'
		WHEN m.c_banc IN (3, 9) THEN 'UNK'
	END::text AS fishing_mode
	,sp.lc_esp::text AS species
	,clt.v_classe_t::integer AS length
	,m.v_mensur::numeric AS no_length
FROM
	public.mensur m
	INNER JOIN public.temps t ON (m.id_date=t.id_date)
	INNER JOIN public.pavillon p ON (m.c_pav=p.c_pav)
	INNER JOIN public.espece sp ON (m.c_esp=sp.c_esp)
	INNER JOIN public.carre loc ON (m.id_carre=loc.id_carre)
	INNER JOIN public.banc tb ON (m.c_banc=tb.c_banc)
	INNER JOIN public.cl_taille clt ON (m.id_classe_t=clt.id_classe_t)
WHERE
	t.an IN (2015, 2016, 2017, 2018)
	-- For the French fleet, 1 = France & 41 = Mayotte
	AND m.c_pav IN (1, 41)
	-- 1 = PS, 2 = BB and 3 = LL 
	AND m.c_g_engin IN (1, 2, 3)
;
