SELECT
	p.c_pav_iso::text AS country
	,t.an::integer AS year
	,t.n_trim::integer AS quarter
	,m.c_g_engin::numeric AS gear
	,loc.c_cwp5::integer AS cwp
	,m.c_banc::integer AS fishing_mode
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
	t.an IN (?periode)
	AND m.c_pav IN (?flag)
	AND m.c_g_engin IN (?gear)
;
