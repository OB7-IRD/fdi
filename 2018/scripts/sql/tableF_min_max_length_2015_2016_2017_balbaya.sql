SELECT country, year, metier, ocean, schooltype, species, 
min(v_classe_t::integer)::integer AS min_length,
max(v_classe_t::integer)::integer AS max_length
FROM
(
	SELECT
	pavillon.c_pav_iso AS country,
	temps.an AS year,
	CASE
		WHEN g_engin.lc_g_engin='PS' THEN 'PS_LPF_0_0_0'
		WHEN g_engin.lc_g_engin='BB' THEN 'LHP_LPF_0_0_0'
		ELSE 'UNKN'
	END AS metier,
	CASE 
		WHEN ocean.luk_ocean='Atlantic O.' THEN 'ATL'
		WHEN ocean.luk_ocean='Indian O.' THEN 'IND'
		ELSE 'UNKN'
	END AS ocean,
	CASE 
		WHEN banc.luk_banc='FSC' THEN 'FSC'
		WHEN banc.luk_banc='FAD' THEN 'FOB'
		WHEN banc.luk_banc='IND' THEN 'UNK'
		ELSE 'UNKN'
	END AS schooltype,
	espece.lc_esp AS species,
	cl_taille.v_classe_t::integer
	FROM 
	public.mensur, 
	public.ocean, 
	public.banc, 
	public.espece, 
	public.temps, 
	public.cl_taille, 
	public.t_carre, 
	public.carre, 
	public.g_engin, 
	public.engin, 
	public.pavillon, 
	public.type_mens

	WHERE 
	mensur.c_banc = banc.c_banc AND
	mensur.c_esp = espece.c_esp AND
	mensur.id_classe_t = cl_taille.id_classe_t AND
	mensur.id_carre = carre.id_carre AND
	mensur.c_pav = pavillon.c_pav AND
	ocean.c_ocean = mensur.c_ocean AND
	temps.id_date = mensur.id_date AND
	t_carre.c_t_carre = mensur.c_t_carre AND
	g_engin.c_g_engin = mensur.c_g_engin AND
	engin.c_engin = mensur.c_engin AND
	type_mens.c_type_mens = mensur.c_type_mens AND
	temps.an IN (2015, 2016, 2017) AND
	pavillon.c_pav_iso= 'FRA'

) AS tableF_cas
GROUP BY country, year, metier, ocean, schooltype, species
ORDER BY country, year, metier, ocean, schooltype, species
;
