WITH vessel AS (
SELECT bateau.c_bat
FROM bateau
	JOIN type_bateau
		USING (c_typ_b)
WHERE
	type_bateau.c_typ_b IN (4, 5, 6)
)
SELECT
	--Declaring country 
	p.l3c_pays_d::text AS country
	--Year
	,EXTRACT(YEAR FROM act.d_act)::integer AS year
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
	--Total catch
	--,sum(c.v_poids_capt)::numeric AS balbayacatchwt
	--Number of activity , act.d_act, act.n_act
	--,count(DISTINCT (act.c_bat, act.d_act, act.n_act))::integer AS act_number
	,sum(act.v_nb_calees)::integer AS act_number
	
FROM
	public.activite act
	JOIN public.a_pays_d p
		USING (c_pays_d)
	JOIN public.engin en
		USING (c_engin)
	JOIN public.ocean o
		USING (c_ocea)
	JOIN public.type_banc tb
		USING (c_tban)	
	--JOIN public.capture c
		--USING (c_bat, d_act, n_act)
	JOIN vessel
		USING (c_bat)
WHERE
	p.l3c_pays_d='FRA'
	AND EXTRACT(YEAR FROM act.d_act) IN (2015, 2016, 2017)
	AND act.c_bat IN (vessel.c_bat)
	AND act.c_opera IN (0,1,2,14)
GROUP BY
	p.l3c_pays_d
	,EXTRACT(YEAR FROM act.d_act)
	,en.l_engin
	,o.l_ocea
	,tb.l4c_tban
ORDER BY
	p.l3c_pays_d
	,EXTRACT(YEAR FROM act.d_act)
	,en.l_engin
	,o.l_ocea
	,tb.l4c_tban
;
