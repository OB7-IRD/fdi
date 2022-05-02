SELECT 
	sub1.country
	,sub1.year
	,sub1.vessel_length
	,sub1.fishing_tech
	,sub1.supra_region
	,sub1.geo_indicator
	,count(*) AS totves
	,avg(vessel_age) AS avgage
	,avg(vessel_length_m) AS avgloa
FROM 
(SELECT DISTINCT
	f.c_pays_fao::text AS country
	,EXTRACT(YEAR FROM t.d_dbq)::integer AS year
	,CASE
		WHEN v.v_l_ht < 10 THEN 'VL0010'
		WHEN (v.v_l_ht >= 10 AND v.v_l_ht < 12) THEN 'VL1012'
		WHEN (v.v_l_ht >= 12 AND v.v_l_ht < 18) THEN 'VL1218'
		WHEN (v.v_l_ht >= 18 AND v.v_l_ht < 24) THEN 'VL1824'
		WHEN (v.v_l_ht >= 24 AND v.v_l_ht < 40) THEN 'VL2440'
		WHEN (v.v_l_ht >= 40) THEN 'VL40XX'
		ELSE 'NK'
	END::text AS vessel_length
	,CASE
		WHEN g.c_engin = 1 THEN 'PS'
		ELSE 'HOK'
	END::text AS fishing_tech
	,'OFR'::text AS supra_region
	,'IWE'::text AS geo_indicator
	-- Age of vessel
	,(EXTRACT(YEAR FROM t.d_dbq) - v.an_serv)::integer AS vessel_age
	-- Length over all of the vessel
	,v.v_l_ht::numeric AS vessel_length_m
FROM
	public.maree t
	INNER JOIN public.bateau v ON (t.c_bat=v.c_bat)
	INNER JOIN public.pavillon f ON (f.c_pav_b=v.c_pav_b)
	INNER JOIN public.type_bateau vt ON (v.c_typ_b=vt.c_typ_b)
	INNER JOIN public.engin g ON (vt.c_engin=g.c_engin)
WHERE
	EXTRACT(YEAR FROM t.d_dbq) IN (?periode)
	AND v.c_pav_b IN (?flag)
	AND g.c_engin IN (?gear)
ORDER BY
	year) AS sub1
GROUP BY
	country
	,year
	,vessel_length
	,fishing_tech
	,supra_region
	,geo_indicator
ORDER BY
	year
	,fishing_tech
