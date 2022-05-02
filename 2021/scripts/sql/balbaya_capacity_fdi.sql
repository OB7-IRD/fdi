WITH balbaya_trip AS 
(SELECT
	f.c_pays_fao::text AS country
	,EXTRACT(YEAR FROM t.d_dbq)::integer AS YEAR
	,t.c_bat::integer AS vessel_id
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
	-- Number of trips	
	,count(*)::integer AS tottrips
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
GROUP BY
	country
	,year
	,vessel_id
	,vessel_length
	,fishing_tech
	,supra_region
	,geo_indicator
ORDER BY
	year
	,fishing_tech),
balbaya_capacity_effort AS 
(SELECT DISTINCT
	f.c_pays_fao::text AS country
	,EXTRACT(YEAR FROM t.d_dbq)::integer AS year
	,t.c_bat::integer AS vessel_id 
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
	-- Fishing capacity = engine power * 0.735499
	,(v.v_p_cv * 0.735499)::numeric AS totkw
	-- Fising capacity in Gross Tonnage (GT)
	-- GT = K.V with K = 0.2 + 0.02 * log10(V)
	-- V volume in m3
	,((0.2 + 0.02 * log(v.v_ct_m3)) * v.v_ct_m3)::numeric AS totgt
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
	year
	,vessel_id)
SELECT
	*
FROM
	balbaya_trip bt
	INNER JOIN balbaya_capacity_effort bce USING ("country", "year", "vessel_id", "vessel_length", "fishing_tech", "supra_region", "geo_indicator")
;
