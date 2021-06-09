WITH balbaya_maxseadays AS 
(SELECT
	sub1.country
	,sub1.year
	,sub1.vessel_length
	,sub1.fishing_tech
	,sub1.supra_region
	,sub1.geo_indicator
	,sub1.vessel_id
	,sub1.totseadays
	,rank()
OVER 
(PARTITION BY
	country
	,year
	,vessel_length
	,fishing_tech
	,supra_region
	,geo_indicator
ORDER BY 
	totseadays DESC
)
FROM 
(SELECT
	f.c_pays_fao::text AS country
	,EXTRACT(YEAR FROM act.d_dbq)::integer AS year
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
	,act.c_bat AS vessel_id
	-- Days at sea
	,sum(act.v_tmer / 24)::numeric as totseadays
FROM
	public.activite act
	INNER JOIN public.bateau v ON (act.c_bat=v.c_bat)
	INNER JOIN public.pavillon f ON (f.c_pav_b=v.c_pav_b)
	INNER JOIN public.type_bateau vt ON (v.c_typ_b=vt.c_typ_b)
	INNER JOIN public.engin g ON (vt.c_engin=g.c_engin)
WHERE
	EXTRACT(YEAR FROM act.d_dbq) IN (2015, 2016, 2017, 2018, 2019)
	-- For the French fleet, 1 = France & 41 = Mayotte
	AND v.c_pav_b IN (1, 41)
	-- 1 = PS, 2 = BB and 3 = LL
	AND g.c_engin IN (1, 2, 3)
GROUP BY
	country
	,year
	,vessel_length
	,fishing_tech
	,supra_region
	,geo_indicator
	,vessel_id) AS sub1),
balbaya_maxseadays_f AS 
(SELECT
	msd.country
	,msd.year
	,msd.vessel_length
	,msd.fishing_tech
	,msd.supra_region
	,msd.geo_indicator
	,msd.totseadays
FROM
	balbaya_maxseadays msd
WHERE
	msd.rank <= 10)
SELECT
	msdf.country
	,msdf.year
	,msdf.vessel_length
	,msdf.fishing_tech
	,msdf.supra_region
	,msdf.geo_indicator
	,avg(msdf.totseadays)::numeric AS maxseadays
FROM
	balbaya_maxseadays_f msdf
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
;
