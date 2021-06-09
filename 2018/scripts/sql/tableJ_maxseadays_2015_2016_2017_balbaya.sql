SELECT
	rq1.country,
	rq1.year,
	rq1.vessel_length,
	rq1.fishing_tech,
	rq1.supra_region,
	rq1.geo_indicator,
	rq1.c_bat,
	sum(round(rq1.maxseadays::numeric, 2)) as maxseadays
FROM (
	SELECT 
		--Declaring country 
		p.l3c_pays_d::text AS country
		--Year
		,extract(YEAR FROM act.d_act)::integer AS year
		--Vessel length
		,CASE
			WHEN b.v_l_ht < 10 THEN 'VL0010'
			WHEN b.v_l_ht >= 10 AND b.v_l_ht < 12 THEN 'VL1012'
			WHEN b.v_l_ht >= 12 AND b.v_l_ht < 18 THEN 'VL1218'
			WHEN b.v_l_ht >= 18 AND b.v_l_ht < 24 THEN 'VL1824'
			WHEN b.v_l_ht >= 24 AND b.v_l_ht < 40 THEN 'VL2440'
			WHEN b.v_l_ht >= 40 THEN 'VL40XX'
			ELSE 'NK'
		END::text AS vessel_length
		--Fishing technique
		,CASE
			WHEN b.c_typ_b IN (4, 5, 6) THEN 'PS'
			WHEN b.c_typ_b IN (1, 2) THEN 'HOK'
		END::text AS fishing_tech
		--Supra region
		,CASE
			WHEN zf.c_zfao='27' THEN 'AREA27'
			WHEN zf.c_zfao='37' THEN 'AREA37'
			ELSE 'OFR'
		END::text AS supra_region
		--Geo indicator
		,'IWE'::text AS geo_indicator
		-- ClÃ© activitÃ© (to be deleted after aggregation at ZFAO level)
		,m.c_bat
		,act.d_act
		--a.n_act,
		-- TOTSEADAYS
		,act.v_tmer/24::numeric as maxseadays
	FROM
		maree m
		JOIN public.a_pays_d p
			USING (c_pays_d)
		JOIN public.activite act
			ON (act.c_bat=m.c_bat AND act.d_dbq=m.d_dbq)
		JOIN public.bateau b
			ON (m.c_bat=b.c_bat)
		JOIN public.zfao zf
			USING (id_zfao)
	WHERE
		extract(YEAR FROM act.d_act) IN (2015, 2016, 2017)
		AND b.c_typ_b IN (1, 2, 4, 5, 6, 7)	-- Seiners, longliners, line hand pole
) AS rq1
GROUP BY
	rq1.country,
	rq1.year,
	rq1.vessel_length,
	rq1.fishing_tech,
	rq1.supra_region,
	rq1.geo_indicator,
	rq1.c_bat
ORDER BY 
	rq1.country,
	rq1.year,
	rq1.vessel_length,
	rq1.fishing_tech,
	rq1.supra_region,
	rq1.geo_indicator,
	sum(round(rq1.maxseadays::numeric, 2)) DESC
;
