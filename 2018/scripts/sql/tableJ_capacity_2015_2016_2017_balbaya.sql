SELECT country, year, vessel_length, fishing_tech, supra_region, geo_indicator, vessel_code, vessel_kw,
vessel_gt, vessel_age, vessel_length_m, trips, SUM(trip_time_at_sea_d) AS trip_time_at_sea_d 
FROM
(
SELECT
--Declaring country
pavillon.c_pays_fao::text AS country,
--Year
extract(year FROM a.d_act)::integer AS year,
--Vessel length
CASE
	WHEN v.v_l_ht < 10 THEN 'VL0010'
	WHEN v.v_l_ht >= 10 AND v.v_l_ht < 12 THEN 'VL1012'
	WHEN v.v_l_ht >= 12 AND v.v_l_ht < 18 THEN 'VL1218'
	WHEN v.v_l_ht >= 18 AND v.v_l_ht < 24 THEN 'VL1824'
	WHEN v.v_l_ht >= 24 AND v.v_l_ht < 40 THEN 'VL2440'
	WHEN v.v_l_ht >= 40 THEN 'VL40XX'
	ELSE 'NK'
END::text AS vessel_length,
--Fishing technique
CASE
	WHEN v.c_typ_b IN (4, 5, 6) THEN 'PS'
	WHEN v.c_typ_b IN (1, 2) THEN 'HOK'
END::text AS fishing_tech,
--Supra region
CASE
	WHEN z.c_zfao='27' THEN 'AREA27'
	WHEN z.c_zfao='37' THEN 'AREA37'
	ELSE 'OFR'
END::text AS supra_region,
--Geo indicator
'IWE'::text AS geo_indicator,
-- Vessel details for further usage
v.c_cfr AS vessel_code,
ROUND(v.v_p_cv*0.735499,0) AS vessel_kw,
(0.2+0.2*log(v.v_ct_m3))*v.v_ct_m3 AS vessel_gt,
(extract(year from now()) - v.an_serv) AS vessel_age,
v.v_l_ht AS vessel_length_m,
-- trip details for further usage
a.d_dbq::text || a.c_bat::text  AS trips,
ROUND(a.v_tmer/24,1) AS trip_time_at_sea_d
FROM 
public.activite AS a, 
public.bateau AS v, 
public.type_bateau, 
public.ocean, 
public.pavillon,
public.zfao AS z
WHERE 
a.c_bat = v.c_bat AND
a.c_ocea = ocean.c_ocea AND
v.c_pav_b = pavillon.c_pav_b AND
type_bateau.c_typ_b = v.c_typ_b AND
a.id_zfao = z.id_zfao
AND v.c_pav_b IN (1,41)
AND extract(year FROM d_act) IN (2015,2016,2017)
) AS tableJ_capacity
GROUP BY country, year, vessel_length, fishing_tech, supra_region, geo_indicator, vessel_code, vessel_kw,
vessel_gt, vessel_age, vessel_length_m, trips
;
