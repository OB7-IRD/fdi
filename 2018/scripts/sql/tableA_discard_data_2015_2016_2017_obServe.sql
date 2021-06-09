WITH fao_zone_sub_qry1 AS (
SELECT
	f_code,
	regexp_split_to_array(f_code, '\.') AS arr
FROM
	public.zfao_areas
WHERE
	f_level IN ('MAJOR', 'SUBAREA','DIVISION') 
),
fao_zone_sub_qry2 AS (
SELECT
	f_code,
	CASE
		WHEN array_length(arr, 1)=1 THEN NULL
		WHEN array_length(arr, 1)=2 THEN arr[1]
		WHEN array_length(arr, 1)=3 THEN arr[1] || '.' || arr[2]
	END AS f_code_parent
FROM
	fao_zone_sub_qry1
),
fao_zone_qry AS (
SELECT 
	f_code 
FROM 
	fao_zone_sub_qry2 
WHERE 
	f_code NOT IN (SELECT DISTINCT f_code_parent FROM fao_zone_sub_qry2 WHERE f_code_parent IS NOT NULL)
),
stratum_sub_qry AS (
SELECT
	observe_seine.set.topiaid as set,
	country.iso3code AS country,
	'PS'::text AS fishing_tech,
	'PS'::text AS gear_type,
	'PS_LPF_0_0_0'::text AS metier,
	CASE 
		WHEN ocean.label2='Atlantique' THEN 'ATL'
		WHEN ocean.label2='Indien' THEN 'IND'
		ELSE 'NK'
	END::text AS ocean,
	CASE 
		WHEN observe_seine.set.schooltype=2 THEN 'FSC'
		WHEN observe_seine.set.schooltype=1 THEN 'FOB'
		WHEN observe_seine.set.schooltype=0 THEN 'UNK'
		ELSE 'UNKN'
	END::text AS schooltype
FROM
	observe_seine.set
	INNER JOIN observe_seine.activity ON (observe_seine.activity.set=observe_seine.set.topiaid)
	INNER JOIN observe_seine.route ON (observe_seine.activity.route = observe_seine.route.topiaid)
	INNER JOIN observe_seine.trip ON (observe_seine.route.trip=observe_seine.trip.topiaid)
	INNER JOIN observe_common.program ON (observe_seine.trip.program=observe_common.program.topiaid)
	INNER JOIN observe_common.ocean ON (observe_seine.trip.ocean=observe_common.ocean.topiaid)
	INNER JOIN observe_common.vessel ON (observe_seine.trip.vessel=observe_common.vessel.topiaid)
	INNER JOIN observe_common.country ON (observe_common.vessel.flagcountry=country.topiaid)
WHERE
	observe_common.program.topiaid IN ('fr.ird.observe.entities.referentiel.Program#1239832686262#0.31033946454061234', 'fr.ird.observe.entities.referentiel.Program#1308048349668#0.7314513252652438', 'fr.ird.observe.entities.referentiel.Program#1363095174385#0.011966550987014823', 'fr.ird.observe.entities.referentiel.Program#1373642516190#0.998459307142491') -- DCF (IRD), DCF (TAAF), Moratoire ICCAT 2013-? (IRD), OCUP
),
stratum_qry AS (
SELECT
	observe_seine.set.topiaid AS set
	,stratum_sub_qry.country::text AS country
	,stratum_sub_qry.ocean::text AS ocean
	,extract(year FROM observe_seine.route.date)::integer AS year
	,extract(quarter FROM observe_seine.route.date)::integer as quarter
	,'VL40XX'::text AS vessel_length
	,stratum_sub_qry.fishing_tech::text AS fishing_tech
	,stratum_sub_qry.gear_type::text AS gear_type
	,'NK'::text AS mesh_size_range
	,stratum_sub_qry.metier::text AS metier
	,stratum_sub_qry.schooltype::text AS schooltype
	,'OFR'::text AS supra_region
	,CASE
		WHEN public.zfao_areas.f_code='34.1.3' THEN '34.1.3'
		WHEN public.zfao_areas.f_code='34.3.1' THEN '34.3.1'
		WHEN public.zfao_areas.f_code='34.3.2' THEN '34.3.2'
		WHEN public.zfao_areas.f_code='34.3.3' THEN '34.3.3'
		WHEN public.zfao_areas.f_code='34.3.4' THEN '34.3.4'
		WHEN public.zfao_areas.f_code='34.3.5' THEN '34.3.5'
		WHEN public.zfao_areas.f_code='34.3.6' THEN '34.3.6'
		WHEN public.zfao_areas.f_code='34.4.1' THEN '34.4.1'
		WHEN public.zfao_areas.f_code='34.4.2' THEN '34.4.2'
		WHEN public.zfao_areas.f_code='41.1.4' THEN '41.1'
		WHEN public.zfao_areas.f_code='47.1.1' THEN '47.1'
		WHEN public.zfao_areas.f_code='47.1.2' THEN '47.1'
		WHEN public.zfao_areas.f_code='47.A.0' THEN '47.A'
		WHEN public.zfao_areas.f_code='47.A.1' THEN '47.A'
		WHEN public.zfao_areas.f_code='47.B.1' THEN '47.B'
		WHEN public.zfao_areas.f_code='51.3' THEN '51.3'
		WHEN public.zfao_areas.f_code='51.4' THEN '51.4'
		WHEN public.zfao_areas.f_code='51.5' THEN '51.5'
		WHEN public.zfao_areas.f_code='51.6' THEN '51.6'
		WHEN public.zfao_areas.f_code='51.7' THEN '51.7'
		WHEN public.zfao_areas.f_code='57.1' THEN '57.1'
		WHEN public.zfao_areas.f_code='57.2' THEN '57.2'
		ELSE 'ATTENTION PROB'
	END::text AS sub_region
	,CASE
		WHEN public.zfao_areas.f_code='34.1.3' THEN 'RFMO'
		WHEN public.zfao_areas.f_code='34.3.1' THEN 'NA'
		WHEN public.zfao_areas.f_code='34.3.2' THEN 'NA'
		WHEN public.zfao_areas.f_code='34.3.3' THEN 'NA'
		WHEN public.zfao_areas.f_code='34.3.4' THEN 'NA'
		WHEN public.zfao_areas.f_code='34.3.5' THEN 'NA'
		WHEN public.zfao_areas.f_code='34.3.6' THEN 'NA'
		WHEN public.zfao_areas.f_code='34.4.1' THEN 'NA'
		WHEN public.zfao_areas.f_code='34.4.2' THEN 'NA'
		WHEN public.zfao_areas.f_code='41.1.4' THEN 'NA'
		WHEN public.zfao_areas.f_code='47.1.1' THEN 'NA'
		WHEN public.zfao_areas.f_code='47.1.2' THEN 'NA'
		WHEN public.zfao_areas.f_code='47.A.0' THEN 'NA'
		WHEN public.zfao_areas.f_code='47.A.1' THEN 'NA'
		WHEN public.zfao_areas.f_code='47.B.1' THEN 'NA'
		WHEN public.zfao_areas.f_code='51.3' THEN 'NA'
		WHEN public.zfao_areas.f_code='51.4' THEN 'NA'
		WHEN public.zfao_areas.f_code='51.5' THEN 'NA'
		WHEN public.zfao_areas.f_code='51.6' THEN 'NA'
		WHEN public.zfao_areas.f_code='51.7' THEN 'NA'
		WHEN public.zfao_areas.f_code='57.1' THEN 'NA'
		WHEN public.zfao_areas.f_code='57.2' THEN 'NA'
		ELSE 'ATTENTION PROB'
	END::text AS eez_indicator
	,'IWE'::text AS geo_indicator
	,'NA'::text AS specon_tech
	--(stratum_sub_qry.country || '_' || stratum_sub_qry.ocean || '_' || stratum_sub_qry.fishery || '_' || stratum_sub_qry.schooltype)::text AS domain,
	,'LPF'::text AS target_assemblage
	,'NA'::text AS deep
	,'N'::text AS confidential
FROM
	--Attention il faut revoir les jointures a partir de la table trip
	observe_seine.set
	INNER JOIN observe_seine.activity ON (observe_seine.activity.set=observe_seine.set.topiaid)
	INNER JOIN observe_seine.route ON (observe_seine.activity.route=observe_seine.route.topiaid)
	INNER JOIN public.zfao_areas ON (st_within(observe_seine.activity.the_geom, public.zfao_areas.geom))
	INNER JOIN stratum_sub_qry ON (stratum_sub_qry.set=observe_seine.set.topiaid)
WHERE
	stratum_sub_qry.country='FRA'
	AND stratum_sub_qry.fishing_tech IN ('PS', 'HOK')
	AND observe_seine.route.date >= '2015-01-01' AND  observe_seine.route.date <= '2017-12-31'
	AND public.zfao_areas.f_code IN (SELECT f_code FROM fao_zone_qry)
)
SELECT
	stratum_qry.country
	,stratum_qry.ocean
	,stratum_qry.year
	,stratum_qry.quarter
	,stratum_qry.vessel_length
	,stratum_qry.fishing_tech
	,stratum_qry.gear_type
	,stratum_qry.mesh_size_range
	,stratum_qry.metier
	,stratum_qry.country || '_' || stratum_qry.quarter || '_' || stratum_qry.sub_region || '_' || stratum_qry.gear_type || '_LPF_' || stratum_qry.mesh_size_range || '_0_' || 'NA_' || 'all_' || 'all_' || stratum_qry.schooltype::text AS domain
	,stratum_qry.supra_region
	,stratum_qry.sub_region
	,stratum_qry.eez_indicator
	,stratum_qry.geo_indicator
	,stratum_qry.specon_tech
	,stratum_qry.target_assemblage
	,stratum_qry.deep
	,observe_common.species.faocode::text AS species
	,sum(observe_seine.targetlength.weight * observe_seine.targetlength.count)::numeric/1000 AS unwanted_catch
	,stratum_qry.confidential
FROM
	observe_seine.targetlength
	INNER JOIN observe_common.species ON (observe_seine.targetlength.species=observe_common.species.topiaid)
	INNER JOIN observe_seine.targetsample ON (observe_seine.targetlength.targetsample=observe_seine.targetsample.topiaid)
	INNER JOIN observe_seine.set ON (observe_seine.targetsample.set=observe_seine.set.topiaid)
	INNER JOIN stratum_qry ON (stratum_qry.set=observe_seine.set.topiaid)
WHERE
	--Just for corrected species
	observe_common.species.faocode IN ('BET', 'SKJ', 'YFT')
GROUP BY
	stratum_qry.country
	,stratum_qry.ocean
	,stratum_qry.year
	,stratum_qry.quarter
	,stratum_qry.vessel_length
	,stratum_qry.fishing_tech
	,stratum_qry.gear_type
	,stratum_qry.mesh_size_range
	,stratum_qry.metier
	,stratum_qry.country || '_' || stratum_qry.quarter || '_' || stratum_qry.sub_region || '_' || stratum_qry.gear_type || '_LPF_' || stratum_qry.mesh_size_range || '_0_' || 'NA_' || 'all_' || 'all_' || stratum_qry.schooltype
	,stratum_qry.supra_region
	,stratum_qry.sub_region
	,stratum_qry.eez_indicator
	,stratum_qry.geo_indicator
	,stratum_qry.specon_tech
	,stratum_qry.target_assemblage
	,stratum_qry.deep
	,observe_common.species.faocode
	,stratum_qry.confidential
;
