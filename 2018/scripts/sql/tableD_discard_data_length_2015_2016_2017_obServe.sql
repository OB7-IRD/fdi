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
query1 AS (
SELECT
	observe_common.country.iso3code AS country,
	extract(year FROM observe_seine.route.date)::integer AS year,
	'PS_LPF_0_0_0'::text AS metier,
	CASE 
		WHEN observe_common.ocean.label2='Atlantique' THEN 'ATL'
		WHEN observe_common.ocean.label2='Indien' THEN 'IND'
		ELSE 'UNKN'
	END::text AS ocean,
	CASE 
		WHEN observe_seine.set.schooltype=2 THEN 'FSC'
		WHEN observe_seine.set.schooltype=1 THEN 'FOB'
		WHEN observe_seine.set.schooltype=0 THEN 'UNK'
		ELSE 'UNKN'
	END::text AS schooltype,
	observe_common.species.faocode::text AS species,
	sum(observe_seine.targetlength.weight * observe_seine.targetlength.count)::NUMERIC / 1000 AS unwanted_catch,
	count(DISTINCT observe_seine.trip.topiaid)::numeric AS no_samples_uc,
	sum(observe_seine.targetlength.count)::NUMERIC AS no_length_measurements_uc,
	min(observe_seine.targetlength.length)::integer AS min_length,
	max(observe_seine.targetlength.length)::integer AS max_length,
	'OFR'::text AS supra_region,
	--public.zfao_areas.f_code AS sub_region
	CASE
		WHEN public.zfao_areas.f_code='34.1.3' THEN '34.1.3'
		WHEN public.zfao_areas.f_code='34.3.1' THEN '34.3.1'
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
		WHEN public.zfao_areas.f_code='51.3' THEN '51.3'
		WHEN public.zfao_areas.f_code='51.4' THEN '51.4'
		WHEN public.zfao_areas.f_code='51.5' THEN '51.5'
		WHEN public.zfao_areas.f_code='51.6' THEN '51.6'
		WHEN public.zfao_areas.f_code='51.7' THEN '51.7'
		WHEN public.zfao_areas.f_code='57.1' THEN '57.1'
		WHEN public.zfao_areas.f_code='57.2' THEN '57.2'
		ELSE 'ATTENTION PROB'
	END::text AS sub_region,
	CASE
		WHEN public.zfao_areas.f_code='34.1.3' THEN 'RFMO'
		WHEN public.zfao_areas.f_code='34.3.1' THEN 'NA'
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
		WHEN public.zfao_areas.f_code='51.3' THEN 'NA'
		WHEN public.zfao_areas.f_code='51.4' THEN 'NA'
		WHEN public.zfao_areas.f_code='51.5' THEN 'NA'
		WHEN public.zfao_areas.f_code='51.6' THEN 'NA'
		WHEN public.zfao_areas.f_code='51.7' THEN 'NA'
		WHEN public.zfao_areas.f_code='57.1' THEN 'NA'
		WHEN public.zfao_areas.f_code='57.2' THEN 'NA'
		ELSE 'ATTENTION PROB'
	END::text AS eez_indicator,
	'PS'::text AS gear_type,
	'NK'::text AS mesh_size_range
FROM
	observe_seine.targetlength
	INNER JOIN observe_common.species ON (observe_seine.targetlength.species=observe_common.species.topiaid)
	INNER JOIN observe_seine.targetsample ON (observe_seine.targetlength.targetsample=observe_seine.targetsample.topiaid)
	INNER JOIN observe_seine.set ON (observe_seine.targetsample.set=observe_seine.set.topiaid)
	INNER JOIN observe_seine.activity ON (observe_seine.activity.set=observe_seine.set.topiaid)
	INNER JOIN observe_seine.route ON (observe_seine.activity.route=observe_seine.route.topiaid)
	INNER JOIN observe_seine.trip ON (observe_seine.route.trip=observe_seine.trip.topiaid)
	INNER JOIN observe_common.program ON (observe_seine.trip.program=observe_common.program.topiaid)
	INNER JOIN observe_common.ocean ON (observe_seine.trip.ocean=observe_common.ocean.topiaid)
	INNER JOIN observe_common.vessel ON (observe_seine.trip.vessel=observe_common.vessel.topiaid)
	INNER JOIN observe_common.country ON (observe_common.vessel.flagcountry=country.topiaid)
	INNER JOIN public.zfao_areas ON (st_within(observe_seine.activity.the_geom, public.zfao_areas.geom))
WHERE
	observe_common.country.iso3code='FRA' 
	AND	extract(year FROM observe_seine.route.date) IN (2015, 2016, 2017)
	AND observe_common.species.faocode IN ('BET', 'SKJ', 'YFT')
	AND observe_seine.targetsample.discarded IS TRUE
	AND observe_common.program.topiaid IN ('fr.ird.observe.entities.referentiel.Program#1239832686262#0.31033946454061234', 'fr.ird.observe.entities.referentiel.Program#1308048349668#0.7314513252652438', 'fr.ird.observe.entities.referentiel.Program#1363095174385#0.011966550987014823', 'fr.ird.observe.entities.referentiel.Program#1373642516190#0.998459307142491') -- DCF (IRD), DCF (TAAF), Moratoire ICCAT 2013-? (IRD), OCUP
	AND public.zfao_areas.f_code IN (SELECT f_code FROM fao_zone_qry)
GROUP BY
	observe_common.country.iso3code,
	extract(year FROM observe_seine.route.date),
	observe_common.ocean.label2,
	observe_seine.set.schooltype,
	observe_common.species.faocode,
	public.zfao_areas.f_code
)
SELECT 
	q1.country,
	q1.year,
	q1.metier,
 	q1.ocean,
	q1.schooltype,
	q1.species,
	q1.unwanted_catch,
	q1.no_samples_uc,
	q1.no_length_measurements_uc,
	q1.min_length,
	q1.max_length,
	q1.supra_region,
	q1.sub_region,
	q1.eez_indicator,
	q1.country || '_all_' || q1.sub_region || '_' || q1.gear_type || '_LPF_' || q1.mesh_size_range || '_0_' || 'NA_' || 'all_' || 'all_' || q1.schooltype::text AS domain_discards
FROM
	query1 q1
;
