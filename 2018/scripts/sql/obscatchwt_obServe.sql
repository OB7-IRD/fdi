SELECT
	--Country
	observe_common.country.iso3code AS country
	--Year
	,EXTRACT(YEAR FROM observe_seine.route.date)::integer AS year
	--Metier
	,'PS_LPF_0_0_0'::text AS metier
	--Ocean
	,CASE 
		WHEN observe_common.ocean.label2='Atlantique' THEN 'ATL'
		WHEN observe_common.ocean.label2='Indien' THEN 'IND'
		ELSE 'NK'
	END::text AS ocean
	--School type
	,CASE 
		WHEN observe_seine.set.schooltype=2 THEN 'FSC'
		WHEN observe_seine.set.schooltype=1 THEN 'FOB'
		WHEN observe_seine.set.schooltype=0 THEN 'UNK'
		ELSE 'UNKN'
	END::text AS schooltype
	--obServe catch weight
	--,sum(observe_seine.targetcatch.catchweight)::numeric AS obscatchweight
	--Number of observation
	,count(DISTINCT observe_seine.set.topiaid) AS obs_act_number
FROM
	observe_seine.targetcatch
	INNER JOIN observe_seine.set ON (observe_seine.targetcatch.set=observe_seine.set.topiaid)
	INNER JOIN observe_seine.activity ON (observe_seine.activity.set=observe_seine.set.topiaid)
	INNER JOIN observe_seine.route ON (observe_seine.activity.route=observe_seine.route.topiaid)
	INNER JOIN observe_seine.trip ON (observe_seine.route.trip=observe_seine.trip.topiaid)
	INNER JOIN observe_common.ocean ON (observe_seine.trip.ocean=observe_common.ocean.topiaid)
	INNER JOIN observe_common.vessel ON (observe_seine.trip.vessel=observe_common.vessel.topiaid)
	INNER JOIN observe_common.country ON (observe_common.vessel.flagcountry=country.topiaid)
WHERE
	observe_common.country.iso3code='FRA' 
	AND	EXTRACT(YEAR FROM observe_seine.route.date) IN (2015, 2016, 2017)
GROUP BY
	observe_common.country.iso3code
	,EXTRACT(YEAR FROM observe_seine.route.date)
	,observe_common.ocean.label2
	,observe_seine.set.schooltype
;
