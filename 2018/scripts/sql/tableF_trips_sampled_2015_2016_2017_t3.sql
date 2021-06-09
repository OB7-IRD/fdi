SELECT
	country.codeiso3 AS country,
	extract(YEAR FROM activity.date) AS year,
	CASE 
		WHEN vesselsimpletype.libelle='Senneur' THEN 'PS_LPF_0_0_0'
		WHEN vesselsimpletype.libelle='Canneur' THEN 'LHP_LPF_0_0_0'
		ELSE 'UNKN'
	END AS metier,
	CASE 
		WHEN ocean.libelle='Atlantique' THEN 'ATL'
		WHEN ocean.libelle='Indien' THEN 'IND'
		ELSE 'UNKN'
	END AS ocean,
	CASE 
		WHEN schooltype.libelle4='BL' THEN 'FSC'
		WHEN schooltype.libelle4='BO' THEN 'FOB'
		WHEN schooltype.libelle4='IND' THEN 'UNK'
		ELSE 'UNKN'
	END AS schooltype,
	COUNT(DISTINCT trip.topiaid) AS no_samples_landg
FROM
	activity
	INNER JOIN schooltype ON activity.schooltype=schooltype.topiaid
	INNER JOIN trip ON activity.trip=trip.topiaid
	INNER JOIN ocean ON activity.ocean=ocean.topiaid
	INNER JOIN sample ON sample.trip=trip.topiaid
	INNER JOIN vessel ON trip.vessel=vessel.topiaid
	INNER JOIN vesseltype ON vessel.vesseltype=vesseltype.topiaid
	INNER JOIN vesselsimpletype ON vesseltype.vesselsimpletype=vesselsimpletype.topiaid
	INNER JOIN country ON vessel.flagcountry=country.topiaid
WHERE
	country.codeiso3='FRA' 
	AND	extract(YEAR FROM activity.date) IN (2015, 2016, 2017)
GROUP BY
	country.codeiso3,	
	extract(YEAR FROM activity.date),
	vesselsimpletype.libelle,
	ocean.libelle,
	schooltype.libelle4
;
