SELECT
	f.c_pays_fao::text AS country
	,EXTRACT(YEAR FROM act.d_act)::integer AS year
	,EXTRACT(quarter FROM act.d_act)::integer AS quarter
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
	,CASE
		WHEN g.c_engin = 1 THEN 'PS'
		WHEN g.c_engin = 2 THEN 'LHP'
		WHEN g.c_engin = 3 THEN 'LLD'
	END::text AS gear_type
	,'LPF'::text AS target_assemblage
	,CASE
		WHEN g.c_engin = 1 THEN 'NK'
		WHEN g.c_engin IN (2, 3) THEN 'NA'
	END::text AS mesh_size_range
	,CASE
		WHEN g.c_engin = 1 THEN 'PS_LPF_0_0_0'
		WHEN g.c_engin = 2 THEN 'LHP_LPF_0_0_0'
		WHEN g.c_engin = 3 THEN 'LLD_LPF_0_0_0'
	END::text AS metier
	,'OFR'::text AS supra_region
	,act.v_la_act::numeric AS latitude
	,act.v_lo_act::numeric AS longitude
	,'IWE'::text AS geo_indicator
	,'NA'::text AS specon_tech
	,'NA'::text AS deep
	,act.c_bat::text AS vessel_id
	-- Days at sea
	,(act.v_tmer / 24)::numeric as totseadays
	-- Fishing effort in kW-days = days at sea * engine power * 0.735499
	-- 1 ch = 0.73539875 kW
	,((act.v_tmer / 24) * v.v_p_cv * 0.735499)::numeric AS totkwdaysatsea
	-- Fishing effort in Gross Tonnage (GT) * days at sea
	-- GT = K.V with K = 0.2 + 0.02 * log10(V)
	-- V volume in m3
	,((act.v_tmer / 24) * ((0.2 + 0.02 * log(v.v_ct_m3)) * v.v_ct_m3))::numeric AS totgtdaysatsea
	-- Fishing days
	,CASE
		WHEN act.c_ocea = 1 THEN (act.v_tpec / 12)
		WHEN act.c_ocea = 2 THEN (act.v_tpec / 13)
	END::numeric AS totfishdays
	-- Fishing effort in kW-days =  fishing days * engine power * 0.735499
	,CASE
		WHEN act.c_ocea = 1 THEN ((act.v_tpec / 12) * v.v_p_cv * 0.735499)
		WHEN act.c_ocea = 2 THEN ((act.v_tpec / 13) * v.v_p_cv * 0.735499)
	END::numeric AS totkwfishdays
	-- Fishing effort in GT * fishing days
	,CASE
		WHEN act.c_ocea = 1 THEN ((act.v_tpec / 12) * ((0.2 + 0.02 * log(v.v_ct_m3)) * v.v_ct_m3))
		WHEN act.c_ocea = 2 THEN ((act.v_tpec / 13) * ((0.2 + 0.02 * log(v.v_ct_m3)) * v.v_ct_m3))
	END::numeric AS totgtfishdays
	-- Hours at sea
	,act.v_tmer::numeric AS hrsea
	-- kW hours at sea
	,(act.v_tmer * (v.v_p_cv * 0.735499))::numeric AS kwhrsea
	-- GT hours at sea
	,(act.v_tmer * ((0.2 + 0.02 * log(v.v_ct_m3)) * v.v_ct_m3))::numeric AS gthrsea
FROM
	public.activite act
	INNER JOIN public.bateau v ON (act.c_bat=v.c_bat)
	INNER JOIN public.pavillon f ON (f.c_pav_b=v.c_pav_b)
	INNER JOIN public.type_bateau vt ON (v.c_typ_b=vt.c_typ_b)
	INNER JOIN public.engin g ON (vt.c_engin=g.c_engin)
WHERE
	EXTRACT(YEAR FROM act.d_act) IN (2015, 2016, 2017, 2018, 2019)
	-- For the French fleet, 1 = France & 41 = Mayotte
	AND v.c_pav_b IN (1, 41)
	-- 1 = PS, 2 = BB and 3 = LL
	AND g.c_engin IN (1, 2, 3)
;
