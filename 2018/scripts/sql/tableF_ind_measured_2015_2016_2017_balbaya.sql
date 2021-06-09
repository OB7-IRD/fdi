SELECT
country,
year,
metier,
--ocean,
schooltype,
species,
ROUND(SUM(v_eff),0) AS no_length_measurements_landg
FROM 
	(
	SELECT
		b.c_pav_b AS flag,
		'FRA'::text AS country,
		extract(year FROM e.d_act)::integer as year,
		e.c_ocea AS ocean,
		CASE
			WHEN c_engin=1 THEN 'PS_LPF_0_0_0'
			WHEN c_engin=2 THEN 'LHP_LPF_0_0_0'
		END::text AS metier,
		CASE 
			WHEN c_tban=2 THEN 'FSC'
			WHEN c_tban=1 THEN 'FOB'
			WHEN c_tban=3 THEN 'UNK'
			ELSE 'UNKN'
		END::text AS schooltype,
		ef.c_esp,
		es.c_esp_3l::text AS species,
		ef.v_long, 
		ef.v_eff::integer AS v_eff,

		e.d_act, 
		e.c_qz_act, 
		e.c_ocea, 
		e.c_port, 
		e.v_la_act, 
		e.v_lo_act, 
		e.q_act, 
		e.cwp11_act, 
		e.cwp55_act, 
		e.c_strate_ech, 
		e.c_typ_ech, 
		e.c_qual_ech, 
		e.v_p_tot_ech, 
		e.v_p_tot_str, 
		e.v_rf_tot, 
		e.v_rf_m10, 
		e.v_rf_p10, 
		e.id_jeu_d, 
		ech_esp.v_nb_total, 
		ech_esp.v_p_esp_ech, 
		ech_esp.v_p_esp_str, 
		ef.c_bat, 
		ef.d_dbq, 
		ef.n_ech, 
		ef.id_cal, 
		ef.c_esp
	FROM 
		public.echant AS e,
		public.ech_esp, 
		public.ech_freqt AS ef,
		public.bateau AS b,
		public.espece AS es
	WHERE
		es.c_esp = ech_esp.c_esp AND
		e.c_bat = b.c_bat AND 
		e.c_bat = ech_esp.c_bat AND
		e.d_dbq = ech_esp.d_dbq AND
		e.id_cal = ech_esp.id_cal AND
		e.n_ech = ech_esp.n_ech AND
		ech_esp.c_esp = ef.c_esp AND
		ech_esp.id_cal = ef.id_cal AND
		ech_esp.n_ech = ef.n_ech AND
		ech_esp.d_dbq = ef.d_dbq AND
		ech_esp.c_bat = ef.c_bat AND
		b.c_pav_b = 1 --France
		AND extract(year FROM e.d_act) IN (2015, 2016, 2017)
		AND ef.c_esp IN (1,2,3)
		AND e.c_qual_ech = 1
		AND e.c_typ_ech = 1
) AS T1
GROUP BY year, ocean, country, metier, schooltype, species
ORDER BY ocean, year, country, metier, schooltype ,species
;
