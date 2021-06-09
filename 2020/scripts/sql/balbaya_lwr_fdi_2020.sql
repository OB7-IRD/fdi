SELECT
	s.code3l::text AS specie_name
	,o.label1::text AS ocean_name
	,split_part(split_part(l.coefficients, ':', 1), '=', 2)::numeric AS a
	,split_part(split_part(l.coefficients, ':', 2), '=', 2)::numeric AS b
FROM
	public.lengthweightconversion l 
	JOIN public.species s ON (l.species = s.topiaid)
	JOIN public.ocean o ON (l.ocean = o.topiaid)
WHERE
	s.code3l IN ('BET', 'SKJ', 'YFT', 'ALB', 'FRI', 'LTA')
;
