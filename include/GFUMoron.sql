SELECT 
	nombrre AS last_name,
	NULL AS first_name,
	sexo AS gender,
	eemail AS email,
	nacimiento AS birth,
	NULL AS age,
	universidad AS university,
	fechaiscripccion AS inscription_date,
	carrerra AS career,
	codgoposstal AS postal_code
	FROM public.moron_nacional_pampa 
	WHERE universidad='Universidad de morón' AND
	TO_DATE(fechaiscripccion,'DD/MM/YYYY') BETWEEN '2020-09-01' AND '2021-02-01';
	