SELECT 
	names AS last_name,
	NULL AS first_name,
	sexo AS gender,
	email AS email,
	fechas_nacimiento AS birth,
	NULL AS age,
	univiersities AS university,
	inscription_dates AS inscription_date,
	carrera AS career,
	localidad AS location 

	FROM public.rio_cuarto_interamericana
	WHERE univiersities='Universidad-nacional-de-río-cuarto' AND
	TO_DATE(inscription_dates,'YY/MON/DD') BETWEEN '2020-09-01' AND '2021-02-01';
	