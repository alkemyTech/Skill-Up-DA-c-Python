SELECT  universities AS university, 
        careers AS career, 
		inscription_dates AS inscription_date, 
		NULL AS first_name,
		names AS last_name, 
		sexo AS gender, 
		birth_dates AS birth_date, 
		NULL AS age,
		NULL AS postal_code,
		locations AS location,
		emails AS email
FROM 
	lat_sociales_cine lsc 
WHERE 
	lsc.universities = '-FACULTAD-LATINOAMERICANA-DE-CIENCIAS-SOCIALES'
	AND TO_DATE(lsc.inscription_dates,'DD-MM-YYYY') BETWEEN  '2020-09-01' AND '2021-02-01';
