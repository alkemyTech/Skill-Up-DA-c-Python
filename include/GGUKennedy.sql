SELECT  universidades AS university,
		carreras AS career, 
		fechas_de_inscripcion AS inscription_date, 
		NULL AS first_name,
		nombres AS last_name, 
		sexo AS gender, 
		fechas_nacimiento AS birth_date,
		NULL AS age, 
		codigos_postales AS postal_code,
		NULL AS location,
		emails AS email
FROM 
    uba_kenedy uk 
WHERE 
    uk.universidades = 'universidad-j.-f.-kennedy'
    AND TO_DATE(uk.fecha_de_inscripcion,'YY-MON-DD') BETWEEN '2020-09-01' AND '2021-02-01';