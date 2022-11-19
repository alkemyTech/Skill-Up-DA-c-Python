-- Grupo de Universidades B
-- Necesito para UNComahue y UdelSalvador, 
-- personas anotadas entre las fechas 01/9/2020 al 01/02/2021
--
-- university
-- career
-- inscription_date
-- * first_name
-- * last_name
-- gender
-- * age
-- postal_code
-- * location
-- email
--
-- las columnas que no tengo (*) las genero con valor NULL
-- TO_DATE <https://www.postgresqltutorial.com/postgresql-date-functions/postgresql-to_date/>
-- SPLIT para pasar de name a first_name, last_name
-- Para UNComahue

SELECT
	universidad AS university,
	carrera AS career,
	fecha_de_inscripcion AS inscription_date,
	name AS last_name,
	NULL AS first_name,
	sexo AS gender,
	fecha_nacimiento,
	NULL AS age,
	codigo_postal AS postal_code,
	NULL AS location,
	correo_electronico AS email 
FROM public.flores_comahue
WHERE universidad = 'UNIV. NACIONAL DEL COMAHUE'
	AND TO_DATE(fecha_de_inscripcion,'YYYY-MM-DD') BETWEEN '2020/09/01' AND '2021/02/01'
