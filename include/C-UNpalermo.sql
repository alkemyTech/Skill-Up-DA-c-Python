SELECT universidad as university, careers, fecha_de_inscripcion as inscription_date, null as first_name, names AS last_name, sexo as gender, birth_dates, null as age, codigo_postal as postal_code, direcciones as "location", correos_electronicos as email
FROM public.palermo_tres_de_febrero
where universidad = '_universidad_de_palermo' and
to_date(fecha_de_inscripcion, 'DD/Mon/YY')  between '2020-09-01' and '2021-02-01';