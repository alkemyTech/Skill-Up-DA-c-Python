SELECT universidad, careers, fecha_de_inscripcion, names, sexo, birth_dates, codigo_postal, direcciones, correos_electronicos
FROM public.palermo_tres_de_febrero
where universidad = '_universidad_de_palermo' and
to_date(fecha_de_inscripcion, 'DD/Mon/YY')  between '2020-09-01' and '2021-02-01';