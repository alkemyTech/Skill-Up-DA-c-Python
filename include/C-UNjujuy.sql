select university, career, inscription_date, null as first_name, nombre as last_name, sexo as gender, birth_date, null as age, null as postal_code, direccion as "location", email
FROM public.jujuy_utn
where university = 'universidad nacional de jujuy' and
to_date(inscription_date, 'YYYY/MM/DD')  between '2020-09-01' and '2021-02-01';