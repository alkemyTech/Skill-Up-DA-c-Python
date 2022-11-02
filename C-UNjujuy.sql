select nombre, sexo, direccion, email, birth_date, university, inscription_date, career, "location"
FROM public.jujuy_utn
where university = 'universidad nacional de jujuy' and
to_date(inscription_date, 'YYYY/MM/DD')  between '2020-09-01' and '2021-02-01';
