select
    universities as university,
    careers as career,
    inscription_dates as inscription_date,
    null as first_name,
    names as last_name,
    birth_dates as birth_date,
    sexo as gender,
    null as age,
    null as postal_code,
    locations as location,
    emails as email 
from lat_sociales_cine
where to_date(inscription_dates, 'DD-MM-YYYY') between '2020-09-01' and '2021-02-01'
and universities  = 'UNIVERSIDAD-DEL-CINE';