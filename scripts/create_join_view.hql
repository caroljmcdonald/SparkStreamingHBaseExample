create view pumpview as
select s.date, s.hz, s.disp, s.flo, s.sedPPM, s.psi, s.chlPPM, 
p.resourceid, p.type, p.purchasedate, p.dateinservice, p.vendor, p.longitude, p.latitude
from sensor s
join pump_info p
on (s.resid = p.resourceid);
