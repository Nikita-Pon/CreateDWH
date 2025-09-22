select 
	* 
from public.inventory i
where i.last_update >= '2022-08-06'::date;

INSERT INTO public.inventory
(film_id, store_id)
VALUES(1, 1);


alter table public.inventory add column deleted timestamp null;


select count(*) from inventory;

select 
	i.inventory_id 
from
	inventory i
	left join rental r using (inventory_id)
where r.inventory_id is null
	
update public.inventory 
set deleted = now()
where inventory_id = 4582;

update public.inventory 
set deleted = null
where inventory_id = 4582;

select 
	*
from
	public.inventory 
order by inventory_id 
	
select * from public.film order by film_id;

update public.inventory 
set film_id = 2
where inventory_id = 1


INSERT INTO public.inventory
(
	film_id, 
	store_id
)
VALUES(
	1, 
	1
);

select 
	*
from
	public.inventory i 
order by i.last_update desc;
