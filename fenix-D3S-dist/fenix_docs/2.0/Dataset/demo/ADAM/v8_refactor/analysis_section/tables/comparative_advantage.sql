-- creation of the temporary table, root for the other processes
create temporary table temp as (select
	 g.fao_sector ,
	 g.recipientcode,
	 g.year,
	 g.subsector_code,
	 sum(g.value) as value ,
	 max(g.unitcode) as unitcode
	 from
	 (
		select
		channelcode,
		recipientcode,
		year,
		subsector_code,
		sum(value) as value ,
		max(unitcode) as unitcode,
		case when channelcode = '41301' then '1' else '0' end as fao_sector
		from
		usd_commitment
		where
		subsector_code is not null
		group by
		channelcode, recipientcode, year, subsector_code
)g
group by
g.fao_sector, g.recipientcode, g.year, g.subsector_code );


create table comparative_advantage as (
-- creation of the formula through indicators

select
recipientcode,
year,
subsector_code,
case when fao_totz_value>0 then (fao_az_value/fao_totz_value)*100   else null end as delivery,
case when tot_az_value>0   then (fao_az_value/tot_az_value)*100     else null end as fao_delivery,
case when tot_agri_value>0 then (fao_totz_value/tot_agri_value)*100 else null end as total_fao_delivery,
case when fao_totz_value>0 AND (fao_totz_value/tot_agri_value) >0 then ((fao_az_value/tot_az_value)/(fao_totz_value/tot_agri_value)) else null end as advantage_ratio,
case when fao_totz_value>0 AND (fao_totz_value/tot_agri_value) >0 AND ((fao_az_value/tot_az_value)/(fao_totz_value/tot_agri_value)) >=1 then 'YES' else 'NO' end as ratio

from(
	-- join between every indicator

	select  a.recipientcode,
		a.subsector_code,
		a.year,
		fao_az_value,
		tot_az_value,
		COALESCE(fao_totz_value, 0) as fao_totz_value,
		COALESCE(tot_agri_value, 0) as tot_agri_value
		from (

-- join between fao_az_value and tot_az_val7ue

			select
			s.recipientcode as  recipientcode,
			s.year as year,
			s.subsector_code as subsector_code,
			COALESCE(t.fao_az_value, 0) as fao_az_value,
			tot_az_value
			from
			(
-- creation of fao_az_value
				select
				recipientcode,
				year,
				subsector_code,
				sum(value) as fao_az_value
				from
				temp
				where
				fao_sector = '1'
				group by
				recipientcode,
				year,
				subsector_code
			)t

			 right join
			(
-- creation of tot_az_value

				select
				recipientcode,
				year,
				subsector_code,
				sum(value) as tot_az_value
				from
				temp
				group by
				recipientcode,
				year,
				subsector_code
			)s
			on
			(
				t.recipientcode = s.recipientcode and
				t.year = s.year and
				t.subsector_code = s.subsector_code
			)
		)a
		left join
		(
-- join between fao_totz_value and agrit_value

			select
			s.recipientcode as  recipientcode,
			s.year as year,
			COALESCE(t.fao_totz_value, 0) as fao_totz_value,
			tot_agri_value
			from
			(
-- creation of fao_totz_value

				select
				recipientcode,
				year,
				sum(value) as fao_totz_value
				from
				temp
				where
				fao_sector = '1'
				group by
				recipientcode,
				year
			)t

			 right join
			(
-- creation of tot_agri_value

				select
				recipientcode,
				year,
				sum(value) as tot_agri_value
				from
				temp
				group by
				recipientcode,
				year
			)s
			on
			(
				t.recipientcode = s.recipientcode and
				t.year = s.year
			)
		)b
		on
		(
			a.recipientcode = b.recipientcode and
			a.year = b.year
	))z
	);
