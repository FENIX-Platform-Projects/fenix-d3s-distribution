select year, SUM(sum_value) sum_sector, 'Total_Single_Sector_Donor_ODA' as indicator  from 
		data.test_aggregation where 
		oda = 'usd_disbursement' and
		donorcode = '302' and
		year IN (  
		'2000',
		'2001',
		'2002',
		'2003',
		'2004',
		'2005',
		'2006',
		'2007',
		'2008', 
		'2009', 
		'2010',
		'2011',
		'2012',
		'2013') 
		GROUP by year 


		union

select year, SUM(sum_value) sum_sector, 'Single_Sector_Donor_ODA' as indicator  from 
		data.test_aggregation where 
		oda = 'usd_disbursement' and
		donorcode = '302' and
		sectorcode = '151' and
		year IN (  
		'2000',
		'2001',
		'2002',
		'2003',
		'2004',
		'2005',
		'2006',
		'2007',
		'2008', 
		'2009', 
		'2010',
		'2011',
		'2012',
		'2013') 
GROUP by year 

		union

		select subsect.year, (sum_subsector/sum_sector)*100 as percentage, 'percentage' as indicator from

	(select year, SUM(sum_value) sum_subsector from 
		data.test_aggregation where 
		oda = 'usd_disbursement' and
		donorcode = '302' and
		sectorcode = '151' and

		year IN (  
		'2000',
		'2001',
		'2002',
		'2003',
		'2004',
		'2005',
		'2006',
		'2007',
		'2008', 
		'2009', 
		'2010',
		'2011',
		'2012',
		'2013') 
		GROUP by year 
	) subsect

			join
				(select year, SUM(sum_value) sum_sector  from 
					data.test_aggregation where 
					oda = 'usd_disbursement' and
					donorcode = '302' and
					year IN (  
					'2000',
					'2001',
					'2002',
					'2003',
					'2004',
					'2005',
					'2006',
					'2007',
					'2008', 
					'2009', 
					'2010',
					'2011',
					'2012',
					'2013') 
					GROUP by year)sect
						on(sect.year = subsect.year)
						

					ORDER by year DESC