SELECT top_10_recipients_sum.unitcode,top_10_recipients_sum.value

AS top_10_recipients_sum_value,top_10_recipients_sum.unitcode_EN

AS top_10_recipients_sum_unitcode_EN,top_10_recipients_sum.indicator
AS top_10_recipients_sum_indicator,top_all_recipients_sum.value
AS top_all_recipients_sum_value,top_all_recipients_sum.unitcode_EN
AS top_all_recipients_sum_unitcode_EN,top_all_recipients_sum.indicator
AS top_all_recipients_sum_indicator
FROM (SELECT value,unitcode,unitcode_EN, CASE WHEN 1=1 THEN 'Top Recipient Countries'
END AS indicator FROM (SELECT SUM(value) AS value,unitcode,FIRST(unitcode_EN) AS unitcode_EN
FROM (SELECT unitcode,value,unitcode_EN
FROM (SELECT * FROM (SELECT * FROM (SELECT value,recipientcode,unitcode,recipientcode_EN,unitcode_EN FROM (SELECT SUM(value) AS value,recipientcode,first(unitcode) AS unitcode,FIRST(recipientcode_EN) AS recipientcode_EN,FIRST(unitcode_EN) AS unitcode_EN FROM (SELECT recipientcode,value,unitcode,recipientcode_EN,unitcode_EN FROM DATA."adam_usd_aggregation_table" WHERE oda IN (?) AND parentsector_code IN (?) AND (year >= ? AND year <= ?)) as filter_top_10_recipients_sum GROUP BY recipientcode) as D3P_R_3___78326536091929352297766556877054171360 WHERE recipientcode NOT IN (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)) as D3P_R_4___546828004092353028406720197817542421 ORDER BY value DESC) as D3P_R_5___63617893882979758536906825434432324342 LIMIT 10 OFFSET 0) as D3P_R_6___21564167538426709135298094737314882673) as D3P_R_7___64655108340403222435738689315703133906 GROUP BY unitcode) as D3P_R_8___72412665542236754559110204897737764544 ) as top_10_recipients_sum JOIN (SELECT value,unitcode,unitcode_EN, CASE WHEN 1=1 THEN 'sum of all recipients' END AS indicator FROM (SELECT SUM(value) AS value,unitcode,FIRST(unitcode_EN) AS unitcode_EN FROM (SELECT value,unitcode,unitcode_EN FROM DATA."adam_usd_aggregation_table" WHERE oda IN (?) AND parentsector_code IN (?) AND (year >= ? AND year <= ?)) as filter_all_recipients_sum GROUP BY unitcode) as D3P_R_11___72290276717117614106694501750332692689 ) as top_all_recipients_sum ON (top_10_recipients_sum.unitcode = top_all_recipients_sum.unitcode )
