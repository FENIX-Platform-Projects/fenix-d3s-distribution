package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.process.dto.StepId;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;

@ProcessName("unecaPercentageGDP")
public class PercentageByGDP extends org.fao.fenix.d3p.process.Process<Object> {
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;


    @Override
    public Step process(Object params, Step... sourceStep) throws Exception {
        //Retrieve source information
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new UnsupportedOperationException("filter process can be applied only on a table or an other select query");
        String rid = source.getRid().toString();
        String tableName = type==StepType.table ? (String)source.getData() : '('+(String)source.getData()+") as " + rid;
        DSDDataset dsd = source.getDsd();
        //Add label columns if needed
        Language[] languages = DatabaseStandards.getLanguageInfo();
        if (languages!=null && languages.length>0)
            dsd.extend(DatabaseStandards.getLanguageInfo());
        //Create query
        String query = createQuery(
                tableName,
                type==StepType.table ? tableName : rid,
                source.getStorage().getTableName("UNECA_GDP_USD"),
                source.getStorage().getTableName("UNECA_GDP_NC"),
                dsd
        );
        //Return query step
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        step.setData(query);
        step.setParams(type==StepType.query ? ((QueryStep)source).getParams() : null);
        step.setTypes(type==StepType.query ? ((QueryStep)source).getTypes() : null);
        return step;

    }


    private String createQuery(String tableName, String rawTableName, String gdpUsdTableName, String gdpNationalCurrencyTableName, DSDDataset dsd) throws Exception {
        //Find unit of measure column
        String umColumnId = null;
        for (DSDColumn column : dsd.getColumns())
            if ("um".equals(column.getSubject()))
                umColumnId = column.getId();
        if (umColumnId==null)
            throw new Exception("Unit of measure column not found for the dataset");
        //Create selection part of the query (with the formula for the value column)
        StringBuilder selection = new StringBuilder("SELECT ");
        for (DSDColumn column : dsd.getColumns()) {
            if ("value".equals(column.getSubject()))
                selection
                        .append("CASE ").append(rawTableName).append('.').append(umColumnId)
                        .append(" WHEN 'NC' THEN (100/NC.value)*").append(rawTableName).append('.').append(column.getId())
                        .append(" WHEN 'USD' THEN (100/USD.value)*").append(rawTableName).append('.').append(column.getId())
                        .append(" END AS ").append(column.getId()).append(',');
            else
                selection.append(rawTableName).append('.').append(column.getId()).append(',');
        }
        selection.setLength(selection.length()-1);
        //Add source table and join with gdp tables
        selection.append(" FROM ").append(tableName)
        .append(" join (select countrycode, year, value from ").append(gdpNationalCurrencyTableName).append(" where indicatorcode = '020707') as NC on (").append(rawTableName).append(".countrycode = NC.countrycode and ").append(rawTableName).append(".year = NC.year)")
        .append(" join (select countrycode, year, value from ").append(gdpUsdTableName).append(" where indicatorcode = '020707') as USD on (").append(rawTableName).append(".countrycode = USD.countrycode and ").append(rawTableName).append(".year = USD.year)");
        //Return query
        return selection.toString();
    }


}





    /*
    SELECT
sourceData.countrycode,
NC.countrycode as gdpCountry,
sourceData.year,
NC.year as gdpyear,
sourceData.indicatorcode,

sourceData.um,
sourceData.value,
NC.value as gdp,
CASE sourceData.um WHEN 'NC' THEN ((100/NC.value)*sourceData.value) WHEN 'USD' THEN ((100/USD.value)*sourceData.value) END as result

FROM (SELECT * FROM DATA."UNECA_BalanceOfPayments" WHERE indicatorcode in ('020201', '020204') ) as sourceData
join (select countrycode, year, value from DATA.UNECA_GDP_NC where indicatorcode = '020707') as NC on (sourceData.countrycode = NC.countrycode and sourceData.year = NC.year)
join (select countrycode, year, value from DATA.UNECA_GDP_USD where indicatorcode = '020707') as USD on (sourceData.countrycode = USD.countrycode and sourceData.year = USD.year)

     */


