package org.fao.fenix.d3p.process.impl;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.dto.PercentileParameters;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

@ProcessName("gift_std_percentile")
public class StandardPercentile extends org.fao.fenix.d3p.process.Process<PercentileParameters> {
    @Inject DatabaseUtils databaseUtils;
    @Inject StepFactory stepFactory;


    @Override
    public Step process(PercentileParameters params, Step[] sourceStep) throws Exception {
        //Retrieve source information
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table))
            throw new BadRequestException("Average percentile process can be applied only on a table");
        String tableName = (String)source.getData();
        DSDDataset dsd = source.getDsd();
        Language[] languages = DatabaseStandards.getLanguageInfo();
        //Filter columns
        filter(source.getDsd(),languages);
        //Return query step
        Collection<Object> queryParameters = new LinkedList<>();
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        step.setData(createQuery(source, params, tableName, queryParameters, languages));
        step.setParams(queryParameters.toArray());
        step.setTypes(null);
        return step;
    }



    private String buildWhereConditions(PercentileParameters params, Collection<Object> queryParams) {
        StringBuilder where = new StringBuilder();
        if (params.item!=null && params.item.trim().length()>0) {
            where.append(" and item=?");
            queryParams.add(params.item);
        }
        if (params.group!=null && params.group.trim().length()>0) {
            where.append(" and group_code=?");
            queryParams.add(params.group);
        }
        if (params.subgroup!=null && params.subgroup.trim().length()>0) {
            where.append(" and subgroup_code=?");
            queryParams.add(params.subgroup);
        }
        if (params.food!=null && params.food.trim().length()>0) {
            where.append(" and foodex2_code=?");
            queryParams.add(params.food);
        }
        return where.length()>0 ? " where"+where.substring(4) : "";
    }

    private String createQuery(Step source, PercentileParameters params, String tableName, Collection<Object> queryParams,Language[] languages) throws Exception {

        String populationWhereCondition = buildWhereConditions(params, queryParams);

        int population = 0, foodPopulation=0;
        Connection connection = source.getStorage().getConnection();
        try {
            ResultSet resultSet = databaseUtils.fillStatement(connection.prepareStatement( "select count(*) from (select subject from " + tableName + " group by subject) population union all select count(*) from (select subject from " + tableName + populationWhereCondition + " group by subject) population" ),null,queryParams.toArray()).executeQuery();
            resultSet.next();
            population = resultSet.getInt(1);
            resultSet.next();
            foodPopulation = resultSet.getInt(1);
        } finally {
            connection.close();
        }


        long percentileFrom =  Math.round(Math.max(1,Math.ceil((foodPopulation * params.percentileSize) / 100.0)))-1;
        long percentileTo =  Math.round(Math.max(1,Math.ceil((foodPopulation * (100-params.percentileSize)) / 100.0)))-1;
        long percentileMedian =  Math.round(Math.max(1,Math.ceil(foodPopulation * 0.5)))-1;
        String query = population>0 && foodPopulation>=3 ?
                "with raw_data as (\n" +
                "select subject, sum(value) as value, max(um) as um"+labelUnitColumns(languages)+" from "+tableName+
                populationWhereCondition+"group by subject\n" +
                "order by sum(value),subject)\n" +
                "select 'consumers'::text as indicator, "+((((float)foodPopulation)*100)/population)+"::float as value, 'perc'::text as um"+labelPercUnitColumns(languages)+"\n" +
                "union all\n" +
                "select 'perc_low'::text as indicator, avg(value) as value, max (um) as um"+labelUnitColumns(languages)+" from \n" +
                "( select * from raw_data offset "+percentileFrom+" limit 1 ) perc_low\n" +
                "union all\n" +
                "select 'perc_middle'::text as indicator, avg(value) as value, max (um) as um"+labelUnitColumns(languages)+" from \n" +
                "( select * from raw_data offset "+percentileMedian+" limit 1 ) perc_middle\n" +
                "union all\n" +
                "select 'perc_high'::text as indicator, avg(value) as value, max (um) as um"+labelUnitColumns(languages)+" from \n" +
                "( select * from raw_data offset "+percentileTo+" limit 1 ) perc_high"
                //Produce empty dataset with same query parameters
                : "select null::text as indicator, null::float as value, null::text as um"+labelPercUnitColumns(languages) + " from "+tableName+populationWhereCondition+" limit 0";

        return query;
    }

    private void filter(DSDDataset dsd, Language[] languages) throws Exception {
        Collection<DSDColumn> dsdColumns = new LinkedList<>();

        DSDColumn column = new DSDColumn();
        column.setTitle(toLabel("Indicator"));
        column.setId("indicator");
        column.setSubject("item");
        column.setDataType(DataType.text);
        column.setKey(true);
        dsdColumns.add(column);

        column = dsd.findColumnBySubject("value");
        if (column==null)
            throw new BadRequestException("Average percentile process needs a dataset with a decleared value column");
        dsdColumns.add(column);

        column = dsd.findColumnBySubject("um");
        if (column==null)
            throw new BadRequestException("Average percentile process needs a dataset with a decleared unit of measure column");
        dsdColumns.add(column);

        dsd.setColumns(dsdColumns);
        if (languages!=null && languages.length>0)
            dsd.extend(languages);
    }

    private String labelUnitColumns(Language[] languages) {
        StringBuilder umColumns = new StringBuilder();
        if (languages!=null)
            for (Language language : languages)
                umColumns.append(", max(um_").append(language.getCode()).append(") as um_").append(language.getCode());
        return umColumns.toString();
    }

    private String labelPercUnitColumns(Language[] languages) {
        StringBuilder umColumns = new StringBuilder();
        if (languages!=null)
            for (Language language : languages)
                umColumns.append(", '%'::text as um_").append(language.getCode());
        return umColumns.toString();
    }


    private Map<String,String> toLabel(String label) {
        Map<String,String> labelMap = new HashMap<>();
        labelMap.put("EN",label);
        return labelMap;
    }

}
