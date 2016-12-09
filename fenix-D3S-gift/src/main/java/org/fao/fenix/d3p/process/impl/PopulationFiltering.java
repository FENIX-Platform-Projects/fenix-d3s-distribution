package org.fao.fenix.d3p.process.impl;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.StringUtils;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.dto.PercentileParameters;
import org.fao.fenix.d3p.process.dto.PopulationParameters;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

@ProcessName("gift_population_filter")
public class PopulationFiltering extends org.fao.fenix.d3p.process.Process<PopulationParameters> {
    @Inject DatabaseUtils databaseUtils;
    @Inject StepFactory stepFactory;


    @Override
    public Step process(PopulationParameters params, Step[] sourceStep) throws Exception {
        //Retrieve source information
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new BadRequestException("gift_population_filter process can be applied only on a table or an other select query");
        String tableName = type==StepType.table ? (String)source.getData() : '('+(String)source.getData()+") as " + source.getRid();
        DSDDataset dsd = source.getDsd();
        //Run validation function
        validate(params, dsd);
        //Add label columns if needed
        Language[] languages = DatabaseStandards.getLanguageInfo();
        if (languages!=null && languages.length>0)
            dsd.extend(languages);
        //Return query step
        Object[] existingParams = type==StepType.query ? ((QueryStep)source).getParams() : null;
        Collection<Object> queryParameters = existingParams!=null && existingParams.length>0 ? new LinkedList<>(Arrays.asList(existingParams)) : new LinkedList<>();
        //Create population variable
        setGlobalVariable("raw_data_population_size", new Object[]{getPopulationSize(source,params,tableName,queryParameters)});
        //Return query step
        String query = createQuery(params, dsd, tableName, queryParameters);
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        step.setData(query);
        step.setParams(queryParameters.toArray());
        step.setTypes(null);
        return step;
    }

    private long getPopulationSize(Step source, PopulationParameters params, String tableName, Collection<Object> existingParams) throws Exception {
        Collection<Object> queryParams = existingParams!=null ? new LinkedList<>(existingParams) : new LinkedList<>();

        String query = "select count(*) from (select subject from " + tableName + buildWhereConditions(params, queryParams) + " group by subject) as subjects";

        Connection connection = source.getStorage().getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(query);
            databaseUtils.fillStatement(statement,null,queryParams.toArray());
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next())
                return resultSet.getLong(1);
            else
                return 1;
        } finally {
            connection.close();
        }
    }

    private void validate(PopulationParameters params, DSDDataset dsd) throws Exception {
        if (params!=null) {
            if (params.age_month!=null && params.age_year!=null)
                throw new BadRequestException("gift_population_filter: specify only one between 'age_year' and 'age_month' parameters");

            StringBuilder notFoundColumns = new StringBuilder();
            if (params.item!=null && dsd.findColumn("item")==null)
                notFoundColumns.append(",age_month");
            if (params.age_month!=null && dsd.findColumn("age_month")==null)
                notFoundColumns.append(",age_month");
            if (params.age_year!=null && dsd.findColumn("age_year")==null)
                notFoundColumns.append(",age_year");
            if (params.gender!=null && dsd.findColumn("gender")==null)
                notFoundColumns.append(",gender");
            if (params.special_condition!=null && dsd.findColumn("special_condition")==null)
                notFoundColumns.append(",special_condition");
            if (notFoundColumns.length()>0)
                throw new BadRequestException("gift_population_filter: some required column isn't present into the dataset ("+notFoundColumns.substring(1)+")");

            if (params.special_condition!=null && params.special_condition.length>0 && params.gender!=null && params.gender.equals("1"))
                throw new BadRequestException("gift_population_filter: cannot specify special_condition with 'male' gender");
        }
    }


    private String buildWhereConditions(PopulationParameters params, Collection<Object> queryParams) {
        StringBuilder where = new StringBuilder();

        if (params.item!=null) {
            where.append(" AND item = ?");
            queryParams.add(params.item);
        }

        if (params.special_condition!=null && params.special_condition.length>0) {
            where.append(" AND ");
            if (params.gender==null) {
                where.append("(gender != ? OR ");
                queryParams.add("2");
            }

            where.append("special_condition IN (");
            for (String specialCondition : params.special_condition)
                if (specialCondition.equals("1")) { //Pregnant
                    where.append("?,?,");
                    queryParams.add("2");
                    queryParams.add("8");
                } else if (specialCondition.equals("2")) { //Lactating
                    where.append("?,?,");
                    queryParams.add("3");
                    queryParams.add("9");
                } else if (specialCondition.equals("3")) { //Pregnant and lactating
                    where.append("?,?,");
                    queryParams.add("4");
                    queryParams.add("11");
                } else if (specialCondition.equals("4")) { //Non pregnant and non lactating
                    where.append("?,?,?,?,?,");
                    queryParams.add("1");
                    queryParams.add("5");
                    queryParams.add("6");
                    queryParams.add("7");
                    queryParams.add("10");
                }
            where.setCharAt(where.length()-1, ')');

            if (params.gender==null)
                where.append(')');
        }
        if (params.gender!=null) {
            where.append(" AND gender = ?");
            queryParams.add(params.gender);
        }

        if (params.age_year!=null) {
            if (params.age_year.from!=null) {
                where.append(" AND age_year >= ?");
                queryParams.add(params.age_year.from);
            }
            if (params.age_year.to!=null) {
                where.append(" AND age_year <= ?");
                queryParams.add(params.age_year.to);
            }
        }
        if (params.age_month!=null) {
            if (params.age_month.from!=null) {
                where.append(" AND age_month >= ?");
                queryParams.add(params.age_month.from);
            }
            if (params.age_month.to!=null) {
                where.append(" AND age_month <= ?");
                queryParams.add(params.age_month.to);
            }
        }

        return where.length()>0 ? " WHERE"+where.substring(4) : "";
    }

    private String createQuery(PopulationParameters params, DSDDataset dsd, String tableName, Collection<Object> queryParams) throws Exception {

        StringBuilder query = new StringBuilder("SELECT ");
        for (DSDColumn column : dsd.getColumns())
            query.append(column.getId()).append(',');
        query.setCharAt(query.length()-1,' ');

        query.append(" FROM ").append(tableName);

        query.append(buildWhereConditions(params,queryParams));

        return query.toString();
    }

}
