package org.fao.fenix.d3p.process.impl;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.dto.OrganizationFilterParams;
import org.fao.fenix.d3p.process.type.ProcessName;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.util.*;

@ProcessName("wiews_organization_filter")
public class OrganizationFiltering extends org.fao.fenix.d3p.process.Process<OrganizationFilterParams> {
    @Inject StepFactory stepFactory;
    @Inject UIDUtils uidUtils;


    @Override
    public Step process(OrganizationFilterParams params, Step[] sourceStep) throws Exception {
        //Retrieve source information
        Step source = sourceStep!=null && sourceStep.length>0 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || type!=StepType.table || !source.getRid().getUid().equalsIgnoreCase("wiews_organizations"))
            throw new BadRequestException("wiews_organization_filter process can be applied only on wiews_organizations dataset directly");
        DSDDataset dsd = source.getDsd();
        //Prepare query
        Collection<Object> queryParameters = new LinkedList<>();
        String query = getQuery(params, (String)source.getData(), queryParameters);
        //Return step
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(updateDSD(dsd));
        step.setData(query);
        step.setParams(queryParameters.toArray());
        return step;
    }


    private DSDDataset updateDSD(DSDDataset dsd) {
        DSDColumn rankColumn = new DSDColumn();
        rankColumn.setId("search_rank");
        rankColumn.setDataType(DataType.number);
        dsd.getColumns().add(rankColumn);

        return dsd;
    }


    private String getQuery(OrganizationFilterParams parameters, String tableName, Collection<Object> queryParameters) {

        StringBuilder query = new StringBuilder("SELECT *, 0::INTEGER as search_rank FROM ").append(tableName);

        String exclusiveWhereCondition = getWhereCondition(parameters, queryParameters, false);
        if (exclusiveWhereCondition!=null)
            query
                .append(" WHERE ").append(exclusiveWhereCondition)
                .append(" UNION SELECT *, 1::INTEGER as search_rank FROM (")
                .append("SELECT * FROM ").append(tableName).append(" WHERE ").append(getWhereCondition(parameters, queryParameters, true))
                .append(" EXCEPT ").append("SELECT * FROM ").append(tableName).append(" WHERE ").append(getWhereCondition(parameters, queryParameters, false))
                .append(") D3P_").append(uidUtils.newId());

        return query.toString();
    }

    private String getWhereCondition (OrganizationFilterParams parameters, Collection<Object> queryParameters, boolean inclusive) {
        StringBuilder where = new StringBuilder();

        if (parameters.valid!=null) {
            where.append(" AND valid_flag=?");
            queryParameters.add(parameters.valid);
        }
        if (parameters.country_iso3!=null && parameters.country_iso3.length>0) {
            where.append(" AND country_iso3 IN (");
            for (String code : parameters.country_iso3) {
                where.append("?,");
                queryParameters.add(code);
            }
            where.setCharAt(where.length()-1,')');
        }
        appendFreetextParameter("freetext_index", parameters.freetext, where, queryParameters, inclusive);
        appendFreetextParameter("i_name", parameters.name, where, queryParameters, inclusive);
        appendFreetextParameter("i_acronym", parameters.acronym, where, queryParameters, inclusive);
        appendFreetextParameter("i_instcode", parameters.instcode, where, queryParameters, inclusive);
        appendFreetextParameter("i_city", parameters.city, where, queryParameters, inclusive);

        return where.length()>0 ? where.substring(5) : null;
    }

    private void appendFreetextParameter(String columnName, String parameterValue, StringBuilder where, Collection<Object> queryParameters, boolean inclusive) {
        if (parameterValue!=null) {
            StringBuilder placeHolder = new StringBuilder();
            for (String token : parameterValue.split(" "))
                if (token.trim().length()>=3) {
                    placeHolder.append(inclusive ? " OR " : " AND ").append(columnName).append(" LIKE ?");
                    queryParameters.add('%'+token.toLowerCase().trim()+'%');
                }
            if (placeHolder.length()>0)
                if (inclusive)
                    where.append(" AND (").append(placeHolder.substring(4)).append(')');
                else
                    where.append(placeHolder);
        }
    }

}
