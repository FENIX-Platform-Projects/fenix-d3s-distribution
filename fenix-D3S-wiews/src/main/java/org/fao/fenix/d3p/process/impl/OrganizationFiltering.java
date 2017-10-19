package org.fao.fenix.d3p.process.impl;

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


    @Override
    public Step process(OrganizationFilterParams params, Step[] sourceStep) throws Exception {
        //Retrieve source information
        Step source = sourceStep!=null && sourceStep.length>0 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || type!=StepType.table || !source.getRid().getUid().equalsIgnoreCase("wiews_organizations"))
            throw new BadRequestException("wiews_organization_filter process can be applied only on wiews_organizations dataset directly");
        //Prepare query
        Collection<Object> queryParameters = new LinkedList<>();
        String query = getQuery(params, (String)source.getData(), queryParameters);
        //Return step
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(source.getDsd());
        step.setData(query);
        step.setParams(queryParameters.toArray());
        return step;
    }


    private String getQuery(OrganizationFilterParams parameters, String tableName, Collection<Object> queryParameters) {
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
        appendFreetextParameter("freetext_index", parameters.freetext, where, queryParameters);
        appendFreetextParameter("i_name", parameters.name, where, queryParameters);
        appendFreetextParameter("i_acronym", parameters.acronym, where, queryParameters);
        appendFreetextParameter("i_instcode", parameters.instcode, where, queryParameters);
        appendFreetextParameter("i_city", parameters.city, where, queryParameters);


        return "SELECT * FROM "+tableName+(where.length()>0 ? " WHERE "+where.substring(5) : "");
    }

    private void appendFreetextParameter(String columnName, String parameterValue, StringBuilder where, Collection<Object> queryParameters) {
        if (parameterValue!=null) {
            StringBuilder placeHolder = new StringBuilder();
            for (String token : parameterValue.split(" "))
                if (token.trim().length()>=3) {
                    placeHolder.append(" OR ").append(columnName).append(" LIKE ?");
                    queryParameters.add('%'+token.toLowerCase().trim()+'%');
                }
            if (placeHolder.length()>0)
                where.append(" AND (").append(placeHolder.substring(4)).append(')');
        }
    }

}
