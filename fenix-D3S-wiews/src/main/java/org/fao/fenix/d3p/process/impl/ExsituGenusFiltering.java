package org.fao.fenix.d3p.process.impl;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.d3p.dto.QueryStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.dto.ExsituFilterParams;
import org.fao.fenix.d3p.process.type.ProcessName;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.util.*;

@ProcessName("wiews_exsitu_genus_filter")
public class ExsituGenusFiltering extends org.fao.fenix.d3p.process.Process<ExsituFilterParams> {
    private @Inject StepFactory stepFactory;
    private @Inject UIDUtils uidUtils;



    @Override
    public Step process(ExsituFilterParams params, Step[] sourceStep) throws Exception {
        //Retrieve source information
        Map<String, Step> sources = new HashMap<>();
        for (Step step : sourceStep)
            sources.put(step.getRid().getUid(), step);
        if (!sources.containsKey("ref_sdg_species"))
            throw new BadRequestException("wiews_exsitu_genus_filter process requires ref_sdg_species dataset");
        if (params.year==null)
            throw new BadRequestException("wiews_exsitu_genus_filter process requires year parameter");
        Step source = sources.get("ref_sdg_species");
        DSDDataset dsd = source.getDsd();
        //Create query
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
        DSDColumn[] columns = new DSDColumn[1];
        for (DSDColumn column : dsd.getColumns())
            if (column.getId().equals("genus"))
                columns[0] = column;
        dsd.setColumns(Arrays.asList(columns));

        return dsd;
    }


    private String getQuery(ExsituFilterParams parameters, String tableName, Collection<Object> queryParameters) {
        StringBuilder query = new StringBuilder("SELECT genus FROM ").append(tableName);

        String whereCondition = getWhereCondition(parameters, queryParameters);
        if (whereCondition!=null)
            query.append(" WHERE ").append(whereCondition);

        query.append(" GROUP BY genus");

        return query.toString();
    }

    private String getWhereCondition (ExsituFilterParams parameters, Collection<Object> queryParameters) {
        StringBuilder where = new StringBuilder();

        where.append("year=?");
        queryParameters.add(parameters.year);

        appendFreetextParameter("genus", parameters.genus, where, queryParameters, true);

        return where.length()>0 ? where.toString() : null;
    }

    private void appendFreetextParameter(String columnName, String parameterValue, StringBuilder where, Collection<Object> queryParameters, boolean inclusive) {
        if (parameterValue!=null) {
            StringBuilder placeHolder = new StringBuilder();
            for (String token : parameterValue.split(" "))
                if (token.trim().length()>=3) {
                    placeHolder.append(inclusive ? " OR " : " AND ").append("lower(").append(columnName).append(") LIKE ?");
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
