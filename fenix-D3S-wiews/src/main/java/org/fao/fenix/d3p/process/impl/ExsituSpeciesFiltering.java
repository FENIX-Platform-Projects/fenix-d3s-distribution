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

@ProcessName("wiews_exsitu_species_filter")
public class ExsituSpeciesFiltering extends org.fao.fenix.d3p.process.Process<ExsituFilterParams> {
    private @Inject StepFactory stepFactory;
    private @Inject UIDUtils uidUtils;



    @Override
    public Step process(ExsituFilterParams params, Step[] sourceStep) throws Exception {
        //Retrieve source information
        Map<String, Step> sources = new HashMap<>();
        for (Step step : sourceStep)
            sources.put(step.getRid().getUid(), step);
        if (!sources.containsKey("ref_sdg_species"))
            throw new BadRequestException("wiews_exsitu_species_filter process requires ref_sdg_species dataset");
        if (params.year==null)
            throw new BadRequestException("wiews_exsitu_species_filter process requires year parameter");
        if (params.genus==null)
            throw new BadRequestException("wiews_exsitu_species_filter process requires genus parameter");
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
            if (column.getId().equals("species"))
                columns[0] = column;
        dsd.setColumns(Arrays.asList(columns));

        return dsd;
    }


    private String getQuery(ExsituFilterParams parameters, String tableName, Collection<Object> queryParameters) {
        StringBuilder query = new StringBuilder("SELECT species FROM ")
                .append(tableName)
                .append(" WHERE year = ? AND genus = ? GROUP BY species");

        queryParameters.add(parameters.year);
        queryParameters.add(parameters.genus);

        return query.toString();
    }

}
