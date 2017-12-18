package org.fao.fenix.d3p.process.impl;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.utils.UIDUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.dto.ExsituFilterParams;
import org.fao.fenix.d3p.process.type.ProcessName;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.util.*;

@ProcessName("wiews_exsitu_crops_genus_filter")
public class ExsituCropsGenusFiltering extends org.fao.fenix.d3p.process.Process<ExsituFilterParams> {
    private @Inject StepFactory stepFactory;
    private @Inject UIDUtils uidUtils;



    @Override
    public Step process(ExsituFilterParams params, Step[] sourceStep) throws Exception {
        //Retrieve source information
        Map<String, Step> sources = new HashMap<>();
        for (Step step : sourceStep)
            sources.put(step.getRid().getUid(), step);
        if (!sources.containsKey("crop_genus"))
            throw new BadRequestException("wiews_exsitu_crops_genus_filter process requires crop_genus dataset");
        if (!sources.containsKey("ref_sdg_species"))
            throw new BadRequestException("wiews_exsitu_crops_genus_filter process requires ref_sdg_species dataset");
        if (params.year==null)
            throw new BadRequestException("wiews_exsitu_crops_genus_filter process requires year parameter");
        if (params.crops==null || params.crops.length==0)
            throw new BadRequestException("wiews_exsitu_crops_genus_filter process requires crops parameter");
        Step ref_sdg_species_source = sources.get("ref_sdg_species");
        Step crop_genus_source = sources.get("crop_genus");
        DSDDataset dsd = ref_sdg_species_source.getDsd();
        //Create query
        Collection<Object> queryParameters = new LinkedList<>();
        String query = getQuery(params, (String)crop_genus_source.getData(), (String)ref_sdg_species_source.getData(), queryParameters);
        //Return step
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(updateDSD(dsd));
        step.setData(query);
        step.setParams(queryParameters.toArray());
        return step;
    }



    private DSDDataset updateDSD(DSDDataset dsd) {
        DSDColumn[] columns = new DSDColumn[2];
        for (DSDColumn column : dsd.getColumns())
            if (column.getId().equals("genus"))
                columns[0] = column;
            else if (column.getId().equals("species"))
                columns[1] = column;
        dsd.setColumns(Arrays.asList(columns));

        return dsd;
    }



    private String getQuery(ExsituFilterParams parameters, String crop_genus_tn, String ref_sdg_species_tn, Collection<Object> queryParameters) {
        String query =
                ("SELECT <gs>.genus, <gs>.species FROM (SELECT genus, species FROM <crop_genus> WHERE crop_en IN (<crops>) GROUP BY genus, species) <gs>\n" +
                "JOIN (SELECT genus, species FROM <ref_sdg_species> WHERE year = ?) <r> ON (<r>.genus = <gs>.genus AND <r>.species = <gs>.species)\n" +
                "GROUP BY <gs>.genus, <gs>.species")
        .replaceAll("<gs>","D3P_"+uidUtils.newId())
        .replaceAll("<r>","D3P_"+uidUtils.newId())
        .replaceAll("<crop_genus>",crop_genus_tn)
        .replaceAll("<ref_sdg_species>",ref_sdg_species_tn);

        StringBuilder cropsSegment = new StringBuilder();
        for (String code : parameters.crops) {
            cropsSegment.append(",?");
            queryParameters.add(code);
        }
        query = query.replaceAll("<crops>",cropsSegment.substring(1));

        queryParameters.add(parameters.year);

        return query.toString();
    }

}
