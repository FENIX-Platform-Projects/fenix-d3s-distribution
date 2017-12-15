package org.fao.fenix.d3p.process.impl;

import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.dto.ExsituFilterParams;
import org.fao.fenix.d3p.process.type.ProcessName;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.util.*;

@ProcessName("wiews_exsitu_filter")
public class ExsituFiltering extends org.fao.fenix.d3p.process.Process<ExsituFilterParams> {
    private @Inject StepFactory stepFactory;
    private @Inject DatabaseUtils databaseUtils;



    @Override
    public Step process(ExsituFilterParams params, Step[] sourceStep) throws Exception {
        //Retrieve source information
        Map<String, Step> sources = new HashMap<>();
        for (Step step : sourceStep)
            sources.put(step.getRid().getUid(), step);
        if (params.year==null)
            throw new BadRequestException("wiews_exsitu_filter process requires year parameter");
        if (!sources.containsKey("wiews_"+params.year))
            throw new BadRequestException("wiews_exsitu_filter process requires wiews_<year> dataset (wiews_"+params.year+")");
        Step source = sources.get("wiews_"+params.year);
        DSDDataset dsd = source.getDsd();
        //Create query
        Collection<Object> queryParameters = new LinkedList<>();
        String query = getQuery(params, (String)source.getData(), queryParameters);
        //Return step
        QueryStep step = (QueryStep)stepFactory.getInstance(StepType.query);
        step.setDsd(dsd);
        step.setData(query);
        step.setParams(queryParameters.toArray());
        return step;
    }



    private String getQuery(ExsituFilterParams parameters, String tableName, Collection<Object> queryParameters) {
        StringBuilder query = new StringBuilder("SELECT * FROM ").append(tableName);

        String whereCondition = getWhereCondition(parameters, queryParameters);
        if (whereCondition!=null)
            query.append(" WHERE ").append(whereCondition);

        return query.toString();
    }

    private String getWhereCondition (ExsituFilterParams parameters, Collection<Object> queryParameters) {
        StringBuilder where = new StringBuilder();

        if (parameters.institutes!=null && parameters.institutes.length>0) {
            where.append(" AND w_instcode IN (");
            for (String code : parameters.institutes) {
                where.append("?,");
                queryParameters.add(code);
            }
            where.setCharAt(where.length()-1,')');
        }
        if (parameters.countries!=null && parameters.countries.length>0) {
            where.append(" AND nicode IN (");
            for (String code : parameters.countries) {
                where.append("?,");
                queryParameters.add(code);
            }
            where.setCharAt(where.length()-1,')');
        }
        if (parameters.genus_species!=null && parameters.genus_species.length>0) {
            Map<String, Collection<String>> genusSpeciesMap = new HashMap<>();
            Collection<String> genusList = new HashSet<>();
            for (String[] gs : parameters.genus_species) {
                if (gs.length==1 || gs[1]==null || gs[1].trim().equalsIgnoreCase("all")) {
                    genusSpeciesMap.remove(gs[0]);
                    genusList.add(gs[0]);
                } else if (!genusList.contains(gs[0])) {
                    Collection<String> species = genusSpeciesMap.get(gs[0]);
                    if (species==null)
                        genusSpeciesMap.put(gs[0], species = new HashSet<>());
                    species.add(gs[1]);
                }
            }
            if (genusList.size()+genusSpeciesMap.size()>0) {
                StringBuilder genusWhere = new StringBuilder();
                if (genusList.size() > 0) {
                    genusWhere.append(" OR genus IN (");
                    for (String code : genusList) {
                        genusWhere.append("?,");
                        queryParameters.add(code);
                    }
                    where.setCharAt(where.length() - 1, ')');
                }
                for (Map.Entry<String, Collection<String>> genusSpecies : genusSpeciesMap.entrySet()) {
                    for (String species : genusSpecies.getValue()) {
                        genusWhere.append(" OR genus=? AND species=?");
                        queryParameters.add(genusSpecies.getKey());
                        queryParameters.add(species);
                    }
                }
                where.append(" AND (").append(genusWhere.substring(4)).append(')');
            }
        }
        if (parameters.sampstat!=null && parameters.sampstat.length>0) {
            where.append(" AND sampstat IN (");
            for (String code : parameters.sampstat) {
                where.append("?,");
                queryParameters.add(code);
            }
            where.setCharAt(where.length()-1,')');
        }
        if (parameters.cwr!=null) {
            where.append(parameters.cwr ? " AND cwr=TRUE" : " AND no_cwr=TRUE");
        }
        if (parameters.mlsstat!=null) {
            where.append(" AND mlsstat=?");
            queryParameters.add(parameters.mlsstat);
        }
        if (parameters.taxons!=null && parameters.taxons.length>0) {
            where.append(" OR taxon IN (");
            for (String code : parameters.taxons) {
                where.append("?,");
                queryParameters.add(code);
            }
            where.setCharAt(where.length()-1,')');
        }

        return where.length()>0 ? where.substring(5) : null;
    }

}
