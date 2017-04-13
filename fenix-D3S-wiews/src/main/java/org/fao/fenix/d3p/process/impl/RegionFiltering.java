package org.fao.fenix.d3p.process.impl;

import org.fao.fenix.commons.find.dto.filter.CodesFilter;
import org.fao.fenix.commons.find.dto.filter.FieldFilter;
import org.fao.fenix.commons.find.dto.filter.StandardFilter;
import org.fao.fenix.commons.find.dto.type.FieldFilterType;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.dto.InternationalGeneBanks;
import org.fao.fenix.d3p.process.dto.WiewsRegionFilterParams;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.dto.dataset.Column;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

@ProcessName("wiews_area_filter")
public class RegionFiltering extends org.fao.fenix.d3p.process.Process<WiewsRegionFilterParams> {
    @Inject DatabaseUtils databaseUtils;
    @Inject StepFactory stepFactory;

    private static final String internationalGeneBanksCode = "WITC";

    @Override
    public Step process(WiewsRegionFilterParams params, Step[] sourceStep) throws Exception {
        //Retrieve source information
        Step sourceMapping = sourceStep!=null && sourceStep.length>0 ? sourceStep[0] : null;
        StepType typeSourceMapping = sourceMapping!=null ? sourceMapping.getType() : null;
        Step sourceCountries = sourceStep!=null && sourceStep.length>1 ? sourceStep[1] : null;
        StepType typeSourceCountries = sourceCountries!=null ? sourceCountries.getType() : null;
        if (typeSourceMapping==null || typeSourceCountries==null || (typeSourceMapping!=StepType.table && typeSourceMapping!=StepType.query) || (typeSourceCountries!=StepType.table && typeSourceCountries!=StepType.query))
            throw new BadRequestException("wiews_area_filter process requires two table or query sources to be applied: w_regions_mapping and w_regions_countries");
        String tableNameMapping = typeSourceMapping==StepType.table ? (String)sourceMapping.getData() : '('+(String)sourceMapping.getData()+") as " + sourceMapping.getRid();
        String tableNameCountries = typeSourceCountries==StepType.table ? (String)sourceCountries.getData() : '('+(String)sourceCountries.getData()+") as " + sourceCountries.getRid();
        DSDDataset dsdMapping = sourceMapping.getDsd();
        DSDDataset dsdCountries = sourceCountries.getDsd();
        //Create countries variable
        Collection<String> list = new LinkedList<>();
        Map<String, Collection<String>> columnsCodes = getColumnsCodes(params.filter, dsdMapping);
        Collection<String> regions = getRegionCodes(tableNameMapping, columnsCodes, sourceMapping);
        if (params.total)
            list.addAll(regions);
        if (params.list)
            list.addAll(getCountriesCode(tableNameCountries, columnsCodes, regions, sourceCountries));
        setGlobalVariable("required_countries", list.toArray());
        //Create international gene banks variable when needed (to exclude them)
        setGlobalVariable("stakeholders_exclusion", getInternationalGeneBanks());
        //Return ghost step
        Step step = stepFactory.getInstance(typeSourceMapping);
        step.setDsd(dsdMapping);
        step.setData(sourceMapping.getData());
        if (typeSourceMapping==StepType.query)
            ((QueryStep)step).setParams(((QueryStep)sourceMapping).getParams());
        return step;
    }




    private Collection<String> getCountriesCode (String tableName, Map<String, Collection<String>> columnsCodes, Collection<String> regions, Step source) throws Exception {
        //Create query
        Collection<Object> queryParams = new LinkedList<>();
        StringBuilder queryBuffer = new StringBuilder("select distinct (country) from ").append(tableName);
        if (columnsCodes.size()>0) {
            queryBuffer.append(" where");
            if (regions.size()>0) {
                queryBuffer.append(' ').append("w").append(" in (");
                for (int i=0, l=regions.size(); i<l; i++)
                    queryBuffer.append("?,");
                queryBuffer.setCharAt(queryBuffer.length()-1,')');
                queryBuffer.append(" or");
                queryParams.addAll(regions);
            }
            for (Map.Entry<String, Collection<String>> columnCodesEntry : columnsCodes.entrySet())
                if (columnCodesEntry.getKey().equals("iso3")) {
                    queryBuffer.append(' ').append("country").append(" in (");
                    for (int i=0, l=columnCodesEntry.getValue().size(); i<l; i++)
                        queryBuffer.append("?,");
                    queryBuffer.setCharAt(queryBuffer.length()-1,')');
                    queryBuffer.append(" or");
                    queryParams.addAll(columnCodesEntry.getValue());
                }
        }
        String query = columnsCodes.size()>0 ? queryBuffer.substring(0, queryBuffer.length()-3) : queryBuffer.toString();
        //Parse and return result
        Connection connection = source.getStorage().getConnection();
        try {
            Collection<String> countries = new LinkedList<>();
            for (ResultSet resultSet = select(query, queryParams, connection, source); resultSet.next(); )
                countries.add(resultSet.getString(1));
            return countries;
        } finally {
            connection.close();
        }
    }

    private Collection<String> getRegionCodes(String tableName, Map<String, Collection<String>> columnsCodes, Step source) throws Exception {
        Collection<String> regions = new LinkedList<>();

        if (columnsCodes.size()>0) {
            //Create query
            Collection<Object> queryParams = new LinkedList<>();
            StringBuilder where = new StringBuilder();
            for (Map.Entry<String, Collection<String>> columnCodesEntry : columnsCodes.entrySet())
                if (!columnCodesEntry.getKey().equals("iso3")) {
                    where.append(' ').append(columnCodesEntry.getKey()).append(" in (");
                    for (int i=0, l=columnCodesEntry.getValue().size(); i<l; i++)
                        where.append("?,");
                    where.setCharAt(where.length()-1,')');
                    where.append(" or");
                    queryParams.addAll(columnCodesEntry.getValue());
                }
            String query = "select distinct (w) from "+tableName+(where.length()>0 ? " where"+where.substring(0, where.length()-3) : "");
            //Parse result
            Connection connection = source.getStorage().getConnection();
            try {
                for (ResultSet resultSet = select(query, queryParams, connection, source); resultSet.next(); )
                    regions.add(resultSet.getString(1));
            } finally {
                connection.close();
            }
        }

        return regions;
    }

    private Map<String, Collection<String>> getColumnsCodes(StandardFilter params, DSDDataset dsd) throws Exception {
        Map<String, Collection<String>> columnsCodes = new HashMap<>();
        if (params!=null) {
            for (Map.Entry<String, FieldFilter> paramsEntry : params.entrySet()) {

                if (paramsEntry.getValue().retrieveFilterType() != FieldFilterType.code)
                    throw new BadRequestException("Wrong filter type for column " + paramsEntry.getKey() + ". Only codes can be used");

                Collection<String> codes = new LinkedList<>();
                for (CodesFilter codesFilter : paramsEntry.getValue().codes)
                    if (codesFilter.codes != null)
                        codes.addAll(codesFilter.codes);
                if (codes.size() > 0)
                    columnsCodes.put(paramsEntry.getKey(), codes);
            }
        }
        return columnsCodes;
    }

    private Object[] getInternationalGeneBanks() {
        Collection internationalGeneBanks = new LinkedList();
        for (InternationalGeneBanks bank : InternationalGeneBanks.values())
            internationalGeneBanks.add(bank.name());
        return internationalGeneBanks.toArray();
    }



    //Utils
    private ResultSet select (String query, Collection params, Connection connection, Step source) throws Exception {
        Object[] existingParams = source.getType()==StepType.query ? ((QueryStep)source).getParams() : null;
        Collection<Object> queryParams = existingParams!=null ? new LinkedList<>(Arrays.asList(existingParams)) : new LinkedList<>();
        queryParams.addAll(params);

        PreparedStatement statement = connection.prepareStatement(query);
        databaseUtils.fillStatement(statement,null,queryParams.toArray());
        return statement.executeQuery();
    }


}
