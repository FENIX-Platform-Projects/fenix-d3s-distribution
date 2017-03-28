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
import org.fao.fenix.d3p.process.type.ProcessName;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

@ProcessName("wiews_area_filter")
public class RegionFiltering extends org.fao.fenix.d3p.process.Process<StandardFilter> {
    @Inject DatabaseUtils databaseUtils;
    @Inject StepFactory stepFactory;

    private static final String internationalGeneBanksCode = "WITC";

    @Override
    public Step process(StandardFilter params, Step[] sourceStep) throws Exception {
        //Retrieve source information
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || (type!=StepType.table && type!=StepType.query))
            throw new BadRequestException("wiews_area_filter process can be applied only on a table or an other select query");
        String tableName = type==StepType.table ? (String)source.getData() : '('+(String)source.getData()+") as " + source.getRid();
        DSDDataset dsd = source.getDsd();
        //Create countries variable
        setGlobalVariable("required_countries", getCountriesCode(source, type, params, tableName, dsd));
        //Create international gene banks variable when needed (to exclude them)
        //if (!includeInternationalGeneBanks(params))
            setGlobalVariable("stakeholders_exclusion", getInternationalGeneBanks());
        //Return ghost step
        Step step = stepFactory.getInstance(type);
        step.setDsd(dsd);
        step.setData(source.getData());
        if (type==StepType.query)
            ((QueryStep)step).setParams(((QueryStep)source).getParams());
        return step;
    }

    private Object[] getCountriesCode (Step source, StepType type, StandardFilter params, String tableName, DSDDataset dsd) throws Exception {
        Collection<String> countries = new LinkedList<>();

        Object[] existingParams = type==StepType.query ? ((QueryStep)source).getParams() : null;
        Collection<Object> queryParams = existingParams!=null ? new LinkedList<>(Arrays.asList(existingParams)) : new LinkedList<>();
        String query = createQuery(getColumnsCodes(params, dsd),tableName,queryParams);

        Connection connection = source.getStorage().getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(query);
            databaseUtils.fillStatement(statement,null,queryParams.toArray());
            for (ResultSet resultSet = statement.executeQuery(); resultSet.next(); )
                countries.add(resultSet.getString(1));
        } finally {
            connection.close();
        }

        return addWorldCodes(countries, params).toArray();
    }

    private Collection<String> addWorldCodes(Collection<String> countries, StandardFilter params) {
        boolean add_w = false;
        boolean add_w_i = false;
        if (params!=null)
            for (Map.Entry<String, FieldFilter> paramsEntry : params.entrySet()) {
                String worldCode = null;
                if (paramsEntry.getKey().equals("m49") || paramsEntry.getKey().equals("mdg") || paramsEntry.getKey().equals("sdg"))
                    worldCode = "1";
                else if (paramsEntry.getKey().equals("fao"))
                    worldCode = "5000";
                else if (paramsEntry.getKey().equals("itpgrfa") || paramsEntry.getKey().equals("cgrfa"))
                    worldCode = "0";

                for (CodesFilter codesFilter : paramsEntry.getValue().codes) {
                    add_w_i |= codesFilter.codes.contains(internationalGeneBanksCode);
                    add_w |= codesFilter.codes.contains(worldCode);
                }
            }

        if (add_w)
            countries.add("1");
        if (add_w_i)
            countries.add(internationalGeneBanksCode);

        return countries;
    }


    private String createQuery(Map<String, Collection<String>> columnsCodes, String tableName, Collection<Object> params) {
        StringBuilder query = new StringBuilder("select distinct (m49_country) as m49_country from ").append(tableName);
        if (columnsCodes.size()>0) {
            query.append(" where");
            for (Map.Entry<String, Collection<String>> columnCodesEntry : columnsCodes.entrySet()) {
                query.append(' ').append(columnCodesEntry.getKey()).append(" in (");
                for (int i=0, l=columnCodesEntry.getValue().size(); i<l; i++)
                    query.append("?,");
                query.setCharAt(query.length()-1,')');
                query.append(" or");
                params.addAll(columnCodesEntry.getValue());
            }
        }
        return columnsCodes.size()>0 ? query.substring(0, query.length()-3) : query.toString();
    }


    private Map<String, Collection<String>> getColumnsCodes(StandardFilter params, DSDDataset dsd) throws Exception {
        Map<String, Collection<String>> columnsCodes = new HashMap<>();
        if (params!=null) {
            for (Map.Entry<String, FieldFilter> paramsEntry : params.entrySet()) {
                Collection<String> columnsId = new LinkedList<>();
                for (DSDColumn column : dsd.getColumns())
                    if (column.getId().startsWith(paramsEntry.getKey()))
                        columnsId.add(column.getId());
                if (columnsId.size() == 0)
                    throw new BadRequestException("No correspondence found for column " + paramsEntry.getKey() + " into region-country mapping dataset");

                if (paramsEntry.getValue().retrieveFilterType() != FieldFilterType.code)
                    throw new BadRequestException("Wrong filter type for column " + paramsEntry.getKey() + ". Only codes can be used");

                Collection<String> codes = new LinkedList<>();
                for (CodesFilter codesFilter : paramsEntry.getValue().codes)
                    if (codesFilter.codes != null)
                        codes.addAll(codesFilter.codes);
                if (codes.size() > 0)
                    for (String columnId : columnsId)
                        columnsCodes.put(columnId, codes);
            }
        }
        return columnsCodes;
    }


    private boolean includeInternationalGeneBanks(StandardFilter params) {
        if (params!=null)
            for (Map.Entry<String, FieldFilter> paramsEntry : params.entrySet())
                for (CodesFilter codesFilter : paramsEntry.getValue().codes)
                    if (codesFilter.codes.contains(internationalGeneBanksCode))
                        return true;
        return false;
    }


    private Object[] getInternationalGeneBanks() {
        Collection internationalGeneBanks = new LinkedList();
        for (InternationalGeneBanks bank : InternationalGeneBanks.values())
            internationalGeneBanks.add(bank.name());
        return internationalGeneBanks.toArray();
    }
}
