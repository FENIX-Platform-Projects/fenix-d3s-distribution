package org.fao.ess.cstat.d3s;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.fao.fenix.commons.find.dto.condition.ConditionFilter;
import org.fao.fenix.commons.utils.annotations.find.Engine;
import org.fao.fenix.d3s.msd.find.engine.SearchEngine;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedList;


@Engine("cstat")
public class CstatSearchEngine implements SearchEngine {

    private final String ID_FIELD = "index|id";
    @Inject private DatabaseStandards databaseStandards;


    @Override
    public Collection<String> getUids(ConditionFilter... filter) throws Exception {
        Collection<Object> params = new LinkedList<>();
        return getUids(params, filter);
    }

/*
    private Collection<String> getUids(Collection<Object> params, ConditionFilter... filter) throws Exception {
        return getIdsFromDocument(getResources(createQuery(params, filter), params));
    }
*/
    private Collection<String> getUids(Collection<Object> params, ConditionFilter... filter) throws Exception {
        String query = createQuery(params, filter);
        return getIdsFromDocument(getResources(query, params));
    }



    public String createQuery(Collection<Object> params, ConditionFilter... filter) throws Exception {
        StringBuilder queryFilter = new StringBuilder();
        for (ConditionFilter filterCondition : filter) {
            switch (filterCondition.fieldName) {
                case "dsd.contextSystem":
                    params.addAll(filterCondition.values);
                    queryFilter.append(" AND (");
                    for (int i = 0, l = filterCondition.values.size(); i < l; i++)
                        queryFilter.append(filterCondition.indexedFieldName).append(" = ? OR ");
                    queryFilter.setLength(queryFilter.length() - 4);
                    queryFilter.append(')');
                    break;

                case "freetext":
                    params.addAll(filterCondition.values);
                    queryFilter.append(" AND ").append(filterCondition.indexedFieldName).append(" LUCENE ?");

                default:
                    break;
            }
        }
        return "SELECT " + ID_FIELD + " FROM DataIndex" + (queryFilter.length() > 0 ? " WHERE " + queryFilter.substring(4) : "");
    }


    //Utils
    private Collection<String> getIdsFromDocument(Collection<ODocument> collection) {
        Collection<String> ids = new LinkedList<>();
        for (ODocument document : collection)
            ids.add(document.field(ID_FIELD).toString());
        return ids;
    }


    private Collection<ODocument> getResources(String query, Collection<Object> params) {
        return (Collection<ODocument>)databaseStandards.getConnection().getUnderlying().query(new OSQLSynchQuery<ODocument>(query), params.toArray());
    }

}
