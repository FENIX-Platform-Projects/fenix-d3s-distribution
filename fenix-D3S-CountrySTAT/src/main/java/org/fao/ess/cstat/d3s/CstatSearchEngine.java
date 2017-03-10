package org.fao.ess.cstat.d3s;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.sun.xml.internal.bind.v2.TODO;
import org.fao.fenix.commons.find.dto.condition.ConditionFilter;
import org.fao.fenix.commons.find.dto.condition.ConditionTime;
import org.fao.fenix.commons.utils.Context;
import org.fao.fenix.commons.utils.find.Engine;
import org.fao.fenix.d3s.msd.find.engine.SearchEngine;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;
import org.fao.fenix.d3s.wds.OrientClient;

import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

@Context({
        "cstat_afg",
        "cstat_ago",
        "cstat_ben",
        "cstat_bfa",
        "cstat_cmr",
        "cstat_civ",
        "cstat_cog",
        "cstat_eth",
        "cstat_gha",
        "cstat_gab",
        "cstat_gnb",
        "cstat_hti",
        "cstat_ken",
        "cstat_mdg",
        "cstat_mwi",
        "cstat_mli",
        "cstat_moz",
        "cstat_ner",
        "cstat_nga",
        "cstat_rwa",
        "cstat_sen",
        "cstat_tza",
        "cstat_tgo",
        "cstat_uga",
        "cstat_zmb",
        "cstat_training"
})
@Engine("cstat")
public class CstatSearchEngine implements SearchEngine {

    private final String ID_FIELD = "index|id";
    @Inject private DatabaseStandards databaseStandards;


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
        return "SELECT " + ID_FIELD + " FROM DataIndex" + (queryFilter.length() > 0 ? " WHERE " + queryFilter.substring(4) : "") + " group by " + ID_FIELD + "";
    }


    @Override
    public Collection<String> getUids(ConditionFilter... filter) throws Exception {
        Collection<Object> params = new LinkedList<>();
        return getUids(params, filter);
    }


    private Collection<String> getUids(Collection<Object> params, ConditionFilter... filter) throws Exception {

        Collection<String> ids = getIdsFromDocument(getResources(createQuery(params, filter), params));
        return null;
    }

    //Utils
    private Collection<String> getIdsFromDocument(Collection<ODocument> collection) {
        Collection<String> ids = new LinkedList<>();
        for (ODocument document : collection)
            ids.add(document.field(ID_FIELD).toString());
        return ids;
    }


    private Collection<ODocument> getResources(String query, Collection<Object> params) {
        List<ODocument> data = new LinkedList<>();
        OObjectDatabaseTx db = databaseStandards.getConnection();
        try {
            data = db.query(new OSQLSynchQuery<ODocument>(query), params);
        } catch (Exception e) {
            e.printStackTrace();
            //TODO exception
        }
        return data;

    }

}
