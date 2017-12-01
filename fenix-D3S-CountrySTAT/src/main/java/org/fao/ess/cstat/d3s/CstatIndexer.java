package org.fao.ess.cstat.d3s;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import org.apache.log4j.Logger;
import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.commons.msd.dto.type.RepresentationType;
import org.fao.fenix.commons.utils.Context;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.d3s.msd.listener.ResourceListener;
import org.fao.fenix.d3s.msd.services.rest.ResourcesService;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.*;

@ApplicationScoped
@Context({
        "cstat_afg",
        "cstat_ago",
        "cstat_aze",
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
public class CstatIndexer implements ResourceListener {
    private static final Logger LOGGER = Logger.getLogger("info");

    private @Inject DatabaseStandards crossReferences;
    private @Inject ResourcesService resourcesService;

    //Events
    @Override
    public void insertingMetadata(MeIdentification metadata) {}

    @Override
    public void insertedMetadata(MeIdentification metadata) {}

    @Override
    public void updatingMetadata(MeIdentification metadata) {}

    @Override
    public void updatedMetadata(MeIdentification metadata) {}

    @Override
    public void appendingMetadata(MeIdentification metadata) {}

    @Override
    public void appendedMetadata(MeIdentification metadata) {}

    @Override
    public void removingMetadata(MeIdentification metadata) {}

    @Override
    public void removedMetadata(String uid, String version) {
        removeIndexInfo(uid, version);
    }

    @Override
    public void insertingResource(Resource resource) {}

    @Override
    public void insertedResource(MeIdentification metadata) {
        updateIndexInfo(metadata);
    }

    @Override
    public void updatingResource(Resource resource) {
    }

    @Override
    public void updatedResource(MeIdentification metadata) {
        updateIndexInfo(metadata);
    }

    @Override
    public void appendingResource(Resource resource) {}

    @Override
    public void appendedResource(MeIdentification metadata) {
        updateIndexInfo(metadata);
    }

    @Override
    public void removingResource(MeIdentification metadata) {}

    @Override
    public void removedResource(String uid, String version) {
        removeIndexInfo(uid, version);
    }

    @Override
    public <T extends DSD> void updatingDSD(T dsd, MeIdentification metadata) {}

    @Override
    public <T extends DSD> void updatedDSD(MeIdentification metadata) {}

    @Override
    public <T extends DSD> void appendingDSD(T dsd, MeIdentification metadata) {}

    @Override
    public <T extends DSD> void appendedDSD(MeIdentification metadata) {}

    @Override
    public <T extends DSD> void removingDSD(MeIdentification metadata) {}

    @Override
    public <T extends DSD> void removedDSD(MeIdentification metadata) {}

    @Override
    public void removingData(MeIdentification metadata) {}

    @Override
    public void removedData(MeIdentification metadata) {
        removeIndexInfo(metadata.getUid(), metadata.getVersion());
    }

    @Override
    public void updatingData(MeIdentification metadata) {}

    @Override
    public void updatedData(MeIdentification metadata) {}







    //LOGIC
    private void removeIndexInfo(String uid, String version) {
        try {
            OObjectDatabaseTx connection = crossReferences.getConnection();
            connection.command(new OCommandSQL("delete from DataIndex where index|id = ?")).execute(getId(uid,version));
            connection.commit();
        } catch (Exception ex) {
            LOGGER.error("Error trying to index CountrySTAT data", ex);
        }
    }

    private void updateIndexInfo(MeIdentification metadata) {
        try {
            if (getResourceType(metadata)==RepresentationType.dataset && isDSDAvailable(metadata)) {
                OObjectDatabaseTx connection = crossReferences.getConnection();

                ODocument indexDocument = getIndexDocument(getId(metadata), connection);
                updateFields(metadata, indexDocument, connection);

                connection.getUnderlying().save(indexDocument);
            }
        } catch (Exception ex) {
            LOGGER.error("Error trying to index CountrySTAT data", ex);
        }
    }


    private void updateFields(MeIdentification metadata, ODocument indexDocument, OObjectDatabaseTx connection) throws Exception {
        ODocument document = connection.getRecordByUserObject(metadata, false);
        //ID
        String uid = document!=null ? (String)document.field("uid") : null;
        if (uid != null) {
            indexDocument.field("index|id", getId(uid, (String) document.field("version")));
            //Freetext search support
            indexDocument.field("index|freetext", getFreeTextValue(loadData(metadata)));
            //Other fields
            indexDocument.field("index|dsd|contextSystem", metadata.getDsd().getContextSystem());
        }
    }

    private void updateStatusInfo(MeIdentification metadata) {
        try {
            if (getResourceType(metadata)==RepresentationType.dataset && isDSDAvailable(metadata)) {
                OObjectDatabaseTx connection = crossReferences.getConnection();

                ODocument indexDocument = getIndexDocument(getId(metadata), connection);
                updateFields(metadata, indexDocument, connection);

                connection.getUnderlying().save(indexDocument);
            }
        } catch (Exception ex) {
            LOGGER.error("Error trying to index CountrySTAT data", ex);
        }
    }


    private Resource<DSDDataset, Object[]> loadData(MeIdentification metadata) throws Exception {
        //Require english and french labels
        Language[] currentLanguages = crossReferences.getLanguageInfo();
        crossReferences.setLanguageInfo(new Language[] {Language.english, Language.france});
        //Retrieve data
        Resource<DSDDataset, Object[]> dataset = resourcesService.loadResource(metadata.getUid(), metadata.getVersion());
        //Restore client required languages
        crossReferences.setLanguageInfo(currentLanguages);
        //Return data
        return dataset;
    }

    private String getFreeTextValue (Resource<DSDDataset, Object[]> resource) throws Exception {
        StringBuilder text = new StringBuilder();
        //Find inclusions
        boolean[] inclusionMask = new boolean[resource.getMetadata().getDsd().getColumns().size()];
        Iterator<DSDColumn> columnIterator = resource.getMetadata().getDsd().getColumns().iterator();
        for (int i=0; columnIterator.hasNext(); i++) {
            DSDColumn column = columnIterator.next();
            inclusionMask[i] = !"value".equals(column.getSubject());
        }
        //Parse table data
        Set<String> words = new HashSet<>();
        for (Object[] row : resource.getData())
            for (int i=0; i<row.length; i++)
                if (inclusionMask[i] && row[i]!=null)
                    for (String word : getWords(row[i].toString()))
                        words.add(word);
        //Append table data
        for (String word : words)
            text.append(word).append(' ');
        //Return text
        return text.toString();
    }

    private String[] getWords (String text) {
        return text.toLowerCase().split("[\\s\\.\"\\!£$\\%\\&/\\(\\)\\='\\?\\^\\[\\]\\*\\+\\,;\\:]+");
    }

    private boolean isDSDAvailable(MeIdentification metadata) {
        try {
            return ((DSDDataset)metadata.getDsd()).getColumns().size()>0;
        } catch (Exception ex) {
            return false;
        }
    }

    private RepresentationType getResourceType(MeIdentification metadata) {
        MeContent meContent = metadata.getMeContent();
        return meContent!=null ? meContent.getResourceRepresentationType() : null;
    }



    //Utils
    private String getId(MeIdentification metadata) {
        return getId(metadata.getUid(), metadata.getVersion());
    }
    private String getId(String uid, String version) {
        return uid+(version!=null && !version.trim().equals("") ? '|'+version : "");
    }

    private ODocument getIndexDocument(String id, OObjectDatabaseTx connection) {
        OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<>("select from DataIndex where index|id = ?");
        List<ODocument> existingIndexes = connection.getUnderlying().query(query, id);
        return existingIndexes!=null && existingIndexes.size()>0 ? existingIndexes.iterator().next() : new ODocument("DataIndex");
    }

}


