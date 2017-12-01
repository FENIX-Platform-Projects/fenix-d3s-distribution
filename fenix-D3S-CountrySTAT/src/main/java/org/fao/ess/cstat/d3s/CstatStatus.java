package org.fao.ess.cstat.d3s;

import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import org.apache.log4j.Logger;
import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.commons.msd.dto.type.RepresentationType;
import org.fao.fenix.commons.utils.Context;
import org.fao.fenix.d3s.msd.listener.ResourceListener;
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
public class CstatStatus implements ResourceListener {
    private static final Logger LOGGER = Logger.getLogger("info");

    private @Inject DatabaseStandards crossReferences;

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
    }

    @Override
    public void insertingResource(Resource resource) {}

    @Override
    public void insertedResource(MeIdentification metadata) {
    }

    @Override
    public void updatingResource(Resource resource) {
    }

    @Override
    public void updatedResource(MeIdentification metadata) {
    }

    @Override
    public void appendingResource(Resource resource) {}

    @Override
    public void appendedResource(MeIdentification metadata) {
    }

    @Override
    public void removingResource(MeIdentification metadata) {}

    @Override
    public void removedResource(String uid, String version) {
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
    }

    @Override
    public void updatingData(MeIdentification metadata) {}

    @Override
    public void updatedData(MeIdentification metadata) {
        updateConfidentialityStatus(metadata);
    }


    //LOGIC
    private void updateConfidentialityStatus(MeIdentification metadata) {
        try {
            if (getResourceType(metadata)==RepresentationType.dataset && isDSDAvailable(metadata)) {
                OObjectDatabaseTx connection = crossReferences.getConnection();

                MeAccessibility meAccessibility = metadata.getMeAccessibility();
                if (meAccessibility==null)
                    meAccessibility = new MeAccessibility();
                SeConfidentiality seConfidentiality = meAccessibility.getSeConfidentiality();
                if (seConfidentiality==null)
                    seConfidentiality = new SeConfidentiality();

                OjCodeList confidentialityStatus = new OjCodeList();
                OjCode statusCode = new OjCode();
                statusCode.setCode("F");
                confidentialityStatus.setIdCodeList("CL_CONF_STATUS");
                confidentialityStatus.setVersion("1.0");
                confidentialityStatus.setCodes(Arrays.asList(statusCode));

                seConfidentiality.setConfidentialityStatus(confidentialityStatus);
                meAccessibility.setSeConfidentiality(seConfidentiality);
                metadata.setMeAccessibility(meAccessibility);

                connection.save(metadata);
                System.out.println("Saved CSTAT status: "+metadata.getMeAccessibility().getSeConfidentiality().getConfidentialityStatus().getCodes().iterator().next().getCode());
            }
        } catch (Exception ex) {
            LOGGER.error("Error trying to index CountrySTAT data", ex);
        }
    }


    //Utils
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

}


