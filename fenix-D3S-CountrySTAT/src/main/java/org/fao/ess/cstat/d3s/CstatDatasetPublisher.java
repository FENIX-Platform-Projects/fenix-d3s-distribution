package org.fao.ess.cstat.d3s;

import org.apache.log4j.Logger;
import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.commons.utils.Context;
import org.fao.fenix.d3s.msd.listener.ResourceListener;
import org.fao.fenix.d3s.msd.services.spi.Resources;

import javax.inject.Inject;
import java.util.Arrays;

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
public class CstatDatasetPublisher implements ResourceListener {
    private static final Logger LOGGER = Logger.getLogger("info");

    private static ThreadLocal<Integer> resourceSize = new ThreadLocal<>();
    private @Inject Resources resourceService;


    @Override
    public void insertedMetadata(MeIdentification metadata) {
        setConfidentialityStatus("N", metadata);
    }

    @Override
    public void updatingResource(Resource resource) {
        resourceSize.set(resource!=null && resource.getData() !=null ? resource.getData().size() : 0);
    }

    @Override
    public void updatedResource(MeIdentification metadata) {
        setConfidentialityStatus(resourceSize.get()>0 ? "F" : "N", metadata);
    }


    //Unimplemented
    @Override
    public void insertingMetadata(MeIdentification metadata) {}


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
    public void removedMetadata(String uid, String version) {}

    @Override
    public void insertingResource(Resource resource) {}

    @Override
    public void insertedResource(MeIdentification metadata) {}

    @Override
    public void appendingResource(Resource resource) {}

    @Override
    public void appendedResource(MeIdentification metadata) {}

    @Override
    public void removingResource(MeIdentification metadata) {}

    @Override
    public void removedResource(String uid, String version) {}

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
    public void removedData(MeIdentification metadata) {}



    //Utils
    private void setConfidentialityStatus(String code, MeIdentification metadata) {
        MeIdentification updateMetadata = new MeIdentification();
        updateMetadata.setUid(metadata.getUid());
        updateMetadata.setVersion(metadata.getVersion());

        MeAccessibility meAccessibility = new MeAccessibility();
        updateMetadata.setMeAccessibility(meAccessibility);

        SeConfidentiality seConfidentiality = new SeConfidentiality();
        meAccessibility.setSeConfidentiality(seConfidentiality);

        OjCodeList confidentialityStatus = new OjCodeList();
        seConfidentiality.setConfidentialityStatus(confidentialityStatus);
        confidentialityStatus.setIdCodeList("CL_CONF_STATUS");
        confidentialityStatus.setVersion("1.0");

        OjCode confidentialityStatusCode = new OjCode();
        confidentialityStatus.setCodes(Arrays.asList(confidentialityStatusCode));
        confidentialityStatusCode.setCode(code);

        try {
            resourceService.appendMetadata(updateMetadata);
        } catch (Exception e) {
            LOGGER.error("CSTAT confidentiality status update error", e);
        }
    }

}
