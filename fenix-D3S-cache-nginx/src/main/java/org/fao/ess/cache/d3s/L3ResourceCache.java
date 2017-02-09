package org.fao.ess.cache.d3s;

import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.DSD;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.d3s.msd.listener.ResourceListener;
import org.fao.fenix.d3s.server.init.MainController;
import org.fao.fenix.d3s.server.tools.orient.OrientDao;

import javax.inject.Inject;

public class L3ResourceCache extends OrientDao implements ResourceListener {
    @Inject MainController main;
    private boolean initialized;

    private static void init(MainController main) throws Exception {
        main.getInitParameter("cache.l3.host");
        main.getInitParameter("cache.l3.user");
        main.getInitParameter("cache.l3.password");
        main.getInitParameter("cache.l3.command");
    }

    @Override
    public void insertingMetadata(MeIdentification metadata) {

    }

    @Override
    public void insertedMetadata(MeIdentification metadata) {

    }

    @Override
    public void updatingMetadata(MeIdentification metadata) {

    }

    @Override
    public void updatedMetadata(MeIdentification metadata) {

    }

    @Override
    public void appendingMetadata(MeIdentification metadata) {

    }

    @Override
    public void appendedMetadata(MeIdentification metadata) {

    }

    @Override
    public void removingMetadata(MeIdentification metadata) {

    }

    @Override
    public void removedMetadata(String uid, String version) {

    }

    @Override
    public void insertingResource(Resource resource) {

    }

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
    public void appendingResource(Resource resource) {

    }

    @Override
    public void appendedResource(MeIdentification metadata) {

    }

    @Override
    public void removingResource(MeIdentification metadata) {

    }

    @Override
    public void removedResource(String uid, String version) {

    }

    @Override
    public <T extends DSD> void updatingDSD(T dsd, MeIdentification metadata) {

    }

    @Override
    public <T extends DSD> void updatedDSD(MeIdentification metadata) {

    }

    @Override
    public <T extends DSD> void appendingDSD(T dsd, MeIdentification metadata) {

    }

    @Override
    public <T extends DSD> void appendedDSD(MeIdentification metadata) {

    }

    @Override
    public <T extends DSD> void removingDSD(MeIdentification metadata) {

    }

    @Override
    public <T extends DSD> void removedDSD(MeIdentification metadata) {

    }

    @Override
    public void removingData(MeIdentification metadata) {

    }

    @Override
    public void removedData(MeIdentification metadata) {

    }
}
