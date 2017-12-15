package org.fao.ess.wiews.d3s.dao;

import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.d3s.wds.dataset.WDSDatasetDao;

import java.util.Iterator;
import java.util.Map;

public class WiewsEmptyDatasetDAO extends WDSDatasetDao {

    @Override
    public boolean init() {
        return true;
    }

    @Override
    public void init(Map<String, String> properties) throws Exception {
    }

    @Override
    public Iterator<Object[]> loadData(MeIdentification resource) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void storeData(MeIdentification resource, Iterator<Object[]> data, boolean overwrite) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteData(MeIdentification resource) throws Exception {
        throw new UnsupportedOperationException();
    }











}
