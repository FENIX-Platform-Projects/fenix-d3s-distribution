package org.fao.ess.uneca.d3s;

import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.data.ResourceProxy;
import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.commons.msd.dto.full.Code;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.msd.dto.full.OjCode;
import org.fao.fenix.commons.msd.dto.full.OjCodeList;
import org.fao.fenix.commons.msd.dto.full.Period;
import org.fao.fenix.commons.msd.dto.templates.standard.combined.dataset.MetadataDSD;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.d3p.services.Processes;
import org.fao.fenix.d3s.cache.manager.listener.Context;
import org.fao.fenix.d3s.cache.manager.listener.DatasetAccessInfo;
import org.fao.fenix.d3s.cache.manager.listener.DatasetCacheListener;
import org.fao.fenix.commons.process.dto.Process;
import org.fao.fenix.d3s.msd.dao.MetadataResourceDao;
import org.fao.fenix.d3s.msd.services.spi.Resources;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;
import org.fao.fenix.d3s.server.tools.orient.OrientDatabase;
import org.fao.fenix.d3s.server.tools.orient.OrientServer;

import javax.inject.Inject;
import java.util.*;

@Context({"uneca"})
public class UnecaDatasetDistinctCacheListener implements DatasetCacheListener {
    @Inject DatabaseStandards dbParameters;
    @Inject OrientServer client;
    @Inject Processes processesService;
    @Inject MetadataResourceDao metradataDao;
    @Inject Resources resourcesService;



    @Override
    public boolean updating(DatasetAccessInfo datasetInfo) throws Exception {
        return false;
    }

    @Override
    public boolean updated(DatasetAccessInfo datasetInfo) throws Exception {
        MeIdentification<DSDDataset> metadata = datasetInfo.getMetadata();
        DSDDataset dsd = metadata.getDsd();
        Map<String, DSDColumn> columnsMap = new HashMap<>();
        for (DSDColumn column : dsd.getColumns())
            columnsMap.put(column.getId(), column);

        //OObjectDatabaseTx connection = client.getODatabase(OrientDatabase.msd);
        OObjectDatabaseTx connection = dbParameters.getConnection();
        try {
            //dbParameters.setConnection(connection);
            Map<String, Resource<DSDCodelist, Code>> codelists = getCodeLists(dsd.getColumns());
            Map<String,Code> codesMap = getCodesMap(codelists.values());

            ResourceProxy resource = processesService.apply(metadata.getUid(), metadata.getVersion(), new Process[]{new Process("dsdDistinct")}, null);
            MetadataDSD processedMetadata = (MetadataDSD) resource.getMetadata();
            org.fao.fenix.commons.msd.dto.templates.standard.dsd.DSDDataset processedDsd = processedMetadata.getDsd();
            for (org.fao.fenix.commons.msd.dto.templates.standard.dsd.DSDColumn processedColumn : processedDsd.getColumns())
                if (columnsMap.containsKey(processedColumn.getId()))
                    columnsMap.get(processedColumn.getId()).setValues(convert(processedColumn.getValues(), codelists, codesMap));
            connection.save(dsd);
        } finally {
            //connection.close();
        }
        return false;
    }


    @Override
    public boolean removing(DatasetAccessInfo datasetInfo) throws Exception {
        DSDDataset dsd = ((MeIdentification<DSDDataset>)datasetInfo.getMetadata()).getDsd();
        Collection<DSDColumn> columns = dsd!=null ? dsd.getColumns() : null;
        if (columns!=null)
            for (DSDColumn column : columns)
                column.setValues(null);
        dbParameters.getConnection().save(dsd);
        return false;
    }



    //Utils
    private DSDDomain convert(org.fao.fenix.commons.msd.dto.templates.standard.dsd.DSDDomain processedValues, Map<String, Resource<DSDCodelist, Code>> codelists, Map<String,Code> codesMap) {
        if (processedValues!=null) {
            DSDDomain values = new DSDDomain();
            values.setCodes(convert(processedValues.getCodes(), codelists, codesMap));
            values.setPeriod(convert(processedValues.getPeriod()));
            values.setEnumeration(processedValues.getEnumeration());
            values.setTimeList(processedValues.getTimeList());
            return values;
        } else
            return null;
    }

    private Collection<OjCodeList> convert(Collection<org.fao.fenix.commons.msd.dto.templates.standard.metadata.OjCodeList> ojCodeList, Map<String, Resource<DSDCodelist, Code>> codelists, Map<String,Code> codesMap) {
        if (ojCodeList!=null) {
            Collection<OjCodeList> result = new LinkedList<>();
            for (org.fao.fenix.commons.msd.dto.templates.standard.metadata.OjCodeList o : ojCodeList)
                result.add(convert(o, codelists, codesMap));
            return result;
        } else
            return null;
    }
    private Collection<OjCode> convertCodes(Collection<org.fao.fenix.commons.msd.dto.templates.standard.metadata.OjCode> ojCode, String codelistId, Map<String,Code> codesMap) {
        if (ojCode!=null) {
            Collection<OjCode> result = new LinkedList<>();
            for (org.fao.fenix.commons.msd.dto.templates.standard.metadata.OjCode o : ojCode)
                result.add(convert(o, codelistId, codesMap));
            return result;
        } else
            return null;
    }
    private OjCodeList convert(org.fao.fenix.commons.msd.dto.templates.standard.metadata.OjCodeList ojCodeList, Map<String, Resource<DSDCodelist, Code>> codelists, Map<String,Code> codesMap) {
        if (ojCodeList!=null) {
            org.fao.fenix.commons.msd.dto.templates.standard.metadata.MeIdentification linkedMetadata = ojCodeList.getLinkedCodeList();
            String id = linkedMetadata!=null ? getId(linkedMetadata.getUid(), linkedMetadata.getVersion()) : null;

            OjCodeList result = new OjCodeList();
            result.setIdCodeList(ojCodeList.getIdCodeList());
            result.setVersion(ojCodeList.getVersion());
            result.setCodes(convertCodes(ojCodeList.getCodes(), id, codesMap));
            result.setExtendedName(linkedMetadata==null ? ojCodeList.getExtendedName() : null);
            result.setLinkedCodeList(linkedMetadata!=null && codelists.containsKey(id) ? codelists.get(id).getMetadata() : null);
            return result;
        } else
            return null;
    }
    private OjCode convert(org.fao.fenix.commons.msd.dto.templates.standard.metadata.OjCode ojCode, String codelistId, Map<String,Code> codesMap) {
        if (ojCode!=null) {
            OjCode result = new OjCode();
            result.setCode(ojCode.getCode());
            result.setLinkedCode(codelistId!=null ? codesMap.get(codelistId+'_'+ojCode.getCode()) : null);
            result.setLabel(result.getLinkedCode()==null ? ojCode.getLabel() : null);
            return result;
        } else
            return null;
    }
    private Period convert(org.fao.fenix.commons.msd.dto.templates.standard.metadata.Period period) {
        return period!=null ? new Period(period.getFrom(), period.getTo()) : null;
    }




    private Map<String,Code> getCodesMap(Collection<Resource<DSDCodelist, Code>> codelists) {
        Map<String,Code> codesMap = new HashMap<>();
        if (codelists!=null)
            for (Resource<DSDCodelist, Code> codelist : codelists)
                fillCodesMap(getId(codelist.getMetadata().getUid(), codelist.getMetadata().getVersion())+'_', codelist.getData(), codesMap);
        return codesMap;
    }
    private void fillCodesMap (String prefix, Collection<Code> codes, Map<String,Code> codesMap) {
        if (codes!=null)
            for (Code code : codes) {
                codesMap.put(prefix + code.getCode(), code);
                fillCodesMap(prefix, code.getChildren(), codesMap);
            }
    }

    private Map<String, Resource<DSDCodelist, Code>> getCodeLists(Collection<DSDColumn> columns) throws Exception {
        Set<String> existingCodelists = new HashSet<>();
        Map<String, Resource<DSDCodelist, Code>> codeLists = new HashMap<>();
        for (DSDColumn column : columns)
            if (column.getDataType()== DataType.code) {
                OjCodeList domain = column.getDomain().getCodes().iterator().next();
                Resource<DSDCodelist, Code> codelist = existingCodelists.add(getId(domain.getIdCodeList(), domain.getVersion())) ? resourcesService.loadResource(domain.getIdCodeList(), domain.getVersion()) : null;
                if (codelist!=null)
                    codeLists.put(getId(codelist.getMetadata().getUid(), codelist.getMetadata().getVersion()), codelist);
            }
        return codeLists;
    }
    private String getId(String uid, String version) {
        return uid!=null ? (version!=null ? uid+'_'+version : uid) : "";
    }


}
