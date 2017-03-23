package org.fao.fenix.flexmonster.d3s;

import com.flexmonster.compressor.Compressor;
import javassist.*;
import javassist.util.proxy.*;
import org.fao.fenix.commons.msd.dto.data.ResourceProxy;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.templates.ResponseBeanFactory;
import org.fao.fenix.commons.msd.dto.templates.ResponseHandler;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.msd.dto.type.RepresentationType;
import org.fao.fenix.commons.utils.JSONUtils;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.*;

@Provider
@Produces("application/flexmonster")
public class ResourceExportProvider  implements MessageBodyWriter<ResourceProxy> {
    @Inject DataUtils dataUtils;
    @Inject HttpServletRequest request;


    @Override
    public boolean isWriteable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return true;
    }

    @Override
    public long getSize(ResourceProxy resource, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return 0;
    }

    @Override
    public void writeTo(ResourceProxy resource, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> multivaluedMap, OutputStream outputStream) throws IOException, WebApplicationException {
        try {
            //Validation
            Object metadataProxy = resource!=null ? resource.getMetadata() : null;
            if (metadataProxy==null)
                throw new WebApplicationException(Response.Status.NOT_FOUND);
            if (!isDataset(metadataProxy))
                throw new WebApplicationException("Flexmonster export can be applied only on a dataset resource", Response.Status.BAD_REQUEST);
            Collection<DSDColumn> columns = getColumns(metadataProxy);
            if (columns.size()==0)
                throw new WebApplicationException("Flexmonster export can be applied only on a completely defined dataset resource", Response.Status.BAD_REQUEST);
            //Retrieve flexmonster input stream and update columns to produce dsd
            InputStream flexMonsterDataStream = prepareExportData(columns, (Collection<Object[]>)resource.getData());
            //Export resource
            outputStream.write(("{\n\"size\" : "+resource.getSize()+",\n\n\"metadata\" : \n"+JSONUtils.toJSON(getMetadataProxy(metadataProxy, columns))+",\n\n\"data\" : \n\"").getBytes());
            dataUtils.copyData(flexMonsterDataStream, outputStream);
            outputStream.write("\"\n}".getBytes());
            outputStream.flush();
        } catch (Exception e) {
            throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }



    //LOGIC

    private static Map<String,Class> newMetadataClasses = new HashMap<>();
    private Object getMetadataProxy (Object metadata, Collection<DSDColumn> columns) throws Exception {
        ClassPool pool = ClassPool.getDefault();
        String metadataClassName = ((ProxyObject) metadata).getHandler().getClass().getName();
        CtClass metadataClass = pool.getCtClass(metadataClassName);
        CtMethod getDsdMethod = metadataClass.getDeclaredMethod("getDsd");
        String dsdProxyClassName = getDsdMethod.getReturnType().getName();

        Class newMetadataClass = newMetadataClasses.get(metadataClassName);
        if (newMetadataClass==null) {
            CtClass newMetadataCtClass = pool.makeClass("com.flexmonster.MeIdentification", metadataClass);

            CtField flexmonsterDsdField = new CtField(pool.getCtClass("java.lang.Object"), "flexmonsterDsd", newMetadataCtClass);
            flexmonsterDsdField.setModifiers(Modifier.PUBLIC);
            newMetadataCtClass.addField(flexmonsterDsdField);

            CtMethod newGetDsdMethod = CtNewMethod.copy(getDsdMethod,newMetadataCtClass, null);
            newGetDsdMethod.setBody("{ return ("+dsdProxyClassName+") flexmonsterDsd; }");
            newMetadataCtClass.addMethod(newGetDsdMethod);

            newMetadataClasses.put(metadataClassName, newMetadataClass = newMetadataCtClass.toClass());
        }

        DSDDataset newDsd = new DSDDataset();
        try {
            Object dsd = metadata.getClass().getMethod("getDsd").invoke(metadata);
            try { newDsd.setContextSystem((String)dsd.getClass().getMethod("getContextSystem").invoke(dsd)); } catch (Exception ex) {}
            try { newDsd.setUserName((String)dsd.getClass().getMethod("getUserName").invoke(dsd)); } catch (Exception ex) {}
        } catch (Exception ex) {}
        newDsd.setColumns(columns);
        Object newDSDProxy = ResponseBeanFactory.getInstance((Class<? extends ResponseHandler>)Class.forName(dsdProxyClassName), newDsd);

        //return metadata;
        Object proxy = ResponseBeanFactory.getInstance(newMetadataClass, metadata);
        proxy.getClass().getField("flexmonsterDsd").set(proxy, newDSDProxy);
        return proxy;
    }


    private InputStream prepareExportData(Collection<DSDColumn> columns, Collection<Object[]> data) throws Exception {
        //Create CSV
        Collection<String> columnsName = new LinkedList<>();
        Iterator<Object[]> csvIterator = dataUtils.getCSV(data!=null ? data.iterator() : new LinkedList<Object[]>().iterator(), columns, columnsName, DatabaseStandards.getLanguageInfo(), request.getParameter("codeFormat"));
        //create flexmonster format stream
        return Compressor.compressStream(dataUtils.getFastInputStream(columnsName,csvIterator));
        //return dataUtils.getFastInputStream(columnsName,csvIterator);
    }



    private boolean isDataset(Object metadataProxy) {
        //Try with metadata proxy
        try {
            Object meContent = metadataProxy.getClass().getMethod("getMeContent").invoke(metadataProxy);
            return meContent.getClass().getMethod("getResourceRepresentationType").invoke(meContent) == RepresentationType.dataset;
        } catch (Exception e) { }
        //If representation type isn't into the proxy try with original metadata
        try { return dataUtils.loadMetadata(metadataProxy).getMeContent().getResourceRepresentationType()==RepresentationType.dataset; } catch (Exception ex) {}
        //default
        return false;
    }

    private Collection<DSDColumn> getColumns(Object metadataProxy) {
        Collection<DSDColumn> columns = new LinkedList<>();
        //Try with metadata proxy
        try {
            Object dsd = metadataProxy.getClass().getMethod("getDsd").invoke(metadataProxy);
            for (Object columnProxy : (Collection) dsd.getClass().getMethod("getColumns").invoke(dsd))
                columns.add(getColumn(columnProxy));
        } catch (Exception ex) { }
        //If dsd isn't into the proxy try with original metadata
        if (columns.size()==0)
            try { columns.addAll(((DSDDataset)dataUtils.loadMetadata(metadataProxy).getDsd()).getColumns()); } catch (Exception ex) {}
        //Return found columns
        return columns;
    }

    private DSDColumn getColumn(Object columnProxy) {
        if (columnProxy!=null) {
            DSDColumn column = new DSDColumn();
            try { column.setId((String)columnProxy.getClass().getMethod("getId").invoke(columnProxy)); } catch (Exception ex) {}
            try { column.setDataType((DataType) columnProxy.getClass().getMethod("getDataType").invoke(columnProxy)); } catch (Exception ex) {}
            try { column.setKey((Boolean) columnProxy.getClass().getMethod("getKey").invoke(columnProxy)); } catch (Exception ex) {}
            try { column.setSubject((String)columnProxy.getClass().getMethod("getSubject").invoke(columnProxy)); } catch (Exception ex) {}
            try { column.setTitle((Map<String,String>)columnProxy.getClass().getMethod("getTitle").invoke(columnProxy)); } catch (Exception ex) {}
            try { column.setSupplemental((Map<String,String>)columnProxy.getClass().getMethod("getSupplemental").invoke(columnProxy)); } catch (Exception ex) {}
            //TODO Add DSDDomain?
            return column;
        }
        return null;
    }





}
