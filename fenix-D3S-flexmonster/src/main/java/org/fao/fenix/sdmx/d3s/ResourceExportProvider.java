package org.fao.fenix.sdmx.d3s;

import com.flexmonster.compressor.Compressor;
import org.fao.fenix.commons.msd.dto.data.ResourceProxy;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;

import javax.inject.Inject;
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
import java.io.Reader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

@Provider
@Produces("application/flexmonster")
public class ResourceExportProvider  implements MessageBodyWriter<ResourceProxy> {
    @Inject DataUtils dataUtils;

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
            if (resource==null)
                throw new WebApplicationException(Response.Status.NOT_FOUND);
            if (!isDataset(resource))
                throw new WebApplicationException("Flexmonster export can be applied only on a dataset resource", Response.Status.BAD_REQUEST);
            Collection<String> columns = getColumns(resource);
            if (columns==null || columns.size()==0)
                throw new WebApplicationException("Flexmonster export can be applied only on a completely defined dataset resource", Response.Status.BAD_REQUEST);
            //Export data
            export(columns, (Collection<Object[]>)resource.getData(), outputStream);
        } catch (Exception e) {
            throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }


    //LOGIC
    private void export(Collection<String> columns, Collection<Object[]> data, OutputStream out) throws Exception {
        //create flexmonster format strem
        InputStream in = Compressor.compressStream(dataUtils.getFastInputStream(columns,data!=null ? data.iterator() : new LinkedList<Object[]>().iterator()));
        //copy to out stream
        byte[] buffer = new byte[10240];
        for (int length = in.read(buffer); length > 0; length = in.read(buffer))
            out.write(buffer, 0, length);
        out.flush();
    }



    private boolean isDataset(ResourceProxy resource) {
        //TODO
        return false;
    }

    private Collection<String> getColumns(ResourceProxy resource) {
        //TODO
        return null;
    }


}
