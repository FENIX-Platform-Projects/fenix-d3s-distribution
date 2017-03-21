package org.fao.fenix.flexmonster.d3s;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.d3s.msd.services.spi.Resources;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

@ApplicationScoped
public class DataUtils {

    @Inject Resources resourceSrevice;


    public InputStream getFastInputStream (final Collection<String> columns, final Iterator<Object[]> data) {
        return new InputStream() {
            int[] buffer = new int[10240];
            int ri=0, wi=0;

            //add header
            {
                for (String column : columns) {
                    byte[] text = column.trim().getBytes();
                    buffer[wi++] = '"';
                    for (int i=0; i<text.length; i++)
                        buffer[wi++] = text[i];
                    buffer[wi++] = '"';
                    buffer[wi++] = ',';
                }
                buffer[wi-1] = '\n';
            }
            //add row
            int addRow() {
                ri = wi = 0;
                if (data.hasNext()) {
                    Object[] row = data.next();
                    for (int i=0; i<row.length; i++) {
                        if (row[i]!=null) {
                            if (row[i] instanceof String)
                                buffer[wi++] = '"';
                            for (Byte character : row[i]!=null ? row[i].toString().getBytes() : new byte[0])
                                buffer[wi++] = character;
                            if (row[i] instanceof String)
                                buffer[wi++] = '"';
                        }
                        buffer[wi++] = i==row.length-1 ? '\n' : ',';
                    }
                }
                return ri==wi ? -1 : buffer[ri++];
            }

            //consume
            @Override
            public int read() throws IOException {
                return ri==wi ? addRow() : buffer[ri++];
            }
        };
    }


    public Iterator<Object[]> getCSV(Iterator<Object[]> fenixData, Collection<DSDColumn> fenixColumns, Collection<String> csvColumns, Language[] languages, String codePattern) {
        csvColumns.addAll(getColumnsName(fenixColumns, languages));
        return fenixData; //TODO
    }

    public void copyData(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[10240];
        for (int length = in.read(buffer); length > 0; length = in.read(buffer))
            out.write(buffer, 0, length);
        out.flush();
    }

    private Collection<String> getColumnsName(Collection<DSDColumn> fenixColumns, Language[] requiredLanguages) {
        Collection<String> columnsName = new LinkedList<>();

        try {
            if (requiredLanguages==null || requiredLanguages.length==0)
                requiredLanguages = new Language[]{Language.english};

            if (fenixColumns!=null)
                for (DSDColumn column : fenixColumns) {
                    String title = null;
                    Map<String,String> label = column.getTitle();
                    if (label!=null)
                        for (Language language : requiredLanguages)
                            if ((title = label.get(language.getCode()))!=null)
                                break;
                    columnsName.add(title);
                }
        } catch (Exception ex) { }

        return columnsName;
    }


    public MeIdentification loadMetadata (Object metadataProxy) {
        try {
            return loadMetadata(
                    (String) metadataProxy.getClass().getMethod("getUid").invoke(metadataProxy),
                    (String) metadataProxy.getClass().getMethod("getVersion").invoke(metadataProxy)
            );
        } catch (Exception ex) {
            return null;
        }
    }
    public MeIdentification loadMetadata (String uid, String version) {
        try {
            return resourceSrevice.loadMetadata(uid, version);
        } catch (Exception ex) {
            return null;
        }
    }

}
