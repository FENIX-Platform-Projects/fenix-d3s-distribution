package org.fao.fenix.flexmonster.d3s;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.msd.dto.type.DataType;
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


    public enum CodeLabelPlaceolders {
        code,codeFirst,label,labelFirst
    }
    public Iterator<Object[]> getCSV(final Iterator<Object[]> fenixData, Collection<DSDColumn> fenixColumns, Collection<String> csvColumns, Language[] languages, String codePattern) {
        if (languages==null)
            languages = new Language[0];
        String languageCode = languages.length==0 ? null : '_'+languages[0].getCode();
        //Parse code pattern
        if (codePattern==null || "".equals(codePattern))
            codePattern = languages.length>0 ? "<label> (<code>)" : "code";
        StringBuilder codeLabelPlaceoldersName = new StringBuilder();
        final String[] codeLabelParts = getCodeLabelParts(codePattern, codeLabelPlaceoldersName);
        final CodeLabelPlaceolders codeLabelPlaceolders = codeLabelPlaceoldersName.length()>0 ? CodeLabelPlaceolders.valueOf(codeLabelPlaceoldersName.toString()) : null;
        //Parse DSD to compact dataset respect to coded columns
        final int[] correspondentData = new int[fenixColumns.size()];
        final int[] correspondentLabel = new int[fenixColumns.size()];
        Arrays.fill(correspondentLabel, -1);
        final DataType[] dataTypes = new DataType[fenixColumns.size()];
        int rowLengthCount = 0;

        Map<String,Integer> codedColumnsMap = new HashMap<>();
        Iterator<DSDColumn> columnIterator = fenixColumns.iterator();
        for (int i=0; columnIterator.hasNext(); i++) {
            DSDColumn column = columnIterator.next();
            switch (dataTypes[rowLengthCount] = column.getDataType()) {
                case date:
                case month:
                    correspondentData[rowLengthCount++] = i;
                    break;
                case code:
                    codedColumnsMap.put(column.getId(), rowLengthCount);
                    correspondentData[rowLengthCount++] = i;
                    break;
                case text:
                    boolean label = false;
                    String id = column.getId();
                    for (Language language : languages)
                        if (id.endsWith('_'+language.getCode())) {
                            columnIterator.remove();
                            label = true;
                        }
                    if (label) {
                        if (id.endsWith(languageCode)) {
                            String correspondentCodeColumnId = id.substring(0, id.length() - languageCode.length());
                            Integer correspondentCodeColumnIndex = codedColumnsMap.get(correspondentCodeColumnId);
                            if (correspondentCodeColumnIndex != null)
                                correspondentLabel[correspondentCodeColumnIndex] = i;
                        }
                    } else {
                        correspondentData[rowLengthCount++] = i;
                    }
                    break;
                default:
                    correspondentData[rowLengthCount++] = i;
            }
        }
        final int rowLength = rowLengthCount;

        //Retrieve and update columns name
        csvColumns.addAll(getColumnsName(fenixColumns, languages));
        //Return csv iterator
        Iterator<Object[]> csvData = new Iterator<Object[]>() {
            @Override
            public boolean hasNext() {
                return fenixData.hasNext();
            }

            @Override
            public Object[] next() {
                Object[] rawRow = fenixData.next();
                if (rawRow!=null) {
                    Object[] row = new Object[rowLength];
                    for (int i=0; i<rowLength; i++) {
                        if (dataTypes[i]==DataType.date || dataTypes[i]==DataType.month) {
                            Integer date = (Integer)rawRow[correspondentData[i]];
                            if (date!=null) {
                                StringBuilder dateString = new StringBuilder(date);
                                dateString.insert(4, '-');
                                if (dataTypes[i] == DataType.date)
                                    dateString.insert(7, '-');
                                row[i] = dateString.toString();
                            } else
                                row[i] = null;
                        } else if (dataTypes[i]==DataType.code) {
                            String code = (String)rawRow[correspondentData[i]];
                            if (code!=null && codeLabelPlaceolders!=null) {
                                String label = correspondentLabel[i]>=0 ? (String)rawRow[correspondentLabel[i]] : null;
                                switch (codeLabelPlaceolders) {
                                    case code:
                                        row[i] = codeLabelParts[0]+code+codeLabelParts[1];
                                        break;
                                    case codeFirst:
                                        row[i] = codeLabelParts[0]+code+codeLabelParts[1]+(label!=null ? label : "")+codeLabelParts[2];
                                        break;
                                    case label:
                                        row[i] = codeLabelParts[0]+(label!=null ? label : "")+codeLabelParts[1];
                                        break;
                                    case labelFirst:
                                        row[i] = codeLabelParts[0]+(label!=null ? label : "")+codeLabelParts[1]+code+codeLabelParts[2];
                                        break;
                                }
                            } else
                                row[i] = code;
                        } else {
                            row[i] = rawRow[correspondentData[i]];
                        }
                    }
                    return row;
                }
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        return csvData;
        //return fenixData;
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
            //Add english as default language
            Language[] languages = requiredLanguages!=null ? Arrays.copyOf(requiredLanguages, requiredLanguages.length+1) : new Language[1];
            languages[languages.length-1] = Language.english;

            if (fenixColumns!=null)
                for (DSDColumn column : fenixColumns) {
                    String title = null;
                    Map<String,String> label = column.getTitle();
                    if (label!=null)
                        for (Language language : languages)
                            if ((title = label.get(language.getCode()))!=null)
                                break;
                    columnsName.add(title!=null ? title : "NA");
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



    private String[] getCodeLabelParts(String pattern, StringBuilder codeLabelPlaceoldersName) {
        CodeLabelPlaceolders codeLabelPlaceolders = null;
        //take into account label
        String[] parts = new String[] {pattern};
        int i = pattern.indexOf("<label>");
        if (i>=0) {
            parts = new String[]{pattern.substring(0, i), ((i+="<label>".length()) >= pattern.length() ? "" : pattern.substring(i))};
            codeLabelPlaceolders = CodeLabelPlaceolders.label;
        }
        //take into account code
        if (codeLabelPlaceolders == CodeLabelPlaceolders.label) {
            if ((i = parts[0].indexOf("<code>"))>0) {
                parts = new String[] { parts[0].substring(0, i), (i+="<code>".length())>=parts[0].length() ? "" : parts[0].substring(i), parts[1] };
                codeLabelPlaceolders = CodeLabelPlaceolders.codeFirst;
            } else if ((i = parts[1].indexOf("<code>"))>0) {
                parts = new String[] { parts[0], parts[1].substring(0, i), (i+="<code>".length())>=parts[1].length() ? "" : parts[1].substring(i) };
                codeLabelPlaceolders = CodeLabelPlaceolders.labelFirst;
            }
        } else if ((i = parts[0].indexOf("<code>"))>0) {
            parts = new String[] { parts[0].substring(0, i), (i+="<code>".length())>=parts[0].length() ? "" : parts[0].substring(i) };
            codeLabelPlaceolders = CodeLabelPlaceolders.code;
        }
        if (codeLabelPlaceolders!=null)
            codeLabelPlaceoldersName.append(codeLabelPlaceolders.name());
        //Return parts
        return parts;
    }

}
