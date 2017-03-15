package org.fao.fenix.sdmx.d3s;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@ApplicationScoped
public class DataUtils {

    public InputStream getFastInputStream (final Collection<String> columns, final Iterator<Object[]> data) {
        return new InputStream() {
            int[] queue = new int[10240];
            int ri=0, wi=0;

            //add header
            {
                for (String column : columns) {
                    byte[] text = column.trim().getBytes();
                    queue[wi++] = '"';
                    for (int i=0; i<text.length; i++)
                        queue[wi++] = text[i];
                    queue[wi++] = '"';
                    queue[wi++] = ',';
                }
                queue[wi-1] = '\n';
            }
            //add row
            int addRow() {
                if (data.hasNext()) {
                    Object[] row = data.next();
                    for (int i=0; i<row.length; i++) {
                        if (row[i]!=null) {
                            if (row[i] instanceof String) {
                                queue[wi] = '"';
                                wi = wi == 10239 ? 0 : wi + 1;
                            }
                            for (Byte character : row[i]!=null ? row[i].toString().getBytes() : new byte[0]) {
                                queue[wi] = character;
                                wi = wi == 10239 ? 0 : wi + 1;
                            }
                            if (row[i] instanceof String) {
                                queue[wi] = '"';
                                wi = wi == 10239 ? 0 : wi + 1;
                            }
                        }
                        queue[wi] = i==row.length-1 ? '\n' : ',';
                        wi = wi == 10239 ? 0 : wi + 1;
                    }
                }
                return ri==wi ? -1 : queue[ri];
            }

            //consume
            @Override
            public int read() throws IOException {
                int character = ri==wi ? addRow() : queue[ri];
                ri = ri==10239 ? 0 : ri+1;
                return character;
            }
        };
    }


}
