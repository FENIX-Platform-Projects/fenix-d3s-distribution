package org.fao.ess.d3s.cache.postgres.massive;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class AdamMassiveQueryTest implements Runnable {
    Source source;

    int queryIterationCount = 100;

    String query = "SELECT year, donorcode, donorname, agencycode, agencyname, crsid, projectnumber, \n" +
            "       recipientcode, recipientname, regioncode, regionname, flowcode, \n" +
            "       flowname, bimulticode, flowcategory, financetypecode, aidtypecode, \n" +
            "       value, currencycode, projectshortdescription, projecttitle, purposecode, \n" +
            "       purposename, sectorcode, sectorname, channelcode, channelname, \n" +
            "       channelreportedname, geography, projectlongdescription, unitcode, \n" +
            "       unitname, flowamountcode, flowamountname, matching_row_idx, donorcode_en, \n" +
            "       donorcode_es, donorcode_fr, donorcode_de, agencycode_en, agencycode_es, \n" +
            "       agencycode_fr, agencycode_de, recipientcode_en, recipientcode_es, \n" +
            "       recipientcode_fr, recipientcode_de, regioncode_en, regioncode_es, \n" +
            "       regioncode_fr, regioncode_de, flowcode_en, flowcode_es, flowcode_fr, \n" +
            "       flowcode_de, bimulticode_en, bimulticode_es, bimulticode_fr, \n" +
            "       bimulticode_de, flowcategory_en, flowcategory_es, flowcategory_fr, \n" +
            "       flowcategory_de, financetypecode_en, financetypecode_es, financetypecode_fr, \n" +
            "       financetypecode_de, aidtypecode_en, aidtypecode_es, aidtypecode_fr, \n" +
            "       aidtypecode_de, purposecode_en, purposecode_es, purposecode_fr, \n" +
            "       purposecode_de, sectorcode_en, sectorcode_es, sectorcode_fr, \n" +
            "       sectorcode_de, channelcode_en, channelcode_es, channelcode_fr, \n" +
            "       channelcode_de, unitcode_en, unitcode_es, unitcode_fr, unitcode_de, \n" +
            "       flowamountcode_en, flowamountcode_es, flowamountcode_fr, flowamountcode_de\n" +
            "  FROM data.adam_usd_commitment where year = ?";
    Object[] params = new Object[] {2000};

    public static void main(String[] args) {
        String host="localhost";
        String port="5432";
        String database="D3S";
        String usr="postgres";
        String psw="postgres";

        new AdamMassiveQueryTest("connection", 1, new AdamMassiveQueryTestConnection(host,port,database,usr,psw,1000));
        //new AdamMassiveQueryTest("pool", 100, new AdamMassiveQueryTestPool(host,port,database,usr,psw,10));

    }

    AdamMassiveQueryTest(String baseName, int threadPoolSize, Source source) {
        this.source = source;
        //Prepare Thread pool
        Thread[] pool = new Thread[threadPoolSize];
        for (int i=0; i<threadPoolSize; i++) {
            pool[i] = new Thread(this);
            pool[i].setName(baseName+'_'+(i+1));
        }
        //Start test
        System.out.println("Start test "+baseName);
        for (int i=0; i<threadPoolSize; i++)
            pool[i].start();
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i=0; i<queryIterationCount; i++)
            try {
                Connection connection = source.getConnection();
                ResultSet result = createStatement(connection).executeQuery();
                //while (result.next())
                    //result.getObject(1);
                //connection.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
    }

    private PreparedStatement createStatement (Connection connection) throws Exception {
        PreparedStatement statement = connection.prepareStatement(query);
        for (int i=0; i<params.length; i++)
            statement.setObject(i+1, params[i]);
        return statement;
    }
}
