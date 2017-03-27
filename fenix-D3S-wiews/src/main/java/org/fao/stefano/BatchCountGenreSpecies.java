package org.fao.stefano;

import java.sql.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

public class BatchCountGenreSpecies {

    public static void main(String[] args) throws SQLException {
        Collection<Integer> genreCount = new LinkedList<>();
        Collection<Integer> speciesCount = new LinkedList<>();

        Connection connection = DriverManager.getConnection("jdbc:postgresql://faostat3.fao.org:5432/wiews", "fenix", "Qwaszx");
        PreparedStatement statementGenre = connection.prepareStatement("select count(*) from ( select genre from ref_species WHERE \"year\"<=? OR \"year\" IS NULL GROUP BY genre) genres");
        PreparedStatement statementSpecie = connection.prepareStatement("select count(*) from ( select genre, specie from ref_species WHERE \"year\"<=? OR \"year\" IS NULL GROUP BY genre, specie) species");

        ResultSet resultSet = connection.createStatement().executeQuery("select max (year), min (year) from ref_species");
        resultSet.next();
        int fromY=resultSet.getInt(1), toY=resultSet.getInt(2);

        for (int y=fromY; y>=toY; y--) {
            statementGenre.setInt(1, y);
            resultSet = statementGenre.executeQuery();
            if (resultSet.next())
                genreCount.add(resultSet.getInt(1));

            statementSpecie.setInt(1, y);
            resultSet = statementSpecie.executeQuery();
            if (resultSet.next())
                speciesCount.add(resultSet.getInt(1));
        }

        connection.close();


        Iterator<Integer> genreCountIterator = genreCount.iterator();
        Iterator<Integer> speciesCountIterator = speciesCount.iterator();
        System.out.println("year\tgenus\tspecies");
        for (int y=fromY; y>=toY; y--)
            System.out.println(y+"\t"+genreCountIterator.next()+'\t'+speciesCountIterator.next());
    }
}
