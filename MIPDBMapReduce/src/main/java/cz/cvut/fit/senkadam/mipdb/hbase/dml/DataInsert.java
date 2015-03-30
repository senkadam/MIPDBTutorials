/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.cvut.fit.senkadam.mipdb.hbase.dml;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Singleton class which provides methods for communication with HBase database
 * and enables data insertions
 *
 * @author Adam Senk <senkadam@fit.cvut.cz>
 */
public class DataInsert {

    private static Configuration hbc = null;
    private static DataInsert instance = null;
    private static HTable lastTable = null;


    /**
     * Private constructor - this class is singleton
     */
    private DataInsert() {

    }

    /**
     * Static method providing access to HBaseFaceda instance
     *
     * @return single instance of HBaseFacade, there is only one instance of
     * this class during the runtime
     */
    public static DataInsert getInstance() {
        if (instance == null) {
            instance = new DataInsert();
        }

        return instance;
    }

    /**
     * This methods returns HBaseAdmin that allows to access the HBase database
     *
     * @return instance of HBaseAdmin
     * @throws ZooKeeperConnectionException - problem with ZooKeeper
     * @throws MasterNotRunningException - problem with HBase master
     */

    public HBaseAdmin getHBaseAdmin() throws ZooKeeperConnectionException, MasterNotRunningException,IOException {
        HBaseAdmin admin = null;

        try {
            admin = new HBaseAdmin(this.getHbaseConfiguration());
        } catch (MasterNotRunningException ex) {
            String msg = "HBase MasterNotRunning";
            org.apache.log4j.Logger.getLogger(this.getClass().getName()).error(msg, ex);
            throw ex;
        } catch (ZooKeeperConnectionException ex) {
            String msg = "HBase ZooKeeper Connection problem";
            org.apache.log4j.Logger.getLogger(this.getClass().getName()).error(msg, ex);
            throw ex;
        }

        return admin;
    }

    /**
     * private method for accessing table stored in HBase
     *
     * @param tableName instance of Table class that holds information about
     * requested table
     * @return instance of HBase table that represents requested table
     */
    private HTable getHBaseTable(String tableName) {
        if (lastTable != null && new String(lastTable.getTableName()).equals(tableName)) {
            return lastTable;
        }
        try {
            lastTable = new HTable(this.getHbaseConfiguration(), tableName);
            lastTable.setAutoFlush(false);
            return lastTable;
        } catch (IOException ex) {
            String msg = "HBase table";
            org.apache.log4j.Logger.getLogger(this.getClass().getName()).error(msg, ex);

            return null;
        }
    }

    /**
     * private methods providing access to HBaseConfiguration
     *
     * @return instance of HBaseConfiguration
     */
    public Configuration getHbaseConfiguration() {
        if (hbc == null) {
            hbc = HBaseConfiguration.create();

        }
        return hbc;
    }

    /**
     * method for creating and storing of new table in HBase
     *
     * @param tableName instance of Table class that holds information about
     * requested table
     * @return instance of Status that holds information about processed action
     */
    public void createTable(String tableName, List<String> columnFamilies) {
        try {
            HBaseAdmin admin = this.getHBaseAdmin();
            if (admin.tableExists(tableName)) {
                String msg = "Created table " + tableName + " elready exists";
                org.apache.log4j.Logger.getLogger(this.getClass().getName()).warn(msg);
            } else {
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                for (String family : columnFamilies) {
                    tableDesc.addFamily(new HColumnDescriptor(family));
                }
                admin.createTable(tableDesc);
                String msg = "Hbase table created: " + tableName;
                org.apache.log4j.Logger.getLogger(this.getClass().getName()).info(msg);

            }
        } catch (IOException ex) {
            String msg = "HBase while creating table: " + tableName;
            org.apache.log4j.Logger.getLogger(this.getClass().getName()).error(msg, ex);
        }

    }

    /**
     * method for deleting of table in HBase
     *
     * @param tableName
     */
    public void deleteTable(String tableName) {
        try {
            HBaseAdmin admin = this.getHBaseAdmin();
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            String msg = "HBase table deleted: " + tableName;
            org.apache.log4j.Logger.getLogger(this.getClass().getName()).info(msg);

        } catch (IOException ex) {
            String msg = "HBase while deleting table:" + tableName;
            org.apache.log4j.Logger.getLogger(this.getClass().getName()).error(msg, ex);

        }
    }

    private void closeTable(HTable t) throws IOException {
        t.close();
        lastTable = null;
    }

    public void insertPersonData() {
        LinkedList<Put> putList = new LinkedList<Put>();

        String p_properties = "p_properties";
        String[] personFamilies = {p_properties};
        this.createTable("person", Arrays.asList(personFamilies));
        String pname = "person_name";
        String sex = "sex";
        String born = "born";
        //Put object including one person
        //Quentin Tarantino
        Put p = new Put(Bytes.toBytes("1"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(pname),
                Bytes.toBytes("Quentin Tarantino"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(sex),
                Bytes.toBytes("M"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(born),
                Bytes.toBytes("4.5.1960"));
        putList.add(p);

        //Leonardo DiCaprio
        p = new Put(Bytes.toBytes("2"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(pname),
                Bytes.toBytes("Leonardo DiCaprio"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(sex),
                Bytes.toBytes("M"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(born),
                Bytes.toBytes("14.3.1974"));
        putList.add(p);

        //James Cameron
        p = new Put(Bytes.toBytes("3"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(pname),
                Bytes.toBytes("James Cameron"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(sex),
                Bytes.toBytes("M"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(born),
                Bytes.toBytes("16.9.1954"));
        putList.add(p);

        //Kate Winslet
        p = new Put(Bytes.toBytes("4"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(pname),
                Bytes.toBytes("Kate Winslet"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(sex),
                Bytes.toBytes("F"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(born),
                Bytes.toBytes("19.11.1975"));
        putList.add(p);

        //Goerge Lucas
        p = new Put(Bytes.toBytes("5"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(pname),
                Bytes.toBytes("George Lucas"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(sex),
                Bytes.toBytes("M"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(pname),
                Bytes.toBytes("14.5.1944"));
        putList.add(p);

        //Jamie Foxx
        p = new Put(Bytes.toBytes("6"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(pname),
                Bytes.toBytes("Jamie Foxx"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(sex),
                Bytes.toBytes("M"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(pname),
                Bytes.toBytes("13.12.1967"));
        putList.add(p);

        // Christoph Waltz 
        p = new Put(Bytes.toBytes("7"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(pname),
                Bytes.toBytes("Christoph Waltz"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(sex),
                Bytes.toBytes("M"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(pname),
                Bytes.toBytes("4.10.1954"));
        putList.add(p);

        //Mark Hamill
        p = new Put(Bytes.toBytes("8"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(pname),
                Bytes.toBytes("Mark Hamill"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(sex),
                Bytes.toBytes("M"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(pname),
                Bytes.toBytes("25.9.1951"));

        //Carrie Fisher
        p = new Put(Bytes.toBytes("9"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(pname),
                Bytes.toBytes("Leonardo DiCaprio"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(sex),
                Bytes.toBytes("F"));
        p.add(Bytes.toBytes(p_properties),
                Bytes.toBytes(pname),
                Bytes.toBytes("21.10.1956"));
        putList.add(p);

        try {
            this.getHBaseTable("person").put(putList);
            this.getHBaseTable("person").flushCommits();
            this.closeTable(this.getHBaseTable("person"));
        } catch (IOException ex) {
            String msg = "Problem while inserting into person table";
            org.apache.log4j.Logger.getLogger(this.getClass().getName()).error(msg, ex);
        }

    }
    
    public void insertTitanicMovie() {

        String m_properties = "m_properties";
        String actors = "actors";
        String directors = "directors";
        String[] personFamilies = {m_properties, actors, directors};
        this.createTable("movie", Arrays.asList(personFamilies));

        //Puting Titanic attributes
        Put p = new Put(Bytes.toBytes("1m"));
        p.add(Bytes.toBytes(m_properties),
                Bytes.toBytes("m_name"),
                Bytes.toBytes("Titanic"));
        p.add(Bytes.toBytes(m_properties),
                Bytes.toBytes("runtime"),
                Bytes.toBytes("194"));
        p.add(Bytes.toBytes(m_properties),
                Bytes.toBytes("genre"),
                Bytes.toBytes("drama"));
        p.add(Bytes.toBytes(m_properties),
                Bytes.toBytes("first_night"),
                Bytes.toBytes("19.12.1997"));

        //Leonardo DiCaprio as actor playing Jack Dawson
        p.add(Bytes.toBytes(actors),
                Bytes.toBytes("Jack Dawson"),
                Bytes.toBytes("2"));
        // Kate Winslet as Rose DeWitt Bukater 
        p.add(Bytes.toBytes(actors),
                Bytes.toBytes("Rose DeWitt Bukater "),
                Bytes.toBytes("4"));

        //James Cameron director of the movie
        p.add(Bytes.toBytes(directors),
                Bytes.toBytes("3"),
                Bytes.toBytes("James Cameron"));
        
        try {
            this.getHBaseTable("movie").put(p);
            this.getHBaseTable("movie").flushCommits();
            this.closeTable(this.getHBaseTable("movie"));
        } catch (IOException ex) {
            String msg = "Problem while inserting TITANIC into movie table";
            org.apache.log4j.Logger.getLogger(this.getClass().getName()).error(msg, ex);
        }

    }

    public void insertDjangoMovie() {
        String m_properties = "m_properties";
        String actors = "actors";
        String directors = "directors";
        String[] personFamilies = {m_properties, actors, directors};
        this.createTable("movie", Arrays.asList(personFamilies));

        //Puting Titanic attributes
        Put p = new Put(Bytes.toBytes("2m"));
        p.add(Bytes.toBytes(m_properties),
                Bytes.toBytes("m_name"),
                Bytes.toBytes("Django Unchained"));
        p.add(Bytes.toBytes(m_properties),
                Bytes.toBytes("runtime"),
                Bytes.toBytes("165"));
        p.add(Bytes.toBytes(m_properties),
                Bytes.toBytes("genre"),
                Bytes.toBytes("western"));
        p.add(Bytes.toBytes(m_properties),
                Bytes.toBytes("first_night"),
                Bytes.toBytes("25.12.2012"));

        //Jamie Foxx as  Dajngo Freeman
        p.add(Bytes.toBytes(actors),
                Bytes.toBytes("Django"),
                Bytes.toBytes("6"));
        // Christopher Waltz as  Dr. King Schultz
        p.add(Bytes.toBytes(actors),
                Bytes.toBytes("Dr. King Schultz"),
                Bytes.toBytes("7"));
        //Leonardo DiCaprio as Calvin Candie
        p.add(Bytes.toBytes(actors),
                Bytes.toBytes("Calvin Candie"),
                Bytes.toBytes("2"));
        //Quentin Tarantino as comparsist I
        p.add(Bytes.toBytes(actors),
                Bytes.toBytes("Comparsist I"),
                Bytes.toBytes("1"));

        //Quentin Tarantino
        p.add(Bytes.toBytes(directors),
                Bytes.toBytes("1"),
                Bytes.toBytes("Quentin Tarantino"));
        
        try {
            this.getHBaseTable("movie").put(p);
            this.getHBaseTable("movie").flushCommits();
            this.closeTable(this.getHBaseTable("movie"));
        } catch (IOException ex) {
            String msg = "Problem while inserting DJANGO UNCHAINED into movie table";
            org.apache.log4j.Logger.getLogger(this.getClass().getName()).error(msg, ex);
        }

    }

    public void insertStarWarsIVMovie() {
        
        String m_properties = "m_properties";
        String actors = "actors";
        String directors = "directors";
        String[] personFamilies = {m_properties, actors, directors};
        this.createTable("movie", Arrays.asList(personFamilies));

        //Puting Titanic attributes
        Put p = new Put(Bytes.toBytes("3m"));
        p.add(Bytes.toBytes(m_properties),
                Bytes.toBytes("m_name"),
                Bytes.toBytes("Star Wars - A New Hope"));
        p.add(Bytes.toBytes(m_properties),
                Bytes.toBytes("runtime"),
                Bytes.toBytes("121"));
        p.add(Bytes.toBytes(m_properties),
                Bytes.toBytes("genre"),
                Bytes.toBytes("sci-fi"));
        p.add(Bytes.toBytes(m_properties),
                Bytes.toBytes("first_night"),
                Bytes.toBytes("25.5.1977"));

        //Leonardo DiCaprio as Luke Skywalker
        p.add(Bytes.toBytes(actors),
                Bytes.toBytes("Luke Skywalker"),
                Bytes.toBytes("8"));
        // Carrie Fisher as Princess Leia 
        p.add(Bytes.toBytes(actors),
                Bytes.toBytes("Leia Organa"),
                Bytes.toBytes("9"));
        
        //Quentin Tarantino
        p.add(Bytes.toBytes(directors),
                Bytes.toBytes("5"),
                Bytes.toBytes("George Lucas"));
        
        try {
            this.getHBaseTable("movie").put(p);
            this.getHBaseTable("movie").flushCommits();
            this.closeTable(this.getHBaseTable("movie"));
        } catch (IOException ex) {
            String msg = "Problem while inserting STAR WARS into movie table";
            org.apache.log4j.Logger.getLogger(this.getClass().getName()).error(msg, ex);
        }

    }

}
