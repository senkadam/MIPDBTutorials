/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cz.cvut.fit.senkadam.mipdb.hbase.main;

import cz.cvut.fit.senkadam.mipdb.hbase.dml.DataInsert;

/**
 *
 * @author Adam Senk <senkadam@fit.cvut.cz>
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        DataInsert.getInstance().insertPersonData();
        DataInsert.getInstance().insertTitanicMovie();
        DataInsert.getInstance().insertDjangoMovie();
        DataInsert.getInstance().insertStarWarsIVMovie();
    }
    
}
