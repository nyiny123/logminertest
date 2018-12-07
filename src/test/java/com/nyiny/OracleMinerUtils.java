/*
 * Copyright (c) 2018 Navis LLC. All Rights Reserved.
 *
 */
package com.nyiny;

import com.sun.istack.internal.NotNull;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @link https://docs.oracle.com/en/database/oracle/oracle-database/12.2/refrn/V-LOGMNR_CONTENTS.html
 *
 * setting up:
 * sqlplus / as sysdba
 * SQL>shutdown immediate
 * SQL>startup mount
 * SQL>alter database archivelog;
 * SQL>alter database open;
 * SQL>alter database add supplemental log data (all) columns;
 *
 * create role logmnr_role;
 * grant create session to logmnr_role;
 * grant  execute_catalog_role,select any transaction ,select any dictionary to logmnr_role;
 * create user miner identified by minerpass;
 * grant  logmnr_role to miner;
 * alter user miner quota unlimited on users;
 *
 * add additional privilege must be granted before running OracleScnAllLogsImplFlatFile
 * SQL> grant read, write on directory DATA_PUMP_DIR to LOGMNR_ROLE; (use this existing dir)
 *
 */
class OracleMinerUtils {

    static long lastCommitScn = -1;
    static Long startScn;
    static String queryLogmnrScnContents;
    static String queryLogmnrTsContents;
    final static int LOOP_NUMBER = 1000;

    private static String sid;
    private static String schemaName;
    private static String pass;
    private static List<String> logFiles = new ArrayList<>();
    private static long lastScn = -1;

    static void init(@NotNull String[] argv){

        String fileName = (argv.length > 0) ? argv[0] : null;
        AppProperties props = new AppProperties(fileName);

        schemaName = props.getProperty("logminer.schema.name");
        pass = props.getProperty("logminer.user.pass");
        sid = props.getProperty("logminer.sid");
        String files = props.getProperty("logminer.redolog.files");
        logFiles = Arrays.asList(files.split(";"));

        // SCN - The SCN at which a change was made
        //COMMIT_SCN - The SCN at which a change was committed
        queryLogmnrScnContents =
                "SELECT scn, commit_scn, username, operation, seg_owner, src_con_name, sql_redo " +
                        "FROM v$logmnr_contents " +
                        "WHERE " +
                        "username = '"+ schemaName +"' " +
                        " AND OPERATION_CODE in (1,2,3) " +
                        "AND seg_owner = '"+ schemaName +"' " +
                        "AND (commit_scn > ? )";
//                    " OR scn > ?)";

        // COMMIT_TIMESTAMP - Timestamp when the transaction committed; only meaningful
        //if the COMMITTED_DATA_ONLY option was chosen in a DBMS_LOGMNR.START_LOGMNR() invocation
        queryLogmnrTsContents =
                "SELECT scn, commit_scn, username, operation, seg_owner, src_con_name, sql_redo " +
                        "FROM v$logmnr_contents " +
                        "WHERE " +
                        "username = '"+ schemaName +"' " +
                        " AND OPERATION_CODE in (1,2,3) " +
                        "AND seg_owner = '"+ schemaName +"' " +
                        "AND (COMMIT_TIMESTAMP > ? )";

    }

    static void doMining(Connection connection, PreparedStatement ps) throws SQLException {

//        ps.setLong(2, lastScn);
        ResultSet res = ps.executeQuery();

        while(res.next()) {

            long scn = res.getLong(1);
            long commitScn = res.getLong(2);
            String operation = res.getString(4);
            String seqOwner = res.getString(5);
            String conName = res.getString(6);
            String sql = res.getString(7);

            // DDL
            if (commitScn == 0 && scn > lastScn) {
                System.out.println("DDL " + scn + " - " + operation + " - " + sql + "\n, seq owner = " + seqOwner + ", conName = " + conName);
                lastScn = scn;
            }
            // DML
            else if (commitScn > lastCommitScn) {
                System.out.println("DML " + scn + " - " + operation + " - " + sql + "\n, seq owner = " + seqOwner + ", conName = " + conName);
                lastCommitScn = commitScn;
            }
        }
        res.close();
        long nextStartScn = getLogStartScn(lastCommitScn, connection);

        if (nextStartScn > -1) {
            startScn = nextStartScn;
        }
    }

    static void closeCallableStatement(CallableStatement s) {
        try {
            if (s != null) {
                s.close();
            }
        } catch (SQLException e) {
            //ignore
        }
    }

    private static long getLogStartScn(long lastCommitScn, Connection connection) throws SQLException {
        if (lastCommitScn == -1) {
            return -1;
        }

        System.out.println("getting first scn of online log");

        Statement s = connection.createStatement();
        ResultSet res = s.executeQuery("select first_change# from V$LOG where status = 'CURRENT'");
        res.next();
        long firstScnOfOnlineLog = res.getLong(1);
        res.close();

        if (firstScnOfOnlineLog <= lastCommitScn) {
            return firstScnOfOnlineLog;
        }

        System.out.println("getting first scn of archived log");

        res = s.executeQuery("select first_change# from v$archived_log order by NEXT_CHANGE# desc");
        long firstScnOfArchivedLog = -1;
        while (res.next()) {
            firstScnOfArchivedLog = res.getLong(1);
            if (firstScnOfArchivedLog <= lastCommitScn) {
                break;
            }
        }
        res.close();

        System.out.println("done getting first scn of archived log");

        return firstScnOfArchivedLog;
    }

    static long getCurrentScn(Connection connection) throws SQLException {
        Statement s = connection.createStatement();
        ResultSet res = s.executeQuery("select min(current_scn) CURRENT_SCN from gv$database");
        res.next();
        long scn = res.getLong(1);
        res.close();
        return scn;
    }

    static Connection getConnection(){
        try {
            return DriverManager.getConnection(
                    "jdbc:oracle:thin:@localhost:1521:" + sid, schemaName, pass);
        }
        catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            System.exit(1);
        }
        return null;

    }

    static void addLastRedoLogForMining(Connection connection) throws SQLException {
        String anotherOne = "SELECT VL.MEMBER FROM V$LOGFILE VL,V$LOG L WHERE L.GROUP#=VL.GROUP# AND L.STATUS='CURRENT'";
        //final String sql = "SELECT NAME FROM V$ARCHIVED_LOG WHERE FIRST_TIME = (SELECT MAX(FIRST_TIME) FROM V$ARCHIVED_LOG)";
        PreparedStatement ps = connection.prepareStatement(anotherOne);
        ResultSet rs = ps.executeQuery();
        rs.next();
        String location = rs.getString(1);
        System.out.println("mine active log:" + location);
        CallableStatement s1 = connection.prepareCall("BEGIN sys.dbms_logmnr.add_logfile('" + location +"', OPTIONS => DBMS_LOGMNR.NEW);END;");
        s1.execute();
        s1.close();
    }

    static void addAllRedoLogForMining(Connection connection) throws SQLException {
        Iterator<String> iter = logFiles.iterator();
        String firstLogFile = iter.next();
        CallableStatement s1 = connection.prepareCall("BEGIN sys." +
                "dbms_logmnr.add_logfile(" +
                "LOGFILENAME => '" + firstLogFile + "', " +
                "OPTIONS => DBMS_LOGMNR.NEW);" +
                "END;");
        s1.execute();

        while (iter.hasNext()) {
            String nextLog = iter.next();
            s1 = connection.prepareCall("BEGIN sys." +
                    "dbms_logmnr.add_logfile(" +
                    "LOGFILENAME => '" + nextLog + "', " +
                    "OPTIONS => DBMS_LOGMNR.ADDFILE);" +
                    "END;");
            s1.execute();
        }
        s1.close();

    }

}
