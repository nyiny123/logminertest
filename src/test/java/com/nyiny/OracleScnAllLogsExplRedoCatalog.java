package com.nyiny;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.nyiny.OracleMinerUtils.*;

/**
 * This test for mining changes having dictionary loaded into redo logs
 * Mining filter based on SCN (system change number)
 * All redo log files are added for mining explicitly
 * DBMS_LOGMNR.CONTINUOUS_MINE
 *      it is deprecated and adds slowness, hence comment it out. It should automatically add redo log files, as needed.
 */
public class OracleScnAllLogsExplRedoCatalog {

    public static void main(String[] argv) {

        init(argv);
        CallableStatement s = null;
        try (
                Connection connection = getConnection();
                PreparedStatement ps = connection.prepareStatement(queryLogmnrScnContents)
        ) {

            startScn = getCurrentScn(connection);
            CallableStatement s1 = connection
                    .prepareCall("BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;");
            s1.execute();
            s1.close();

            addAllRedoLogForMining(connection);

            for(int i = 1; i <= LOOP_NUMBER; i++) {

                long start = System.currentTimeMillis();
                long endScn = getCurrentScn(connection);
                System.out.println("\n#" + i + " start logminer (start: " + startScn + ", end: " + endScn + ")");

                s = connection.prepareCall("BEGIN sys." +
                        "dbms_logmnr.start_logmnr(" +
                        "startScn => '" + startScn + "', " +
                        "endScn => '" + endScn + "', " +
                        "OPTIONS => DBMS_LOGMNR.DICT_FROM_REDO_LOGS + " +
                        "DBMS_LOGMNR.DDL_DICT_TRACKING + " +
                        "DBMS_LOGMNR.COMMITTED_DATA_ONLY);" +
//                        "DBMS_LOGMNR.CONTINUOUS_MINE);" +
                        "END;");
                s.execute();

                ps.setLong(1, lastCommitScn);
                doMining(connection, ps);
                startScn = endScn;

                System.out.println("******\nmining data took(ms): " + (System.currentTimeMillis() - start) + "\n*********");
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeCallableStatement(s);
        }
    }
}
