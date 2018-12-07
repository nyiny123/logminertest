package com.nyiny;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.nyiny.OracleMinerUtils.*;

/**
 * This test for mining changes having dictionary in the online catalog (No dictionary DDL get tracked)
 * Mining filter based on SCN (system change number)
 * All redo log files are added for mining explicitly
 */
public class OracleScnAllLogsExplOnlineCatalog {

    public static void main(String[] argv) {

        init(argv);
        CallableStatement s = null;
        try (
             Connection connection = getConnection();
             PreparedStatement ps = connection.prepareStatement(queryLogmnrScnContents)
        ) {

            startScn = getCurrentScn(connection);

            addAllRedoLogForMining(connection);

            for(int i = 1; i <= LOOP_NUMBER; i++) {

                long start = System.currentTimeMillis();
                long endScn = getCurrentScn(connection);
                System.out.println("\n#" + i + " start logminer (start: " + startScn + ", end: " + endScn + ")");

                s = connection.prepareCall("BEGIN sys." +
                        "dbms_logmnr.start_logmnr(" +
                        "startScn => '" + startScn + "', " +
                        "endScn => '" + endScn + "', " +
                        "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + " +
//                        "DBMS_LOGMNR.DDL_DICT_TRACKING + " +
                        "DBMS_LOGMNR.COMMITTED_DATA_ONLY );" +
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
