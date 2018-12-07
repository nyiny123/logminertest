package com.nyiny;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.nyiny.OracleMinerUtils.*;

/**
 * This test for mining changes having dictionary loaded into redo logs
 * Mining filter based on time boundaries
 * Redo log files are added for mining implicitly (DBMS_LOGMNR.CONTINUOUS_MINE)
 */
// sometimes throws error : ORA-00600: internal error code, arguments: [krvrdgpm_getpdbmap
public class OracleTimeAllLogsImplRedoCatalog {

    public static void main(String[] argv) {

        init(argv);
        CallableStatement s = null;
        try (
                Connection connection = getConnection();
                PreparedStatement ps = connection.prepareStatement(queryLogmnrTsContents)
        ) {

            String alterSession  = "ALTER SESSION SET NLS_DATE_FORMAT = 'dd-mon-yyyy hh24:mi:ss'";
            s = connection.prepareCall(alterSession);
            s.execute();

            s = connection.prepareCall("BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;");
            s.execute();

            for(int i = 1; i <= LOOP_NUMBER; i++) {
                long start = System.currentTimeMillis();

                LocalDateTime startTime = LocalDateTime.now().plusSeconds((long) -10);
                LocalDateTime endTime = LocalDateTime.now().plusSeconds((long) 10);
                String startTimeString = DateTimeFormatter.ofPattern( "dd-MMM-yyyy HH:mm:ss" ).format(startTime);
                String endTimeString = DateTimeFormatter.ofPattern( "dd-MMM-yyyy HH:mm:ss" ).format(endTime);
                System.out.println("start time: " + startTime + " end time: " + endTime);

                s = connection.prepareCall("BEGIN sys." +
                        "dbms_logmnr.start_logmnr(" +
                        "STARTTIME => '" + startTimeString + "', " +
                        "ENDTIME => '" + endTimeString + "', " +
                        "OPTIONS => DBMS_LOGMNR.DICT_FROM_REDO_LOGS + " +
                        "DBMS_LOGMNR.DDL_DICT_TRACKING + " +
                        "DBMS_LOGMNR.COMMITTED_DATA_ONLY +" +
                        "DBMS_LOGMNR.CONTINUOUS_MINE);" +
                        "END;");
                s.execute();

                ps.setString(1, startTimeString);
                doMining(connection, ps);

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
