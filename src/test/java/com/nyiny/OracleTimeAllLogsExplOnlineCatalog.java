package com.nyiny;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.nyiny.OracleMinerUtils.*;

/**
 * This test for mining changes having dictionary in the online catalog (No dictionary DDL get tracked)
 * Mining filter based on time boundaries
 * All redo log files are added for mining explicitly
 */
public class OracleTimeAllLogsExplOnlineCatalog {

    private static final String DATE_TIME_PATTERN = "dd-MMM-yyyy HH:mm:ss";

    public static void main(String[] argv) {

        init(argv);
        CallableStatement s = null;
        try (
                Connection connection = getConnection();
                PreparedStatement ps = connection.prepareStatement(queryLogmnrTsContents)
        ) {

            addAllRedoLogForMining(connection);

            String alterSession  = "ALTER SESSION SET NLS_DATE_FORMAT = 'dd-mon-yyyy hh24:mi:ss'";
            s = connection.prepareCall(alterSession);
            s.execute();

            for(int i = 1; i <= LOOP_NUMBER; i++) {
                long start = System.currentTimeMillis();

                LocalDateTime startTime = LocalDateTime.now().plusSeconds((long) -10);// todo play with time boundaries
                LocalDateTime endTime = LocalDateTime.now().plusSeconds((long) 10);
                String startTimeString = DateTimeFormatter.ofPattern( DATE_TIME_PATTERN ).format(startTime);
                String endTimeString = DateTimeFormatter.ofPattern(DATE_TIME_PATTERN).format(endTime);
                System.out.println("start time: " + startTime + " end time: " + endTime);

                s = connection.prepareCall("BEGIN sys." +
                        "dbms_logmnr.start_logmnr(" +
                        "STARTTIME => '" + startTimeString + "', " +
                        "ENDTIME => '" + endTimeString + "', " +
                        "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + " +
//this one could not be set                        "DBMS_LOGMNR.DDL_DICT_TRACKING + " +
                        "DBMS_LOGMNR.COMMITTED_DATA_ONLY );" +
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
