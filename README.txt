This is just a fork from the


The project contains some cloned code from https://github.com/gunnarmorling/debezium-incubator/tree/logminer_test,
and refactored it.
Basically I extracted common cases into a utility class.

The main purpose of this project is to exercise performance of different LogMiner configurations.
Original branch code takes > 10 seconds to get a change from the log.

Some tests do not capture DDL, mostly I was focused on DML for now.
It is possible to either add redo log files or set continuous mining (allow DDL capturing)
No PDB, no RAC yet, just a simple configuration to compare performance

The way to test is:
 - make sure ojdbc8.jar is in the class path
 - start one of classes
 - wait "start logminer" message
 - do some DML, make sure it is captured

To run:
java -classpath "<PATH>\ojdbc8.jar;<PATH TO CLASS DIR>" com.nyiny.OracleScnAllLogsExplOnlineCatalog


This table contains measurements on following configurations:

| N | Class Name        		         | Filter by  | Log File(s)          | Data Dictionary in | Latency (DML)                    |
|------------------------------------------------------------------------------------------------------------------------------------|
| 1 | OracleScnAllLogsExplOnlineCatalog  | SCN        | All explicitly       | online catalog     |  0.15 sec                        |
| 2 | OracleScnAllLogsExplRedoCatalog    | SCN        | All explicitly       | redo log           |  takes time initially, then 0.15 |
| 3 | OracleScnAllLogsImplFlatFile       | SCN        | All implicitly       | flat file          |  takes time initially, then 0.4  |
| 4 | OracleScnCurrentLogOnlineCatalog   | SCN        | One current redo log | online catalog     |  0.18 sec                        |
| 5 | OracleTimeAllLogsExplOnlineCatalog | Timestamp  | All explicitly       | online catalog     |  4 sec                           |
| 6 | OracleTimeAllLogsImplRedoCatalog   | Timestamp  | All implicitly       | redo log           |  >20 sec, throws errors          |

