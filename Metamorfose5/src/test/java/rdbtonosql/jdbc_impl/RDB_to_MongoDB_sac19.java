/*
    This class executes the transformation from RDB (ds2_10mb) to MongoDB, using the schema specification 'cp-sac19.json'.
    The input RDB data can be transformed into Document or Column Family models.
    The output data is persisted in a locally MongoDB server.
    This experiment was presented int SAC'19 Conference.

    WARNING.: in this class we are using the JDBC implementation! 
          That implementation have some performance problems to joning entities (TODO).
INPUT:
    ConverstionProcess: cp-sac19.json
    RDB(input data): ds2_10mb, ds2_50mb, ds2_100mb (we provided the Postgres database backup file in resource folder).
    Target NoSQL Model: doc or col.
    Command Executor Type: spark or JDBC implementation (later is experimental).
    NoSQL(output data): MongoDB server.

OUTPUT:
    A MongoDB database called sac19, with three collections of documents: Orders, Orderlines and Products.
    Each collection represent a different way to transform RDB entities to NoSQL documents.
    You can use the QBMetrics to inspect the cp-sac19.json file and see the NoSQL schemas before executing the migration. 
 */
package rdbtonosql.jdbc_impl;

import java.io.File;
import java.net.URISyntaxException;
import metamorfose5.rdbtonosql.command.run.NoSQLTargetModelEnum;
import metamorfose5.rdbtonosql.command.run.RunProcess;

/**
 *
 * @author evand
 */
public class RDB_to_MongoDB_sac19 {
    public static void main(String[] args) {
        RunProcess.nosql_target_model = NoSQLTargetModelEnum.DOC; // doc or col
        RunProcess.command_executor_type = "JDBC";  // spark or JDBC
        // source
        RunProcess.rdb_server = "localhost";
        RunProcess.rdb_user = "postgres";
        RunProcess.rdb_passwd = "123456";
        RunProcess.rdb_database = "ds2_10mb";
        // target
        RunProcess.mongoserver = "localhost";
        // start the conversion...
        try {
            File f = new File(RDB_to_MongoDB_sac19.class.getResource("/sac19/cp-sac19.json").toURI());
            RunProcess.run(f.getAbsolutePath());
        } catch (URISyntaxException ex) {
            System.out.println(ex);
        }
    }
}
