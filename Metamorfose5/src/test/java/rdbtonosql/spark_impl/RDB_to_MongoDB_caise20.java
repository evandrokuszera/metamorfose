/*
    This class executes the transformation from RDB (ds2_10mb) to MongoDB, using the schema specification 'demo_article_conversion_process.json'.
    The input RDB data can be transformed into Document or Column Family models.
    The output data is persisted in a locally MongoDB server.
    This experiment was presented int CAiSE'20 Forum Conference.

INPUT:
    ConversionProcess: demo_article_conversion_process.json
    RDB(input data): ds2_10mb, ds2_50mb, ds2_100mb (we provided the Postgres database backup file in resource folder).
    Target NoSQL Model: doc or col.
    Command Executor Type: spark or JDBC implementation (later is experimental).
    NoSQL(output data): MongoDB server.

OUTPUT:
    Three MongoDB databases, called SchemaA, SchemaB and SchemaC.
    Each of them have different ways to convert the RDB entities into NoSQL format.
    The RDB entities used are: Customers, Orders and Orderlines.
    You can use the QBMetrics to inspect the demo_article_conversion_process.json file and see the NoSQL schemas before execute the migration.
 */
package rdbtonosql.spark_impl;

import java.io.File;
import java.net.URISyntaxException;
import metamorfose5.rdbtonosql.command.run.NoSQLTargetModelEnum;
import metamorfose5.rdbtonosql.command.run.RunProcess;

/**
 *
 * @author evand
 */
public class RDB_to_MongoDB_caise20 {
    public static void main(String[] args) {
        RunProcess.nosql_target_model = NoSQLTargetModelEnum.DOC; // doc or col
        RunProcess.command_executor_type = "spark";  // spark or JDBC
        // source
        RunProcess.rdb_server = "localhost";
        RunProcess.rdb_user = "postgres";
        RunProcess.rdb_passwd = "123456";
        RunProcess.rdb_database = "ds2_10mb";
        // target
        RunProcess.mongoserver = "localhost";
        // start the conversion...
        try {
            File f = new File(RDB_to_MongoDB_caise20.class.getResource("/caise20/demo_article_conversion_process.json").toURI());
            RunProcess.run(f.getAbsolutePath());
        } catch (URISyntaxException ex) {
            System.out.println(ex);
        }
    }
}
