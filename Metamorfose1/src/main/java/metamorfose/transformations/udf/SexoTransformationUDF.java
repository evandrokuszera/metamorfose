/*
 * Classe para testes.
 * Usada pela classe ExUsingUDFtoTransformation para avaliar formas de chamar UDFs no Spark.
 */
package metamorfose.transformations.udf;

import org.apache.spark.sql.api.java.UDF1;

/**
 *
 * @author Evandro
 */
public class SexoTransformationUDF implements UDF1<String, Integer> {

    @Override
    public Integer call(String arg0) throws Exception {
        int result = 0;
            switch (arg0) {
                case "M":
                    result = 1;
                    break;
                case "F":
                    result = 0;
                    break;
            }

            return result;
    }
    
}
