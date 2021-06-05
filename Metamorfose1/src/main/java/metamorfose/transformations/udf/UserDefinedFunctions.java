/*
 * Definição de UDFs para definir transformações de dados.
 */

 /*
 * Classe para testes.
 * Usada pela classe ExUsingUDFtoTransformation para avaliar formas de chamar UDFs no Spark.
 */
package metamorfose.transformations.udf;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.api.java.UDF1;

/**
 *
 * @author Evandro
 */
public class UserDefinedFunctions {

    private static Map<String, UDF1> udfs = new HashMap<>();

    public static void register_udfs() {
        udfs.put("sexoUDF", sexoTransformation);
        udfs.put("convenioPoderPublicoTransformationUDF", convenioPoderPublicoTransformation);
        udfs.put("mod_ensinoUDF", mod_ensinoTransformation);
    }

    public static UDF1 getUDFbyName(String udfName) {
        return udfs.get(udfName);
    }

    // FUNÇÕES DEFINIDAS PELO USUÁRIO
    public static UDF1<String, Integer> sexoTransformation = (arg0) -> {
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
    };

    public static UDF1<String, Integer> convenioPoderPublicoTransformation = (arg0) -> {
        int result = 0;
        
        if (arg0 == null)
            return null;        
        
        int arg_as_int = Integer.parseInt(arg0.toString());
        
        switch (arg_as_int) {
            case 1:
                result = 2;
                break;
            case 2:
                result = 1;
                break;
            case 3:
                result = 3;
                break;
        }

        return result;
    };
    
    public static UDF1<String, Integer> mod_ensinoTransformation = (arg0) -> {
        int result = 0;
        
        if (arg0 == null)
            return null;        
        
        int arg_as_int = Integer.parseInt(arg0.toString());
        
        switch (arg_as_int) {
            case 1:
                result = 0;
                break;
            case 2:
                result = 1;
                break;
            case 3:
                result = 0;
                break;
        }

        return result;
    };
    
}
