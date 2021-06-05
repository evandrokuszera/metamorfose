/*
 * Recebe uma Entity e retorna um Schema Spark para ela.
 * Entity: objeto contendo uma coleção de campos e tipos de dados.
 * Schema: lista de campos e tipos de dados compatíveis.
 *  Obs.: esse mapeamento deve ser melhorado... fiz específico para simcaq (tipos do MonetDB encontrados na planilha de mapeamento).
 */
package metamorfose.util;

import java.util.ArrayList;
import java.util.List;
import metamorfose.model.Entity;
import metamorfose.model.Field;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author Evandro
 */
public class SparkSchemaCreator {
    
    public static StructType createSchemaFromEntity(Entity entity){
        
        List<StructField> fields = new ArrayList<>();         
        
        for (Field f : entity.getFields()){
            
            // Identificando o tipo de dados do Field e setando para o Schema do Spark
            DataType type = null;            
            switch (f.getDataType()){
                case "STRING":
                    type = DataTypes.StringType;                    
                    break;
                case "BIGINT": case "LONG":
                    type = DataTypes.LongType;                    
                    break;
                case "INTEGER": case "INT": case "TINYINT":  case "SMALLINT":
                    type = DataTypes.IntegerType;                    
                    break;
                case "BOOLEAN":
                    type = DataTypes.BooleanType;
                    break;
                case "DOUBLE": 
                    type = DataTypes.DoubleType;
                    break;
                case "FLOAT":
                    type = DataTypes.FloatType;
                    break;
                default :
                    type = DataTypes.StringType;                    
                    break;
            } 
                                    
            StructField field = DataTypes.createStructField(f.getName(), type, true);
            fields.add(field);
            
        }        
       
        // retornando o Schema criado.
        return DataTypes.createStructType(fields);
    }    
}
