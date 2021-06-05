/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose.transformations;

/**
 *
 * @author Evandro
 */
public class EjaTransformation implements Transformation {
    
    @Override
    public Object executeTransformation(Object[] value) {
        
        if (value == null) return null;
        if (value[0] == null) return null;
        if (value[1] == null) return null;
        
        int FK_COD_ETAPA_ENSINO = Integer.parseInt(value[0].toString());
        int FK_COD_MOD_ENSINO = Integer.parseInt(value[1].toString());
        
        if ( (FK_COD_ETAPA_ENSINO > 43 && FK_COD_ETAPA_ENSINO < 48) ||
              FK_COD_ETAPA_ENSINO == 51 || FK_COD_ETAPA_ENSINO == 58 ||
             (FK_COD_ETAPA_ENSINO > 60 && FK_COD_ETAPA_ENSINO < 63) ||
              FK_COD_ETAPA_ENSINO == 65 || FK_COD_ETAPA_ENSINO == 67 ||
             (FK_COD_ETAPA_ENSINO > 69 && FK_COD_ETAPA_ENSINO < 74) ){
            
            if (FK_COD_MOD_ENSINO == 2 || FK_COD_MOD_ENSINO == 3){
                //return 1;
                return true;
            }
            
        }
        
        return false;
    }      
}



//~CASE WHEN (
//            CASE WHEN ("FK_COD_ETAPA_ENSINO" > 43 AND "FK_COD_ETAPA_ENSINO" < 48) OR 
//                        "FK_COD_ETAPA_ENSINO"=51 OR 
//                        "FK_COD_ETAPA_ENSINO"=58 OR 
//                        ("FK_COD_ETAPA_ENSINO">60 AND "FK_COD_ETAPA_ENSINO"<63) OR 
//                        "FK_COD_ETAPA_ENSINO"=65 OR "FK_COD_ETAPA_ENSINO"=67 OR 
//                        ("FK_COD_ETAPA_ENSINO" > 69 AND "FK_COD_ETAPA_ENSINO" < 74) 
//            THEN 1 
//            ELSE 0 END) = TRUE AND 
//            ("FK_COD_MOD_ENSINO" = 2 OR "FK_COD_MOD_ENSINO"=3) 
//            THEN 1 ELSE 0 END
