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
public class EnsinoRegularTransformation implements Transformation {

    @Override
    public Object executeTransformation(Object[] value) {
        
        if (value == null) return null;
        if (value[0] == null) return null;
        if (value[1] == null) return null;
                
        int FK_COD_ETAPA_ENSINO = Integer.parseInt(value[0].toString());
        int FK_COD_MOD_ENSINO = Integer.parseInt(value[1].toString());
        
        if ( (FK_COD_ETAPA_ENSINO > 1 && FK_COD_ETAPA_ENSINO < 38) || FK_COD_ETAPA_ENSINO == 41 || FK_COD_ETAPA_ENSINO == 56 ){
            if (FK_COD_MOD_ENSINO == 1 || FK_COD_MOD_ENSINO == 2){
                return true;            
            }
        }
        return false;      
    }
    
}


//    ~CASE 
//            WHEN "FK_COD_ETAPA_ENSINO" IS NULL THEN NULL 
//            WHEN (
//                    CASE 
//                            WHEN ("FK_COD_ETAPA_ENSINO">1 AND "FK_COD_ETAPA_ENSINO"<38) OR "FK_COD_ETAPA_ENSINO"=41 OR "FK_COD_ETAPA_ENSINO"=56 
//                                THEN 1 
//                                ELSE 0 
//                            END)=TRUE AND ("FK_COD_MOD_ENSINO"=1 OR "FK_COD_MOD_ENSINO"=2) THEN 1 ELSE 0 END