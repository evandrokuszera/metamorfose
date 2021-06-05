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
public class ProfissionalizanteTransformation implements Transformation {
    
    @Override
    public Object executeTransformation(Object[] value) {
        if (value == null) return null;
        if (value[0] == null) return null;
        if (value[1] == null) return null;
        
        int FK_COD_ETAPA_ENSINO = Integer.parseInt(value[0].toString());
        int FK_COD_MOD_ENSINO = Integer.parseInt(value[1].toString());
        
        
        if (FK_COD_MOD_ENSINO == 1 || FK_COD_MOD_ENSINO == 2 || FK_COD_MOD_ENSINO ==3){
            
            if ( (FK_COD_ETAPA_ENSINO > 30 && FK_COD_ETAPA_ENSINO < 30) ||   // ???????????????????????????????????????????? apenas copiei do arquivo de mapeamento.
                 (FK_COD_ETAPA_ENSINO > 60 && FK_COD_ETAPA_ENSINO < 65) ||
                  FK_COD_ETAPA_ENSINO == 67 || FK_COD_ETAPA_ENSINO == 68 || FK_COD_ETAPA_ENSINO == 73 || FK_COD_ETAPA_ENSINO == 74  ){
                
                return true;
                
            }            
        }
        return false;        
    }    
}

// ~CASE WHEN ("FK_COD_MOD_ENSINO" = 1 OR "FK_COD_MOD_ENSINO" = 2 OR "FK_COD_MOD_ENSINO" = 3) AND
//               (("FK_COD_ETAPA_ENSINO">30 AND "FK_COD_ETAPA_ENSINO" < 30 ) OR
//                     ("FK_COD_ETAPA_ENSINO" > 60 AND "FK_COD_ETAPA_ENSINO" < 65) OR
//                     ("FK_COD_ETAPA_ENSINO" = 67 OR "FK_COD_ETAPA_ENSINO" = 68) OR ("FK_COD_ETAPA_ENSINO"=73 OR "FK_COD_ETAPA_ENSINO"=74)) THEN 1 ELSE 0 END
