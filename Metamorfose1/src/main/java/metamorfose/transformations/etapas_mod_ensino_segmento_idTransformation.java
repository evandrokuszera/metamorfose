/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose.transformations;

import java.util.ArrayList;

/**
 *
 * @author Evandro
 */
public class etapas_mod_ensino_segmento_idTransformation implements Transformation {

    @Override
    public Object executeTransformation(Object[] value) {
        if (value == null) return null;
        if (value[0] == null) return null;
        
        int FK_COD_ETAPA_ENSINO = Integer.parseInt(value[0].toString());
        
        if (FK_COD_ETAPA_ENSINO == 1) return 1;
        if (FK_COD_ETAPA_ENSINO == 2) return 2;
        if (FK_COD_ETAPA_ENSINO == 3) return 3;
        if ( (FK_COD_ETAPA_ENSINO > 3 && FK_COD_ETAPA_ENSINO < 8) || (FK_COD_ETAPA_ENSINO > 13 && FK_COD_ETAPA_ENSINO < 19)) return 4;
        if ( (FK_COD_ETAPA_ENSINO > 7 && FK_COD_ETAPA_ENSINO < 12) || (FK_COD_ETAPA_ENSINO > 18 && FK_COD_ETAPA_ENSINO < 22) || FK_COD_ETAPA_ENSINO == 41) return 5;
        if ( (FK_COD_ETAPA_ENSINO > 24 && FK_COD_ETAPA_ENSINO < 39) ) return 6;
        if (FK_COD_ETAPA_ENSINO == 12 || FK_COD_ETAPA_ENSINO == 13 || (FK_COD_ETAPA_ENSINO > 21 && FK_COD_ETAPA_ENSINO < 25) || FK_COD_ETAPA_ENSINO == 56 ) return 7;
        
        int op8[] = {43,44,46,47,49,50,51,53,54,58,59,60,61,65,69,70,72,73};
        if (findElementInArray(op8, FK_COD_ETAPA_ENSINO)) return 8;
        
        int op9[] = {45, 48, 52, 55, 57, 62, 63, 67, 71, 74};
        if (findElementInArray(op9, FK_COD_ETAPA_ENSINO)) return 9;
        
        
        int op10[] = {39, 40, 64, 68};
        if (findElementInArray(op10, FK_COD_ETAPA_ENSINO)) return 10;

        
        return 0; // não sei qual seria o valor default       
    }
    
    private boolean findElementInArray(int[] array, int element){
        for (int i=0; i<array.length; i++){
            if (array[i]== element){
                return true;
            }
        }
        return false;
    }
    
}


// ~CASE WHEN ("FK_COD_ETAPA_ENSINO"= 1) THEN 1 
//       WHEN ("FK_COD_ETAPA_ENSINO"= 2) THEN 2 
//       WHEN ("FK_COD_ETAPA_ENSINO"= 3) THEN 3 
//       WHEN ("FK_COD_ETAPA_ENSINO"= 4 OR  "FK_COD_ETAPA_ENSINO"= 5 OR  "FK_COD_ETAPA_ENSINO"= 6 OR  "FK_COD_ETAPA_ENSINO"= 7 OR  "FK_COD_ETAPA_ENSINO"= 14 OR  "FK_COD_ETAPA_ENSINO"= 15 OR  "FK_COD_ETAPA_ENSINO"= 16 OR  "FK_COD_ETAPA_ENSINO"= 17 OR  "FK_COD_ETAPA_ENSINO"= 18) THEN 4 
//       WHEN ("FK_COD_ETAPA_ENSINO"= 8 OR  "FK_COD_ETAPA_ENSINO"= 9 OR  "FK_COD_ETAPA_ENSINO"= 10 OR  "FK_COD_ETAPA_ENSINO"= 11 OR  "FK_COD_ETAPA_ENSINO"= 19 OR  "FK_COD_ETAPA_ENSINO"= 20 OR  "FK_COD_ETAPA_ENSINO"= 21 OR  "FK_COD_ETAPA_ENSINO"= 41) THEN 5 
//       WHEN ("FK_COD_ETAPA_ENSINO"= 25 OR  "FK_COD_ETAPA_ENSINO"= 26 OR  "FK_COD_ETAPA_ENSINO"= 27 OR  "FK_COD_ETAPA_ENSINO"= 28 OR  "FK_COD_ETAPA_ENSINO"= 29 OR "FK_COD_ETAPA_ENSINO"= 30 OR  "FK_COD_ETAPA_ENSINO"= 31 OR  "FK_COD_ETAPA_ENSINO"= 32 OR "FK_COD_ETAPA_ENSINO"= 33 OR  "FK_COD_ETAPA_ENSINO"= 34 OR  "FK_COD_ETAPA_ENSINO"= 35 OR  "FK_COD_ETAPA_ENSINO"= 36 OR  "FK_COD_ETAPA_ENSINO"= 37 OR "FK_COD_ETAPA_ENSINO"= 38) THEN 6 
//       WHEN ("FK_COD_ETAPA_ENSINO"= 12 OR  "FK_COD_ETAPA_ENSINO"= 13 OR  "FK_COD_ETAPA_ENSINO"= 22 OR  "FK_COD_ETAPA_ENSINO"= 23 OR  "FK_COD_ETAPA_ENSINO"= 24 OR "FK_COD_ETAPA_ENSINO"= 56)  THEN 7 
//       WHEN ("FK_COD_ETAPA_ENSINO"= 43 OR  "FK_COD_ETAPA_ENSINO"= 44 OR  "FK_COD_ETAPA_ENSINO"= 46 OR  "FK_COD_ETAPA_ENSINO"= 47 OR  "FK_COD_ETAPA_ENSINO"= 49 OR  "FK_COD_ETAPA_ENSINO"= 50 OR  "FK_COD_ETAPA_ENSINO"= 51 OR "FK_COD_ETAPA_ENSINO"= 53 OR "FK_COD_ETAPA_ENSINO"= 54 OR "FK_COD_ETAPA_ENSINO"= 58 OR  "FK_COD_ETAPA_ENSINO"= 59 OR  "FK_COD_ETAPA_ENSINO"= 60 OR  "FK_COD_ETAPA_ENSINO"= 61 OR  "FK_COD_ETAPA_ENSINO"= 65 OR  "FK_COD_ETAPA_ENSINO"= 69 OR  "FK_COD_ETAPA_ENSINO"= 70 OR  "FK_COD_ETAPA_ENSINO"= 72 OR  "FK_COD_ETAPA_ENSINO"= 73) THEN 8 
//       WHEN ("FK_COD_ETAPA_ENSINO"= 45 OR "FK_COD_ETAPA_ENSINO"= 48 OR "FK_COD_ETAPA_ENSINO"= 52 OR "FK_COD_ETAPA_ENSINO"= 55 OR "FK_COD_ETAPA_ENSINO"= 57 OR "FK_COD_ETAPA_ENSINO"= 62 OR "FK_COD_ETAPA_ENSINO"= 63 OR   "FK_COD_ETAPA_ENSINO"= 67 OR "FK_COD_ETAPA_ENSINO"= 71 OR  "FK_COD_ETAPA_ENSINO"= 74) THEN 9 
//       WHEN ("FK_COD_ETAPA_ENSINO"= 39 OR "FK_COD_ETAPA_ENSINO"= 40 OR "FK_COD_ETAPA_ENSINO"= 64 OR "FK_COD_ETAPA_ENSINO"= 68) THEN 10 END
