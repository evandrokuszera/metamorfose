/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose.transformations.javascript;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 *
 * @author Evandro
 */
public class JavascriptTransformationEngineOld implements Serializable {

    private static ScriptEngineManager factory = null;
    private static ScriptEngine engine = null;
    private static String javascriptCodes = "";
    private static Invocable invocable = null;
    private static List<Script> scripts = null;

    public static void setScripts(List<Script> scripts) {
        JavascriptTransformationEngineOld.scripts = scripts;
        // carregando as implementações das transformações em Javascript
        javascriptCodes = "";
        for (Script script : scripts) {
            javascriptCodes += script.getFunctionImplementation() + "\n\n";
        }
    }

    public static Object executeTransformation(String methodName, Object[] value) {              

        if (factory == null) {
            factory = new ScriptEngineManager();
            engine = factory.getEngineByName("JavaScript");
            
            try {
                engine.eval(javascriptCodes);
                invocable = (Invocable) engine;
            } catch (ScriptException ex) {
                System.out.println(ex);
                System.out.println(javascriptCodes);
            }
        }

        Object result = null;

        try {

            if (value == null) {
                result = invocable.invokeFunction(methodName, null);
            } else {
                result = invocable.invokeFunction(methodName, Arrays.asList(value));
            }

        } catch (ScriptException | NoSuchMethodException ex) {
            System.out.println(ex);
        }

        return result;
    }

    
    // ######################################################################################################################
    // ######################################################################################################################
    // CHAMADA DOS TESTES.
    public static void main(String[] args) {
        // Testes
        ArrayList<Script> scripts = new ArrayList<>();
        
        //Script script = new Script("desconto", "function desconto(value) {return (value[0]/100) * value[0];};");
        Script script = new Script("down", "function down(value) {return value[0].toLowerCase();};");
        scripts.add(script);
        
        setScripts(scripts);
                
        String methodName = "down";

        Object[] parameters = new Object[1];
        parameters[0] = "PENDRIVE 10GB";

        System.out.println(executeTransformation(methodName, parameters));

    }

    private static Map<String, String> GetImplementations() {
        // simulando um pool de implementações de funções javascript
        Map<String, String> implementations = new HashMap<>();

        implementations = new HashMap();
        implementations.put("SexoTransformation", sexoTransformationImpl);
        implementations.put("TipoConvenioPPTransformation", tipoConvenioPPTransformationImpl);
        implementations.put("ExclusivaEspecialTransformation", exclusivaEspecialTransformationImpl);
        implementations.put("EnsinoRegularTransformation", ensinoRegularTransformationImpl);
        implementations.put("EjaTransformation", EjaTransformationImpl);
        implementations.put("ProfissionalizanteTransformation", ProfissionalizanteTransformationImpl);
        implementations.put("Etapas_mod_ensino_segmento_idTransformation", etapas_mod_ensino_segmento_idTransformationImpl);

        return implementations;

    }

    // TESTES ENVOLVENDO VETORES
    private static String testeArray2
            = "function teste(arg) {\n"
            + "	if (arg == null) return;\n"
            + "	print(arg);\n"
            + "	print(arg[0]);\n"
            + "	print(arg[1]);	\n"
            + "};";

    private static String testeArray1
            = "function teste1(arg) {\n"
            + "	for (var i=0; i<arg.length; i++) {\n"
            + "		print(arg[i]);\n"
            + "	}\n"
            + "};";

    // FUNÇÕES DEFINIDAS PELO USUÁRIO (JAVASCRIPT) - ESCOPO PROJETO SIMCAQ
    private static String sexoTransformationImpl = "function SexoTransformation(arg) {  "
            + "	if (arg[0] == 'F')                 "
            + "         return 1;                   "
            + "     else if (arg == 'M')            "
            + "         return 2;                   "
            + "};                                    ";

    private static String tipoConvenioPPTransformationImpl
            = "function TipoConvenioPPTransformation(arg) {"
            + "if (arg == null) return null;"
            + "if (arg[0] == null) return null;"
            + "result = 0;"
            + "switch (arg[0]) {"
            + "	case 1:"
            + "		result = 2;"
            + "		break;"
            + "	case 2:"
            + "		result = 1;"
            + "		break;"
            + "	case 3:"
            + "		result = 3;"
            + "		break;"
            + "}"
            + "return result;"
            + "};";

    private static String exclusivaEspecialTransformationImpl
            = "function ExclusivaEspecialTransformation(arg) {"
            + "       result = false;"
            + "       if (arg == null) return null;             "
            + "       switch (arg[0]) {                            "
            + "            case 1:                              "
            + "                result = false;                  "
            + "                break;                           "
            + "            case 2:                              "
            + "                result = true;                   "
            + "                break;                           "
            + "            case 3:                              "
            + "                result = false;                  "
            + "                break;                           "
            + "        }                                        "
            + "        return result;                           "
            + "};";

    private static String ensinoRegularTransformationImpl
            = "function EnsinoRegularTransformation(arg) {"
            + "        if (arg == null) return null;"
            + "        if (arg[0] == null) return null;"
            + "        if (arg[1] == null) return null;"
            + "        FK_COD_ETAPA_ENSINO = arg[0];"
            + "        FK_COD_MOD_ENSINO = arg[1];"
            + "        if ( (FK_COD_ETAPA_ENSINO > 1 && FK_COD_ETAPA_ENSINO < 38) || FK_COD_ETAPA_ENSINO == 41 || FK_COD_ETAPA_ENSINO == 56 ){"
            + "            if (FK_COD_MOD_ENSINO == 1 || FK_COD_MOD_ENSINO == 2){"
            + "                return true;"
            + "            }"
            + "        return false;"
            + "        }"
            + "};";

    private static String EjaTransformationImpl
            = "function EjaTransformation(arg) {\n"
            + "	if (arg == null) return null;\n"
            + "        if (arg[0] == null) return null;\n"
            + "        if (arg[1] == null) return null;\n"
            + "        \n"
            + "        FK_COD_ETAPA_ENSINO = arg[0];\n"
            + "        FK_COD_MOD_ENSINO = arg[1];\n"
            + "        \n"
            + "        if ( (FK_COD_ETAPA_ENSINO > 43 && FK_COD_ETAPA_ENSINO < 48) ||\n"
            + "              FK_COD_ETAPA_ENSINO == 51 || FK_COD_ETAPA_ENSINO == 58 ||\n"
            + "             (FK_COD_ETAPA_ENSINO > 60 && FK_COD_ETAPA_ENSINO < 63) ||\n"
            + "              FK_COD_ETAPA_ENSINO == 65 || FK_COD_ETAPA_ENSINO == 67 ||\n"
            + "             (FK_COD_ETAPA_ENSINO > 69 && FK_COD_ETAPA_ENSINO < 74) ){\n"
            + "            \n"
            + "            if (FK_COD_MOD_ENSINO == 2 || FK_COD_MOD_ENSINO == 3){                \n"
            + "                return true;\n"
            + "            }\n"
            + "            \n"
            + "        }\n"
            + "        \n"
            + "        return false;\n"
            + "};";

    private static String ProfissionalizanteTransformationImpl
            = "function ProfissionalizanteTransformation(value) {\n"
            + "	if (value == null) return null;\n"
            + "        if (value[0] == null) return null;\n"
            + "        if (value[1] == null) return null;\n"
            + "        \n"
            + "        FK_COD_ETAPA_ENSINO = value[0];\n"
            + "        FK_COD_MOD_ENSINO = value[1];\n"
            + "        \n"
            + "        \n"
            + "        if (FK_COD_MOD_ENSINO == 1 || FK_COD_MOD_ENSINO == 2 || FK_COD_MOD_ENSINO ==3){\n"
            + "            \n"
            + "            if ( (FK_COD_ETAPA_ENSINO > 30 && FK_COD_ETAPA_ENSINO < 30) ||  \n"
            + "                 (FK_COD_ETAPA_ENSINO > 60 && FK_COD_ETAPA_ENSINO < 65) ||\n"
            + "                  FK_COD_ETAPA_ENSINO == 67 || FK_COD_ETAPA_ENSINO == 68 || FK_COD_ETAPA_ENSINO == 73 || FK_COD_ETAPA_ENSINO == 74  ){\n"
            + "                \n"
            + "                return true;\n"
            + "                \n"
            + "            }            \n"
            + "        }\n"
            + "        return false;\n"
            + "};";

    private static String etapas_mod_ensino_segmento_idTransformationImpl
            = "function Etapas_mod_ensino_segmento_idTransformation(value) {\n"
            + "	if (value == null) return null;\n"
            + "        if (value[0] == null) return null;\n"
            + "        \n"
            + "        FK_COD_ETAPA_ENSINO = value[0];\n"
            + "        \n"
            + "        if (FK_COD_ETAPA_ENSINO == 1) return 1;\n"
            + "        if (FK_COD_ETAPA_ENSINO == 2) return 2;\n"
            + "        if (FK_COD_ETAPA_ENSINO == 3) return 3;\n"
            + "        if ( (FK_COD_ETAPA_ENSINO > 3 && FK_COD_ETAPA_ENSINO < 8) || (FK_COD_ETAPA_ENSINO > 13 && FK_COD_ETAPA_ENSINO < 19)) return 4;\n"
            + "        if ( (FK_COD_ETAPA_ENSINO > 7 && FK_COD_ETAPA_ENSINO < 12) || (FK_COD_ETAPA_ENSINO > 18 && FK_COD_ETAPA_ENSINO < 22) || FK_COD_ETAPA_ENSINO == 41) return 5;\n"
            + "        if ( (FK_COD_ETAPA_ENSINO > 24 && FK_COD_ETAPA_ENSINO < 39) ) return 6;\n"
            + "        if (FK_COD_ETAPA_ENSINO == 12 || FK_COD_ETAPA_ENSINO == 13 || (FK_COD_ETAPA_ENSINO > 21 && FK_COD_ETAPA_ENSINO < 25) || FK_COD_ETAPA_ENSINO == 56 ) return 7;\n"
            + "        \n"
            + "        op8 = [43,44,46,47,49,50,51,53,54,58,59,60,61,65,69,70,72,73];\n"
            + "        if (findElementInArray(op8, FK_COD_ETAPA_ENSINO)) return 8;\n"
            + "        \n"
            + "        op9 = [45, 48, 52, 55, 57, 62, 63, 67, 71, 74];\n"
            + "        if (findElementInArray(op9, FK_COD_ETAPA_ENSINO)) return 9;\n"
            + "        \n"
            + "        \n"
            + "        op10 = [39, 40, 64, 68];\n"
            + "        if (findElementInArray(op10, FK_COD_ETAPA_ENSINO)) return 10;\n"
            + "\n"
            + "        \n"
            + "        return 0;  \n"
            + "};\n"
            + "\n"
            + "function findElementInArray(array, element){\n"
            + "        for (var i=0; i<array.length; i++){\n"
            + "            if (array[i]== element){\n"
            + "                return true;\n"
            + "            }\n"
            + "        }\n"
            + "        return false;\n"
            + "};";

}
