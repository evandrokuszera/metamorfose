/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package metamorfose5.map.script;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
//import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.Scriptable;

/**
 *
 * @author Evandro
 */
public class JavascriptEngine implements Serializable {
    private long count = 0;

//    private ScriptEngineManager factory = null;
//    private ScriptEngine engine = null;
//    private Invocable invocable = null;
    
    private String javascriptCodes = "";
    private List<Script> scripts = null;
    private Context context;
    private Scriptable scope;
    
    private void loadFactory(){
        
        if (context == null){
            
            context = Context.enter();

            try{            
                scope = context.initStandardObjects();
                context.evaluateString(scope, javascriptCodes, "scripts", 1, null);
                System.out.println("JAVASCRIPT-RHINO_1.7.8-ENGINE: SCRIPTS LOADED WITH SUCCESS!");
            } catch (Exception e){
                System.out.println("JAVASCRIPT-RHINO_1.7.8-ENGINE-ERROR: " + JavascriptEngine.class.getName() + ":" + e);
                System.out.println(javascriptCodes);
            }
            
        }
        
//        
//        
//        if (factory == null) {
//            factory = new ScriptEngineManager();
//            engine = factory.getEngineByName("JavaScript");
//            
//            try {
//                engine.eval(javascriptCodes);                
//                invocable = (Invocable) engine;
//                System.out.println("JAVASCRIPT-ENGINE: SCRIPTS LOADED WITH SUCCESS!");
//            } catch (ScriptException ex) {
//                System.out.println("JAVASCRIPT-ENGINE-ERROR: " + JavascriptEngine.class.getName() + ":" + ex);
//                System.out.println(javascriptCodes);
//            }
//        }
    }

    public void setScripts(List<Script> scripts) {
        this.scripts = scripts;
        // carregando as implementações das transformações em Javascript
        javascriptCodes = "";
        for (Script script : scripts) {
            javascriptCodes += script.getFunctionCode() + "\n\n";
        }
    }

    public List<Script> getScripts() {
        return scripts;
    }   
    
    // One value return.
    public Object executeTransformationJSON(String methodName, Object value) {   
        
        loadFactory();
        
        Object result = null;
        
        try{  
            Function fct = (Function) scope.get(methodName, scope);                        
            result = fct.call(context, scope, scope, new Object[] {value}); 
        }
        catch (Exception e) {
            System.out.println("ERRO: " + JavascriptEngine.class.getName() + ":" + e);
        }

        return result;
    }
    
//    // One value return.
//    public Object executeTransformation(String methodName, Object[] value) {              
//
//        loadFactory();
//
//        Object result = null;
//
//        try {
//
//            if (value == null) {
//                result = invocable.invokeFunction(methodName, null);
//            } else {
//                result = invocable.invokeFunction(methodName, Arrays.asList(value));
//            }
//
//        } catch (ScriptException | NoSuchMethodException ex) {
//            System.out.println("ERRO: " + JavascriptEngine.class.getName() + ":" + ex);
//        }
//
//        return result;
//    }
//    
//    // Array value return.
//    public Object[] executeTransformationAndReturnArray(String methodName, Object[] value) {              
//
//        loadFactory();
//
//        Object result = null;
//
//        try {
//
//            if (value == null) {                
//                result = invocable.invokeFunction(methodName, null);
//            } else {
//                result = invocable.invokeFunction(methodName, Arrays.asList(value));
//            }
//
//        } catch (ScriptException | NoSuchMethodException ex) {
//            System.out.println("ERRO: " + JavascriptEngine.class.getName() + ":" + ex);
//        }
//        return getArray(result);
//    }
    
    // Transform the return value in an Java array.
//    private static Object[] getArray(Object jsArray){
//        ScriptObjectMirror obj = (ScriptObjectMirror)jsArray;  
//        return obj.values().toArray();        
//    }
    
}
