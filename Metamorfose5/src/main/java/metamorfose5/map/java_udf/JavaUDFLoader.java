/*
 * Recupera uma instância de classe que implementa a interface Transformation através do nome completo da classe.
 */
package metamorfose5.map.java_udf;

/**
 *
 * @author Evandro
 */
public class JavaUDFLoader {
    
    public static JavaUDF getJavaUDFClass(String className){
        
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();        
        Class javaUDFClass = null;
        JavaUDF javaUDF = null;  // Interface JavaUDF            
        
        try {
            javaUDFClass = classLoader.loadClass(className);
            javaUDF = (JavaUDF) javaUDFClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            System.out.println("ERROR - JavaUDFLoader.class.getName(): " + ex);
        }
        
        return javaUDF;
    }
    
}
