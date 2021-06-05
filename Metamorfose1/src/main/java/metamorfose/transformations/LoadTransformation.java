/*
 * Recupera uma instância de classe que implementa a interface Transformation através do nome completo da classe.
 */
package metamorfose.transformations;

/**
 *
 * @author Evandro
 */
public class LoadTransformation {
    
    public static Transformation getTransformationByClassName(String className){
        
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();        
        Class transformationClass = null;
        Transformation transformer = null;  // Interface Transformation            
        
        try {
            transformationClass = classLoader.loadClass(className);
            transformer = (Transformation) transformationClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            System.out.println("LoadTransformation.class.getName(): " + ex);
        }
        
        return transformer;
    }
    
}
