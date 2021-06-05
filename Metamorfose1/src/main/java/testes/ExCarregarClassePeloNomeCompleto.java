/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testes;

import metamorfose.transformations.Transformation;

/**
 *
 * @author Evandro
 */
public class ExCarregarClassePeloNomeCompleto {

    public static void main(String[] args) {

        teste_com_vetor_de_valores();

    }

    public static void teste_transformacao_com_um_valor() {

        ClassLoader classLoader = ClassLoader.getSystemClassLoader();

        try {
            Class t1 = classLoader.loadClass("com.kuszera.simcaq.transformations.SexoTransformation");
            Transformation transformer = (Transformation) t1.newInstance();
            System.out.println(transformer.executeTransformation( new Object[]{"F"}) );

        } catch (ClassNotFoundException ex) {
            System.out.println(ex);
        } catch (InstantiationException ex) {
            System.out.println(ex);
        } catch (IllegalAccessException ex) {
            System.out.println(ex);
        }
    }

    public static void teste_com_vetor_de_valores() {

        ClassLoader classLoader = ClassLoader.getSystemClassLoader();

        try {
            Class t1 = classLoader.loadClass("com.kuszera.simcaq.transformations.EnsinoRegularTransformation");
            Transformation transformer = (Transformation) t1.newInstance();

            Object[] valores = {62, 1};
            System.out.println(transformer.executeTransformation(valores));

        } catch (ClassNotFoundException ex) {
            System.out.println(ex);
        } catch (InstantiationException ex) {
            System.out.println(ex);
        } catch (IllegalAccessException ex) {
            System.out.println(ex);
        }
    }

}
