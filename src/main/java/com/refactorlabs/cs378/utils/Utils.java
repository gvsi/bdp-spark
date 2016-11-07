package com.refactorlabs.cs378.utils;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by gvsi on 9/24/16.
 */
public class Utils {
    /**
     * Writes the classpath to standard out, for inspection.
     */
    public static void printClassPath() {
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        URL[] urls = ((URLClassLoader) cl).getURLs();
        System.out.println("classpath BEGIN");
        for (URL url : urls) {
            System.out.println(url.getFile());
        }
        System.out.println("classpath END");
        System.out.flush();
    }

}
