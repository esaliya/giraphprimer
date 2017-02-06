package org.saliya.giraphprimer.multilinear;

/**
 * Saliya Ekanayake on 2/4/17.
 */
public class Utils {
    public static int log2(int x) {
        if (x <= 0) {
            throw new IllegalArgumentException("Error. Argument must be greater than 0. Found " + x);
        }
        int result = 0;
        x >>= 1;
        while (x > 0) {
            result++;
            x >>= 1;
        }
        return result;
    }
}
