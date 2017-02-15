package org.saliya.giraphprimer.tests;

import java.util.Random;

/**
 * Saliya Ekanayake on 2/5/17.
 */
public class RandomTest {
    public static void main(String[] args) {
        long seed = 10;
        Random rand = new Random(seed);
        for (int i = 0; i < 15; ++i){
            System.out.println(rand.nextInt());
        }
        Random rand2 = new Random(seed);


        for (int i = 0; i < 15; ++i){
            System.out.println(rand.nextInt() + " " + rand2.nextInt());
        }
    }
}
