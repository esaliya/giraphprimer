package org.saliya.giraphprimer;

import com.google.common.base.Strings;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.regex.Pattern;

/**
 * Saliya Ekanayake on 2/12/17.
 */
public class ConvertSnapToSimple {
    public static void main(String[] args) {
        String f = "/Users/esaliya/sali/projects/graphs/giraph/data/snap/p2p-Gnutella04.txt";
        int k = 8;

        File file = new File(f);
        String parent = file.getParent();
        String name = com.google.common.io.Files.getNameWithoutExtension(f);
        try(BufferedReader reader = Files.newBufferedReader(Paths.get(f));
            BufferedWriter writer = Files.newBufferedWriter(Paths.get(parent, name+"_simple_k" + k + ".txt"))) {
            PrintWriter printer = new PrintWriter(writer, true);

            Random random = new Random();
            Pattern pat = Pattern.compile("\t");
            String line = null;
            String[] splits;
            int currentNode = -1;
            while ((line = reader.readLine()) != null){
                if (Strings.isNullOrEmpty(line) || line.startsWith("#")) continue;

                splits = pat.split(line);
                int src = Integer.parseInt(splits[0]);
                int dest = Integer.parseInt(splits[1]);
                if (currentNode != src){
                    if (currentNode != -1){
                        printer.println();
                    }
                    currentNode = src;
                    printer.print(src + " " + random.nextInt(k-1) + " ");// this will give colors from [0,k-2] that's k-1 colors
                }
                printer.print(dest + " ");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
