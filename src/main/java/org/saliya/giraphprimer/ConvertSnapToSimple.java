package org.saliya.giraphprimer;

import com.google.common.base.Strings;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

/**
 * Saliya Ekanayake on 2/12/17.
 */
public class ConvertSnapToSimple {
    public static void main(String[] args) {
        String f = "/Users/esaliya/sali/projects/graphs/giraph/data/snap/p2p-Gnutella04.txt";
        int k = 8;
        int maxNodeCount = 10876;

        File file = new File(f);
        String parent = file.getParent();
        String name = com.google.common.io.Files.getNameWithoutExtension(f);
        try(BufferedReader reader = Files.newBufferedReader(Paths.get(f));
            BufferedWriter writer = Files.newBufferedWriter(Paths.get(parent, name+"_simple_k" + k + ".txt"))) {
            PrintWriter printer = new PrintWriter(writer, true);

            int boundForRandomInts = k-1;
            Random random = new Random();
            Pattern pat = Pattern.compile("\t");
            String line = null;
            String[] splits;
            int currentNode = -1;
            TreeMap<Integer, Integer> allVertices = new TreeMap<>();
            Hashtable<Integer, ArrayList<Integer>> adjecencyList = new Hashtable<>();
            int count = 0;
            while ((line = reader.readLine()) != null){
                if (Strings.isNullOrEmpty(line) || line.startsWith("#")) continue;

                splits = pat.split(line);
                int src = Integer.parseInt(splits[0]);
                int dest = Integer.parseInt(splits[1]);
                if (allVertices.putIfAbsent(src,count) == null) ++count;
                if (allVertices.putIfAbsent(dest,count) == null) ++count;

                if (currentNode != src){
                    currentNode = src;
                    adjecencyList.put(src, new ArrayList<>());
                }
                adjecencyList.get(src).add(dest);
            }

            if (allVertices.size() != maxNodeCount){
                System.out.println("Error, allVertices.size() should be equal to maxNodeCount");
            }

            Set<Integer> keySet = allVertices.keySet();
            TreeMap<Integer, Integer> reverseMappingOfAllVertices = new TreeMap<>();
            for (int oldIdx : keySet){
                int newIdx = allVertices.get(oldIdx);
                reverseMappingOfAllVertices.put(newIdx, oldIdx);
            }

            keySet = reverseMappingOfAllVertices.keySet();
            count = 0;
            for (int newIdx : keySet){
                int oldIdx = reverseMappingOfAllVertices.get(newIdx);
                printer.print(newIdx + " " + random.nextInt(boundForRandomInts) + " ");
                if (adjecencyList.containsKey(oldIdx)){
                    ArrayList<Integer> neighbors = adjecencyList.get(oldIdx);
                    IntStream.range(0, neighbors.size()).forEach(oldNeighborIdx -> printer.print(allVertices.get(oldNeighborIdx) + " "));
                }

                if (count<keySet.size()-1){
                    printer.println();
                }
                ++count;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
