package org.saliya.giraphprimer.multilinear.giraph.weighted;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.saliya.giraphprimer.multilinear.giraph.unweighted.VData;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Saliya Ekanayake on 2/1/17.
 */
public class WeightedVInputFormat
        extends TextVertexInputFormat<IntWritable, WeightedVData, NullWritable>{

    /* Separator of the vertex and neighbors */
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexReader createVertexReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        return new WeightedVReader();
    }

    public class WeightedVReader extends
            TextVertexReaderFromEachLineProcessed<String[]> {
        /* Cached vertex id for the current line */
        private IntWritable id;
        /* Cached vertex value for the current line */
        private WeightedVData value;

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            String[] tokens = SEPARATOR.split(line.toString());
            id = new IntWritable(Integer.parseInt(tokens[0]));
            double weight = Double.parseDouble(tokens[1]);
            value = new WeightedVData(id.get(), weight);
            return tokens;
        }

        @Override
        protected IntWritable getId(String[] tokens) throws IOException {
            return id;
        }

        @Override
        protected WeightedVData getValue(String[] tokens) throws IOException {
            return value;
        }

        @Override
        protected Iterable<Edge<IntWritable, NullWritable>> getEdges(
                String[] tokens) throws IOException {
            List<Edge<IntWritable, NullWritable>> edges =
                    Lists.newArrayListWithCapacity(tokens.length - 2);
            for (int n = 2; n < tokens.length; n++) {
                edges.add(EdgeFactory.create(
                        new IntWritable(Integer.parseInt(tokens[n]))));
            }
            return edges;
        }
    }
}
