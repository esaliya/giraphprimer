package org.saliya.giraphprimer;

import com.google.common.collect.Lists;
import org.apache.commons.lang.ObjectUtils;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Saliya Ekanayake on 2/1/17.
 */
public class VInputFormat
        extends TextVertexInputFormat<IntWritable, VData, NullWritable>{

    /* Separator of the vertex and neighbors */
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexReader createVertexReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        return new VReader();
    }

    public class VReader extends
            TextVertexReaderFromEachLineProcessed<String[]> {
        /* Cached vertex id for the current line */
        private IntWritable id;
        /* Cached vertex value for the current line */
        private VData value;

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            String[] tokens = SEPARATOR.split(line.toString());
            id = new IntWritable(Integer.parseInt(tokens[0]));
            int color = Integer.parseInt(tokens[1]);
            value = new VData(id.get(), color);
            return tokens;
        }

        @Override
        protected IntWritable getId(String[] tokens) throws IOException {
            return id;
        }

        @Override
        protected VData getValue(String[] tokens) throws IOException {
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
