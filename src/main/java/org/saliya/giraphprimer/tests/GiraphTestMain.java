package org.saliya.giraphprimer.tests.withmaster;

import org.apache.giraph.GiraphRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Saliya Ekanayake on 2/2/17.
 */
public class GiraphTestMain {

    public static void main(String[] args) throws Exception {
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setInt("my.conf.param", 203);
        GiraphRunner runner = new GiraphRunner();
        runner.setConf(conf);
        System.exit(ToolRunner.run(runner, args));
//        System.exit(ToolRunner.run(new GiraphRunner(), args));
    }
}
