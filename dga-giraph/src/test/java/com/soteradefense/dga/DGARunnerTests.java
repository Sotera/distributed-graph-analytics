package com.soteradefense.dga;

import com.soteradefense.dga.hbse.HBSEComputation;
import com.soteradefense.dga.lc.LeafCompressionComputation;
import com.soteradefense.dga.louvain.giraph.LouvainComputation;
import com.soteradefense.dga.pr.PageRankComputation;
import com.soteradefense.dga.wcc.WeaklyConnectedComponentComputation;
import org.apache.giraph.GiraphRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.whenNew;
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.stub;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DGARunner.class, ToolRunner.class, DGAConfiguration.class, FileSystem.class, Job.class})
public class DGARunnerTests {


    @Test
    public void testWCCDefaults() throws Exception {
        String[] expected = {
                "-D", "giraph.useSuperstepCounters=false", "-D", "giraph.zkList=localhost:2181", "com.soteradefense.dga.wcc.WeaklyConnectedComponentComputation", "-eip", "/input/", "-op", "/output/", "-eof",
                "com.soteradefense.dga.io.formats.DGAEdgeTTTOutputFormat", "-eif", "com.soteradefense.dga.io.formats.DGATextEdgeValueInputFormat", "-w", "1", "-ca", "write.vertex.value=true", "-ca", "mapred.task.timeout=600000"};
        PowerMockito.mockStatic(ToolRunner.class);
        String[] args = {"libs/","wcc", "/input/", "/output/"};
        GiraphRunner runner = mock(GiraphRunner.class);
        DGAConfiguration conf = new DGAConfiguration();
        whenNew(GiraphRunner.class).withNoArguments().thenReturn(runner);
        whenNew(DGAConfiguration.class).withNoArguments().thenReturn(conf);
        stub(method(ToolRunner.class, "run", runner.getClass(), String[].class)).toReturn(0);
        stub(method(System.class, "exit")).toReturn(0);
        DGARunner.main(args);
        for (String s : conf.convertToCommandLineArguments(WeaklyConnectedComponentComputation.class.getCanonicalName())) {
            boolean hasValue = false;
            int i = 0;
            while (!hasValue && i < expected.length) {
                hasValue = s.equals(expected[i]);
                i++;
            }
            assertTrue(hasValue);
        }
    }

    @Test
    public void testHBSEDefaults() throws Exception {
        String[] expected = {"-D", "giraph.useSuperstepCounters=false", "-D", "giraph.zkList=localhost:2181", "com.soteradefense.dga.hbse.HBSEComputation", "-eip", "/input/", "-op", "/output/", "-vof", "com.soteradefense.dga.io.formats.HBSEOutputFormat", "-eif", "com.soteradefense.dga.io.formats.DGATextEdgeValueInputFormat", "-mc", "com.soteradefense.dga.hbse.HBSEMasterCompute", "-w", "1", "-ca", "betweenness.output.dir=/output/", "-ca", "pivot.batch.size=10", "-ca", "betweenness.set.maxSize=10", "-ca", "pivot.batch.size.initial=10", "-ca", "mapred.task.timeout=600000", "-ca", "vertex.count=5"};
        PowerMockito.mockStatic(ToolRunner.class);
        String[] args = {"libs/","hbse", "/input/", "/output/"};
        GiraphRunner runner = mock(GiraphRunner.class);
        DGAConfiguration conf = new DGAConfiguration();
        whenNew(GiraphRunner.class).withNoArguments().thenReturn(runner);
        whenNew(DGAConfiguration.class).withNoArguments().thenReturn(conf);
        stub(method(ToolRunner.class, "run", runner.getClass(), String[].class)).toReturn(0);
        stub(method(System.class, "exit")).toReturn(0);
        DGARunner.main(args);
        for (String s : conf.convertToCommandLineArguments(HBSEComputation.class.getCanonicalName())) {
            boolean hasValue = false;
            int i = 0;
            while (!hasValue && i < expected.length) {
                hasValue = s.equals(expected[i]);
                i++;
            }
            assertTrue(hasValue);
        }
    }

    @Test
    public void testLCDefaults() throws Exception {
        String[] expected = {"-D", "giraph.useSuperstepCounters=false", "-D", "giraph.zkList=localhost:2181", "com.soteradefense.dga.lc.LeafCompressionComputation", "-eip", "/input/", "-op", "/output/", "-eof", "com.soteradefense.dga.io.formats.DGAEdgeTTTOutputFormat", "-eif", "com.soteradefense.dga.io.formats.DGATextEdgeValueInputFormat", "-esd", "/output/", "-w", "1", "-ca", "mapred.task.timeout=600000"};
        PowerMockito.mockStatic(ToolRunner.class);
        String[] args = {"libs/","lc", "/input/", "/output/"};
        GiraphRunner runner = mock(GiraphRunner.class);
        DGAConfiguration conf = new DGAConfiguration();
        whenNew(GiraphRunner.class).withNoArguments().thenReturn(runner);
        whenNew(DGAConfiguration.class).withNoArguments().thenReturn(conf);
        stub(method(ToolRunner.class, "run", runner.getClass(), String[].class)).toReturn(0);
        stub(method(System.class, "exit")).toReturn(0);
        DGARunner.main(args);
        for (String s : conf.convertToCommandLineArguments(LeafCompressionComputation.class.getCanonicalName())) {
            boolean hasValue = false;
            int i = 0;
            while (!hasValue && i < expected.length) {
                hasValue = s.equals(expected[i]);
                i++;
            }
            assertTrue(hasValue);
        }
    }

    @Test
    public void testPRDefaults() throws Exception {
        String[] expected = {"-D", "giraph.useSuperstepCounters=false", "-D", "giraph.zkList=localhost:2181", "com.soteradefense.dga.pr.PageRankComputation", "-eip", "/input/", "-op", "/output/", "-eof", "com.soteradefense.dga.io.formats.DGAEdgeTDTOutputFormat", "-eif", "com.soteradefense.dga.io.formats.DGATextEdgeValueInputFormat", "-mc", "com.soteradefense.dga.pr.PageRankMasterCompute", "-w", "1", "-ca", "write.vertex.value=true", "-ca", "mapred.task.timeout=600000"};
        PowerMockito.mockStatic(ToolRunner.class);
        String[] args = {"libs/","pr", "/input/", "/output/"};
        GiraphRunner runner = mock(GiraphRunner.class);
        DGAConfiguration conf = new DGAConfiguration();
        whenNew(GiraphRunner.class).withNoArguments().thenReturn(runner);
        whenNew(DGAConfiguration.class).withNoArguments().thenReturn(conf);
        stub(method(ToolRunner.class, "run", runner.getClass(), String[].class)).toReturn(0);
        stub(method(System.class, "exit")).toReturn(0);
        DGARunner.main(args);
        for (String s : conf.convertToCommandLineArguments(PageRankComputation.class.getCanonicalName())) {
            boolean hasValue = false;
            int i = 0;
            while (!hasValue && i < expected.length) {
                hasValue = s.equals(expected[i]);
                i++;
            }
            assertTrue(hasValue);
        }
    }

    @Test
    public void testLouvainDefaults() throws Exception {
        String[] expected = {"-D", "giraph.useSuperstepCounters=false", "-D", "giraph.zkList=localhost:2181", "com.soteradefense.dga.louvain.giraph.LouvainComputation", "-eip", "/input/", "-op", "/output/giraph_0", "-vof", "com.soteradefense.dga.io.formats.LouvainVertexOutputFormat", "-eif", "com.soteradefense.dga.io.formats.DGALongEdgeValueInputFormat", "-mc", "com.soteradefense.dga.louvain.giraph.LouvainMasterCompute", "-esd", "/output/giraph_0", "-w", "1", "-ca", "minimum.progress=2000", "-ca", "progress.tries=1", "-ca", "mapred.task.timeout=600000", "-ca", "actual.Q.aggregators=1"};
        PowerMockito.mockStatic(ToolRunner.class);
        PowerMockito.mockStatic(Job.class);
        Job job = mock(Job.class);
        GiraphRunner runner = mock(GiraphRunner.class);
        Configuration hadoopConf = mock(Configuration.class);
        FileSystem fs = mock(FileSystem.class);
        String[] args = {"libs/","louvain", "/input/", "/output/"};
        DGAConfiguration conf = new DGAConfiguration();
        whenNew(GiraphRunner.class).withNoArguments().thenReturn(runner);
        whenNew(DGAConfiguration.class).withNoArguments().thenReturn(conf);
        whenNew(FileSystem.class).withAnyArguments().thenReturn(fs);
        whenNew(Configuration.class).withNoArguments().thenReturn(hadoopConf);
        stub(method(ToolRunner.class, "run", runner.getClass(), String[].class)).toReturn(1);
        whenNew(Job.class).withAnyArguments().thenReturn(job);
        stub(method(Job.class, "getInstance", Configuration.class)).toReturn(job);
        stub(method(Job.class, "waitForCompletion", boolean.class)).toReturn(0);
        stub(method(System.class, "exit")).toReturn(0);
        stub(method(FileSystem.class, "get", hadoopConf.getClass())).toReturn(fs);
        stub(method(FileSystem.class, "exists", Path.class)).toReturn(true);
        stub(method(FileSystem.class, "isFile", Path.class)).toReturn(true);
        DGARunner.main(args);
        for (String s : conf.convertToCommandLineArguments(LouvainComputation.class.getCanonicalName())) {
            boolean hasValue = false;
            int i = 0;
            while (!hasValue && i < expected.length) {
                hasValue = s.equals(expected[i]);
                i++;
            }
            assertTrue(hasValue);
        }
    }

}
