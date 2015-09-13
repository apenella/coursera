import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
    	//TODO: Aleix Penella
	FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

	Job jobA = Job.getInstance(this.getConf(), "Link Count");
	jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

	jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

	FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

	jobA.setJarByClass(OrphanPages.class);
        jobA.waitForCompletion(true);

	Job jobB = Job.getInstance(conf, "Orphan Pages");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(NullWritable.class);

	//todo
	jobB.setMapOutputKeyClass(IntWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);
	
	jobB.setMapperClass(OrphanPagesMap.class);
        jobB.setReducerClass(OrphanPagesReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

	jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

	jobB.setJarByClass(OrphanPages.class);
	return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

	/*
		Strategy: 
		jobA:
			-map: for each page, append an IntWritable(0) for the key and IntWritable(1) for values to the output
			-reduce: count all links. (If a page has no links to it, it only have a IntWritable(0)
			-output: <page, links to it>
		jobB:
			-map: use the links to it from the value as the key and all pages with this number of links as value
			-reduce: list all the pages 0 links to it
	*/


    // JobA: Map count link
    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	//TODO: Aleix Penella
		
        }
    }
    // JobA: Reduce count link
    public static class LinkCountReduce extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        	//TODO: Aleix Penella
        }
    }


    // JobB: Map OrphanPages 
    public static class OrphanPagesMap extends Reducer<IntWritable, IntArrayWritable, IntWritable, NullWritable> {
        @Override
        public void map(IntWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        	//TODO: Aleix Penella
        }
    }
    // JobB: Reduce OrphanPages
    public static class OrphanPagesReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	//TODO: Aleix Penella
        }
    }
}
