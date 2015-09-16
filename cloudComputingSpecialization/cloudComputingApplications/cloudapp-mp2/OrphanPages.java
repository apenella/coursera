import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;

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
	Configuration conf = this.getConf();
	FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

	//delete the output folder
        fs.delete(new Path(args[1]), true);

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
        jobB.setOutputValueClass(IntWritable.class);

	//todo
	jobB.setMapOutputKeyClass(IntWritable.class);
        jobB.setMapOutputValueClass(IntWritable.class);
	
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

    public class IntArrayWritable extends ArrayWritable{
        
        public IntArrayWritable() {
        	super(IntWritable.class);
    	}
        
    	public IntArrayWritable(IntWritable[] values) {
        	super(IntWritable.class, values);
    	}
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
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
	}

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	//TODO: Aleix Penella
		String token = new String();
		IntWritable one = new IntWritable(1);
		IntWritable zero = new IntWritable(0);
		
		StringTokenizer pageLinks = new StringTokenizer(value.toString(),":");
		//get the page id
		if (pageLinks.hasMoreTokens()){
			context.write(new IntWritable(Integer.parseInt(pageLinks.nextToken())),zero);
			// get the page's links 	
			if (pageLinks.hasMoreTokens()){
				StringTokenizer links = new StringTokenizer(pageLinks.nextToken());
				while (links.hasMoreTokens()) {
					context.write(new IntWritable(Integer.parseInt(links.nextToken())),one);
     				}
			}
		}	
        }
    }
    // JobA: Reduce count link
    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	@Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	//TODO: Aleix Penella
		int sum = 0;
		for ( IntWritable link: values ){
			sum += link.get(); 
		}
		if ( sum == 0 ){
			context.write(key, new IntWritable(sum));
		}	
        }
    }


    // JobB: Map OrphanPages 
    public static class OrphanPagesMap extends Mapper<Text, Text, IntWritable, IntWritable> {

	@Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
	}

        @Override
        public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
        	//TODO: Aleix Penella
		context.write(new IntWritable(Integer.parseInt(key.toString())), new IntWritable(Integer.parseInt(values.toString())));
        }
    }
    // JobB: Reduce OrphanPages
    public static class OrphanPagesReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        
	@Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	//TODO: Aleix Penella
		context.write(key, NullWritable.get());
        }
    }
}
