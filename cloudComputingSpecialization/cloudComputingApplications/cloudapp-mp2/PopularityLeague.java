import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    //TODO: Aleix Penella
    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
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

    @Override
    public int run(String[] args) throws Exception {
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

	jobA.setJarByClass(PopularityLeague.class);
        jobA.waitForCompletion(true);

	Job jobB = Job.getInstance(this.getConf(), "Popularity League");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

	jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);

	jobB.setMapperClass(PopularityLeagueMap.class);
        jobB.setReducerClass(PopularityLeagueReduce.class);
        jobB.setNumReduceTasks(1);

	FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

	jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

	jobB.setJarByClass(PopularityLeague.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }


    // Job A: Link Count
    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
	@Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	List<String> league;
	
 	@Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String defLeaguePath = conf.get("league");
            this.league = Arrays.asList(readHDFSFile(defLeaguePath, conf).split("\n"));
	
		while( this.league.size() > 15){
			this.league.remove(this.league.size()-1);
		}
	}

	@Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		if ( this.league.contains(key.toString())){
			for ( IntWritable link: values ){
				sum += link.get(); 
			}
			context.write(key, new IntWritable(sum));
		}
	}
    }
    //Job B: League
    public static class PopularityLeagueMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
	private TreeSet<Pair<Integer, Integer>> countTopLinksMap = new TreeSet<Pair<Integer, Integer>>();

	@Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		Integer[] tuple = {Integer.parseInt(value.toString()),Integer.parseInt(key.toString())};
		context.write(NullWritable.get(), new IntArrayWritable(tuple));	
	}
    }


    public static class PopularityLeagueReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
	private TreeSet<Pair<Integer, Integer>> countTopLinksMap = new TreeSet<Pair<Integer, Integer>>();
	
	@Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
		for (IntArrayWritable val: values) {
			IntWritable[] pair = (IntWritable[]) val.toArray();
			Integer links = Integer.parseInt(pair[0].toString());
			Integer page = Integer.parseInt(pair[1].toString());

			countTopLinksMap.add(new Pair<Integer,Integer>(page,links));
		}
		
		int pos = 0;	
		for ( Pair<Integer,Integer> page: countTopLinksMap ) {
			for ( Pair<Integer,Integer> otherpage: countTopLinksMap ) {
				if ( page.first != otherpage.first && page.second > otherpage.second){
					pos++;	
				}
			}
			context.write(new IntWritable(page.first),new IntWritable(pos));
			pos = 0;
		}	 	
   	} 
    }

}

class Pair<A extends Comparable<? super A>, B extends Comparable<? super B>> implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>, B extends Comparable<? super B>> Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    // return <0 if A less than B, 0 if equal or >0 greater
    // first comparation is for the first item and if they are equal is compared the second one.	
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first) && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
