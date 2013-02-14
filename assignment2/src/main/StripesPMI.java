import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Hashtable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.StringWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;

import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.cloud9.io.pair.PairOfStringFloat;
import edu.umd.cloud9.io.pair.PairOfFloatString;

import edu.umd.cloud9.io.map.String2IntOpenHashMapWritable;

public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

  // Mapper: emits (token, 1) for every word occurrence.
  private static class MyMapper extends Mapper<LongWritable, Text, Text, String2IntOpenHashMapWritable> {

    // Reuse objects to save overhead of object creation.
    private final static String2IntOpenHashMapWritable MAP = new String2IntOpenHashMapWritable();
    private final static Text KEY = new Text();
    private static long rows=0;
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      String[] terms =line.split("\\s+");
      for (int i=0;i<terms.length;i++)
      {
    	  if( terms[i].equals("\0")) continue;
    	  for (int j=i+1; j<terms.length;j++)
    	  {
    		  if(terms[j].equals(terms[i]))
    		  {
    			  terms[j]="\0";
    		  }
    	  }
    	  
      }
      
      
      for (int i=0;i<terms.length;i++)
      {
    	  String term =terms[i];
    	  if (term.length()==0||term.equals("\0"))
    	  	  continue;
    	  
    	  MAP.clear();
    	  
    	  for (int j=0; j<terms.length;j++)
    	  {
    		  if(j==i)
    			  continue;
    		  if(terms[j].length()==0||terms[j].equals("\0"))
    			  continue;
    		  if(MAP.containsKey(terms[j]))
    		  {
    			  continue;
    		  }else{MAP.put(terms[j],1);}
    	  }
    	  MAP.put("\1",1);
    	  KEY.set(term);
    	  context.write(KEY,MAP);
      }
    
    }
  }
  

  
 private static class MyMapper2 extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {

	    // Reuse objects to save overhead of object creation.
	    private final static FloatWritable NUM = new FloatWritable(1);
	    private final static PairOfStrings KEY = new PairOfStrings();

	    @Override
	    public void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {
	      String line = ((Text) value).toString();
	      StringTokenizer itr = new StringTokenizer(line);
	      while (itr.hasMoreTokens()) {
	    	String left=itr.nextToken();
	    	left=left.substring(1, left.length()-1);
	    	String right=itr.nextToken();
	    	right=right.substring(0, right.length()-1);
	    	String numS=itr.nextToken();
	    	float num=Float.parseFloat(numS);
	    	NUM.set(num);
	    	KEY.set(left,right);
	    	context.write(KEY,NUM);
	    	KEY.set(right,left);
	    	context.write(KEY,NUM);
	    	
	      }
	    }
	  }

 private static class MyMapper3 extends Mapper<LongWritable, Text, PairOfFloatString, Text> {

	    // Reuse objects to save overhead of object creation.
	    private final static Text STR = new Text();
	    private final static PairOfFloatString KEY = new PairOfFloatString();

	    @Override
	    public void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {
	      String line = ((Text) value).toString();
	      StringTokenizer itr = new StringTokenizer(line);
	      while (itr.hasMoreTokens()) {
	    	String left=itr.nextToken();
	    	left=left.substring(1, left.length()-1);
	    	String right=itr.nextToken();
	    	right=right.substring(0, right.length()-1);
	    	String numS=itr.nextToken();
	    	float num=Float.parseFloat(numS);
	    	STR.set(right);
	    	KEY.set(num,left);
	    	context.write(KEY,STR);	    	
	      }
	    }
	  }
  
  
  private static class MyCombiner extends Reducer<Text, String2IntOpenHashMapWritable, Text, String2IntOpenHashMapWritable> {

	    // Reuse objects.
		//private final static PairOfStrings KEY = new PairOfStrings();
	    //private final static FloatWritable VALUE = new FloatWritable();
	    //private float marginal =0.0f;

	    @Override
	    public void reduce(Text key, Iterable<String2IntOpenHashMapWritable> values, Context context)
	        throws IOException, InterruptedException {
	      // Sum up values.
	      Iterator<String2IntOpenHashMapWritable> iter = values.iterator();
	      String2IntOpenHashMapWritable map = new String2IntOpenHashMapWritable();

	      //float sum = 0.0f;
	      while (iter.hasNext()) {
	        map.plus(iter.next());
	      }
	      context.write(key, map);
	      
	    }
	  }
  
  
  
  /*protected static class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
	    @Override
	    public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
	    	if(!key.getLeftElement().equals("*")){
	    		return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	    	}
	    	else
	    		return 0;
	    }
	  }
  */
  protected static class MyPartitioner3 extends Partitioner<PairOfFloatString, Text> {
	    @Override
	    public int getPartition(PairOfFloatString key, Text value, int numReduceTasks) {
	   
	    		return 0;
	    	
	    }
	  }
  
  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<Text, String2IntOpenHashMapWritable, PairOfStrings, FloatWritable> {

    // Reuse objects.
	private final static PairOfStrings KEY = new PairOfStrings();
    private final static FloatWritable VALUE = new FloatWritable();
    //private float marginal =0.0f;

    @Override
    public void reduce(Text key, Iterable<String2IntOpenHashMapWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<String2IntOpenHashMapWritable> iter = values.iterator();
      String2IntOpenHashMapWritable map = new String2IntOpenHashMapWritable();
      //System.out.print("REDUCER\n\n\n");
      float sum=0.0f;
      while (iter.hasNext()) {
        map.plus(iter.next());
        //System.out.print("Whileloop\n\n\n");
      }
      //context.write(key, map);

      sum =(float)map.getInt("\1");
    	
    
      for(String2IntOpenHashMapWritable.Entry<String> e : map.object2IntEntrySet())
      {
    	 String keyright = e.getKey();
    	 float temp=(float)map.getInt(keyright);
    	 if (temp>=10&&(!keyright.equals("\1")))
    	 {
    		 float PMI=temp/(sum*sum);
    		 //System.out.print(keyright+"\n");
    		 KEY.set(key.toString(),keyright);
    		 VALUE.set(PMI);
    		 context.write(KEY,VALUE);
    		}
      }
      /*if(sum>=10){
    	  if (key.getRightElement().equals("*") || key.getLeftElement().equals("*"))
    	  {
    		  VALUE.set(sum);
    		  context.write(key, VALUE);
    		  marginal =sum;
    	  }else{
    		  VALUE.set(sum/(marginal*marginal));
    		  //VALUE.set(sum);
    		  context.write(key, VALUE);
    	  }
      }*/
    }
  }
  
  
  private static class MyReducer2 extends Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {

	    // Reuse objects.
	    private final static FloatWritable VALUE = new FloatWritable();
	    //private final static PairOfStringFloat NewKey = new PairOfStringFloat();
	    //private float maxPMI =0.0f;
	    //private final static PairOfStringFloat MaxKey = new PairOfStringFloat();

	    @Override
	    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
	        throws IOException, InterruptedException {
	      // Sum up values.
	    	if (!key.getRightElement().equals("*") && !key.getLeftElement().equals("*"))
	    	{
	    		Iterator<FloatWritable> iter = values.iterator();
	    		float  PMI= 1.0f;
	    		while (iter.hasNext())
	    		{
	    			PMI*=iter.next().get();
	    		}
	    		PMI=(float)Math.sqrt((double)PMI);
	    		PMI=(float)Math.log(156215*(double)PMI);
	    		VALUE.set(PMI);
	    		//NewKey.set(key.getLeftElement(),PMI);
	    		context.write(key, VALUE);
	    	}
	    }
	  }
  
  private static class MyReducer3 extends Reducer<PairOfFloatString, Text, PairOfFloatString, Text> {

	    // Reuse objects.
	    //private final static FloatWritable VALUE = new FloatWritable();
	    //private float marginal =0.0f;

	    @Override
	    public void reduce(PairOfFloatString key, Iterable<Text> values, Context context)
	        throws IOException, InterruptedException {
	      // Sum up values.
	    	Iterator<Text> iter = values.iterator();
	    	while (iter.hasNext()){
	    		context.write(key, iter.next());
	    	}
	    }
	  }

  /**
   * Creates an instance of this tool.
   */
  public StripesPMI() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Configuration conf = getConf();
    Job job1 = Job.getInstance(conf);
    job1.setJobName(StripesPMI.class.getSimpleName());
    job1.setJarByClass(StripesPMI.class);

    job1.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job1, new Path(inputPath));
    FileOutputFormat.setOutputPath(job1, new Path(outputPath+"A"));

    //job1.setOutputKeyClass(PairOfStrings.class);
    //job1.setOutputValueClass(FloatWritable.class);
    
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(String2IntOpenHashMapWritable.class);

    job1.setMapperClass(MyMapper.class);
    job1.setCombinerClass(MyCombiner.class);
    //job1.setPartitionerClass(MyPartitioner.class);
    //job1.setReducerClass(MyCombiner.class);
    job1.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath+"A");
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    LOG.info("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    
    //Have a second job
   
    Job job2 = Job.getInstance(conf);
    job2.setJobName(StripesPMI.class.getSimpleName());
    job2.setJarByClass(StripesPMI.class);

    job2.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job2, new Path(outputPath+"A"));
    FileOutputFormat.setOutputPath(job2, new Path(outputPath+"B"));

    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(FloatWritable.class);

    job2.setMapperClass(MyMapper2.class);
    //job2.setCombinerClass(MyCombiner.class);
    //job2.setPartitionerClass(MyPartitioner.class);
    job2.setReducerClass(MyReducer2.class);

    // Delete the output directory if it exists already.
    Path outputDir2 = new Path(outputPath+"B");
    FileSystem.get(conf).delete(outputDir2, true);

    long startTime2 = System.currentTimeMillis();
    job2.waitForCompletion(true);
    LOG.info("Job2 Finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");
    
    
//Have a third job
    
    Job job3 = Job.getInstance(conf);
    job3.setJobName(PairsPMI.class.getSimpleName());
    job3.setJarByClass(PairsPMI.class);

    job3.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job3, new Path(outputPath+"B"));
    FileOutputFormat.setOutputPath(job3, new Path(outputPath));

    job3.setOutputKeyClass(PairOfFloatString.class);
    job3.setOutputValueClass(Text.class);

    job3.setMapperClass(MyMapper3.class);
    //job2.setCombinerClass(MyCombiner.class);
    job3.setPartitionerClass(MyPartitioner3.class);
    job3.setReducerClass(MyReducer3.class);

    // Delete the output directory if it exists already.
    Path outputDir3 = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir3, true);

    long startTime3 = System.currentTimeMillis();
    job3.waitForCompletion(true);
    LOG.info("Job3 Finished in " + (System.currentTimeMillis() - startTime3) / 1000.0 + " seconds");
    
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}