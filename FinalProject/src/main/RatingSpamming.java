import java.io.IOException;
import java.util.Iterator;
import java.math.*;
import java.util.StringTokenizer;

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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.cloud9.io.pair.PairOfFloats;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.mapreduce.lib.input.NonSplitableSequenceFileInputFormat;

public class RatingSpamming extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RatingSpamming.class);

  // Mapper: emits (token, 1) for every word occurrence.
  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

    // Reuse objects to save overhead of object creation.
    private final static IntWritable Rate = new IntWritable();
    private final static PairOfStrings Pair = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      String[] terms =line.split(",");
      String UID = terms[0].substring(1);
      String PID = terms[1];
      int rate = Integer.parseInt(terms[2].substring(0,terms[2].length()-1));
      Rate.set(rate);
      Pair.set(PID, UID);
      context.write(Pair, Rate);
      Pair.set(PID, "\0");
      context.write(Pair, Rate);
    }
  }

  private static class MyMapper2 extends Mapper<PairOfStrings, PairOfFloats, Text, PairOfFloats> {

	    // Reuse objects to save overhead of object creation.
	    //private final static FloatWritable Evalue = new FloatWritable( );
	    private final static Text UID = new Text();

	    @Override
	    public void map(PairOfStrings key, PairOfFloats value, Context context)
	        throws IOException, InterruptedException {
	      UID.set(key.getRightElement());
	      context.write(UID, value);
	    }
	  }
  
  protected static class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> {
  @Override
  public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
  	if(!key.getLeftElement().equals("\0")){
  		return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  	}
  	else
  		return 0;
  }
}
  
  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfFloats> {

    // Reuse objects.
    private final static PairOfFloats Evalue = new PairOfFloats();
    private final static ArrayListWritable<IntWritable> List = new ArrayListWritable<IntWritable> ();
    private static String product =new String();
    private static int count;
    private static float avgrate;

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      List.clear();
      Iterator<IntWritable> iter = values.iterator();
      if(key.getRightElement().equals("\0"))
      {
          count=0;
          avgrate=0;
    	  product=key.getLeftElement();
    	  while(iter.hasNext())
    	  {
    		  avgrate=(float)iter.next().get();
    		  count++;
    	  }
    	  avgrate=avgrate/count;
      }
      else{
    	  while (iter.hasNext()) 
    	  {
    		  List.add(iter.next());
    	  }
    	  if(List.size()>1)
    	  {
    		  int ratespam=0;
    		  float ratedev=0;
    	  	  for (int i=0;i<List.size();i++)
    	  	  {
    	  		  ratedev = Math.abs(avgrate-(float)List.get(i).get());
    	  		  for (int j=i+1;j<List.size();j++)
    	  		  {
    	  			  ratespam = ratespam + Math.abs(List.get(i).get()-List.get(j).get());
    	  		  }
    	  	  }
    	  	  float sim = 1 - 2 * (float)ratespam/(float)(List.size()*(List.size()-1));
    	  	  Evalue.set(sim*List.size(),ratedev);   //(Spamming rate, Rate deviation)
    	  	  context.write(key, Evalue);
    	  }
      }
    }
  }
  
  private static class MyReducer2 extends Reducer<Text, PairOfFloats, Text, PairOfFloats> {

	    // Reuse objects.
	    private final static PairOfFloats SUM = new PairOfFloats();

	    @Override
	    public void reduce(Text key, Iterable<PairOfFloats> values, Context context)
	        throws IOException, InterruptedException {
	      // Sum up values.
	      Iterator<PairOfFloats> iter = values.iterator();
	      float sum1 = 0;
	      float sum2 = 0;
	      while (iter.hasNext()) {
	        sum1 += iter.next().getLeftElement();
	        sum2 += iter.next().getLeftElement();
	      }
	      SUM.set(sum1,sum2);
	      context.write(key, SUM);
	    }
	  }

  /**
   * Creates an instance of this tool.
   */
  public RatingSpamming() {}

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

    LOG.info("Tool: " + RatingSpamming.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Configuration conf = getConf();
    Job job1 = Job.getInstance(conf);
    job1.setJobName(RatingSpamming.class.getSimpleName());
    job1.setJarByClass(RatingSpamming.class);

    job1.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job1, new Path(inputPath));
    FileOutputFormat.setOutputPath(job1, new Path(outputPath+"temp"));

    job1.setOutputKeyClass(PairOfStrings.class);
    job1.setOutputValueClass(IntWritable.class);
    
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);

    job1.setMapperClass(MyMapper.class);
    job1.setPartitionerClass(MyPartitioner.class);
    job1.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    LOG.info("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    
    Job job2 = Job.getInstance(conf);
    job2.setJobName(RatingSpamming.class.getSimpleName());
    job2.setJarByClass(RatingSpamming.class);

    job2.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job2, new Path(outputPath+"temp"));
    FileOutputFormat.setOutputPath(job2, new Path(outputPath));

    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(PairOfFloats.class);
    
    job2.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job2.setOutputFormatClass(SequenceFileOutputFormat.class);

    job2.setMapperClass(MyMapper2.class);
    job2.setCombinerClass(MyReducer2.class);
    job2.setReducerClass(MyReducer2.class);

    // Delete the output directory if it exists already.
    Path outputDir2 = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir2, true);

    long startTime2 = System.currentTimeMillis();
    job2.waitForCompletion(true);
    LOG.info("Job2 Finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RatingSpamming(), args);
  }
}