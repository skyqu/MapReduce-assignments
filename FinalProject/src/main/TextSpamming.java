import java.io.IOException;
import java.util.ArrayList;
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
import edu.umd.cloud9.io.map.String2FloatOpenHashMapWritable;
import edu.umd.cloud9.io.map.String2IntOpenHashMapWritable;
import edu.umd.cloud9.io.pair.PairOfStringFloat;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.cloud9.io.pair.PairOfFloats;
import edu.umd.cloud9.io.pair.PairOfIntString;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.mapreduce.lib.input.NonSplitableSequenceFileInputFormat;

public class TextSpamming extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(TextSpamming.class);

  // Mapper: emits (token, 1) for every word occurrence.
  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, Text> {

    // Reuse objects to save overhead of object creation.
    private final static Text Comment = new Text();
    private final static PairOfStrings Pair = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      String UID = line.substring(0, 14);
      String PID = line.substring(15, 24);
      String comment = line.substring(26);
      Comment.set(comment);
      Pair.set(PID, UID);
      context.write(Pair, Comment); //(<PID, UID>,  <Comment>)
    }
  }

  private static class MyMapper2 extends Mapper<PairOfStrings, PairOfIntString, PairOfStrings, PairOfFloats> {

	    // Reuse objects to save overhead of object creation.
	    //private final static FloatWritable Evalue = new FloatWritable( );
	    private final static PairOfStrings BigramID = new PairOfStrings();
	    private final static PairOfFloats TF = new PairOfFloats();
	    private final static String2IntOpenHashMapWritable MAP = new String2IntOpenHashMapWritable();

	    @Override
	    public void map(PairOfStrings key, PairOfIntString value, Context context)
	        throws IOException, InterruptedException {
	      String UID = 	key.getRightElement();
	      String PID = 	key.getLeftElement();
	      int RID = value.getLeftElement();
	      String[] comments = value.getRightElement().split("\\s+");
	      for(int i=0;i<comments.length-1;i++)
	      {
	    	  String temp = comments[i]+","+comments[i+1];
	    	  if(!MAP.containsKey(temp))			//buffer all bigrams before emit
	    	  {
	    		  MAP.put(temp, 1);
	    	  }else{
	    		  MAP.increment(temp);
	    	  }
	      }
	      for(String2IntOpenHashMapWritable.Entry<String> e : MAP.object2IntEntrySet())
	      {
	    	  String bigram = e.getKey();
	    	  BigramID.set(bigram, "\0");                 //to get the DF
	    	  TF.set(RID,1);
	    	  context.write(BigramID, TF);
	    	  BigramID.set(bigram, PID+","+UID);
	    	  TF.set(RID,(float)MAP.get(bigram)/(float)(comments.length-1));
	    	  context.write(BigramID, TF);    //(<bigram as string; PID,UID as string>,<RID as int; TF as float>)
	      }
	    }
	  }
  
  private static class MyMapper3 extends Mapper<PairOfStrings, PairOfFloats, PairOfStringInt, PairOfStringFloat> {

	    // Reuse objects to save overhead of object creation.
	    private final static PairOfStringInt IDPair = new PairOfStringInt();
	    private final static PairOfStringFloat BigramPair = new PairOfStringFloat();

	    @Override
	    public void map(PairOfStrings key, PairOfFloats value, Context context)
	        throws IOException, InterruptedException {
	    	String PIDUID = key.getRightElement();
	    	String Bigram = key.getRightElement();
	    	float tfidf = value.getRightElement();
	    	int rid = (int)value.getLeftElement();
	    	IDPair.set(PIDUID, rid);
	    	BigramPair.set(Bigram, tfidf);
	    	context.write(IDPair, BigramPair);       //Put all id as key and <bigram, tfidf> as value
	    }
	  }
  
  private static class MyMapper4 extends Mapper<PairOfStringInt, String2FloatOpenHashMapWritable, Text, String2FloatOpenHashMapWritable> {

	    // Reuse objects to save overhead of object creation.
	    private final static Text PIDUID = new Text();
	    //private final static PairOfStringFloat BigramPair = new PairOfStringFloat();

	    @Override
	    public void map(PairOfStringInt key, String2FloatOpenHashMapWritable value, Context context)
	        throws IOException, InterruptedException {
	    	PIDUID.set(key.getLeftElement());
	    	context.write(PIDUID, value);			//RID is not necessary as the bigrams from one document are now regrouped
	    }
	  }
  
  private static class MyMapper5 extends Mapper<Text, FloatWritable, Text, FloatWritable> {

	    // Reuse objects to save overhead of object creation.
	    private final static Text UID = new Text();
	    //private final static PairOfStringFloat BigramPair = new PairOfStringFloat();

	    @Override
	    public void map(Text key, FloatWritable value, Context context)
	        throws IOException, InterruptedException {
	    	String[] IDs=key.toString().split(",");
	    	UID.set(IDs[1]);
	    	context.write(UID, value);			//RID is not necessary as the bigrams from one document are now regrouped
	    }
	  }
  
  protected static class MyPartitioner2 extends Partitioner<PairOfStrings, PairOfFloats> {
  @Override
  public int getPartition(PairOfStrings key, PairOfFloats value, int numReduceTasks) {
  		return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }
}
  
  protected static class MyPartitioner3 extends Partitioner<PairOfStringInt, PairOfStringFloat> {
  @Override
  public int getPartition(PairOfStringInt key, PairOfStringFloat value, int numReduceTasks) {
  		return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }
}
  
  
  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<PairOfStrings, Text, PairOfStrings, PairOfIntString> {

    // Reuse objects.
    private final static PairOfIntString RID = new PairOfIntString();
    private final static ArrayListWritable<IntWritable> List = new ArrayListWritable<IntWritable> ();
    private static String first =new String();


    @Override
    public void reduce(PairOfStrings key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      List.clear();
      Iterator<Text> iter = values.iterator();
      int counter=0;
      if(iter.hasNext())
      {
    	  first = iter.next().toString();
    	  counter++;
      }
      while (iter.hasNext()) 
      {
    	  RID.set(counter, iter.next().toString());
    	  context.write(key, RID);
    	  counter++;
      }
      if(counter>1)
      {
    	  RID.set(0, first);
    	  context.write(key, RID);
      }
    }
  }
  
  private static class MyReducer2 extends Reducer<PairOfStrings, PairOfFloats, PairOfStrings, PairOfFloats> {

	    // Reuse objects.
	    private static double IDF;
	    private static final long Dnum=100000000;        //NEED the HARDCODE VALUE
	    private static final PairOfFloats TFIDF = new PairOfFloats();

	    @Override
	    public void reduce(PairOfStrings key, Iterable<PairOfFloats> values, Context context)
	        throws IOException, InterruptedException {
	      // Sum up values.
	      Iterator<PairOfFloats> iter = values.iterator();
	      if(key.getRightElement().equals("\0"))
	      {
	    	  IDF=0;
	    	  while(iter.hasNext())
	    	  {
	    		  IDF=IDF+1;
	    	  }
	    	  IDF=Math.log(Dnum/IDF);
	      }else{
	    	  while (iter.hasNext()) {
	        	  float td = iter.next().getRightElement();
	        	  TFIDF.set(iter.next().getLeftElement(),(float)IDF*td);
		    	  context.write(key, TFIDF);
	    	  }
	      }
	      }
	  }
  
  private static class MyReducer3 extends Reducer<PairOfStringInt, PairOfStringFloat, PairOfStringInt, String2FloatOpenHashMapWritable> {

	    // Reuse objects.

	    private final static String2FloatOpenHashMapWritable MAP = new String2FloatOpenHashMapWritable();
	    private static PairOfStringFloat TEMP = new PairOfStringFloat();

	    @Override
	    public void reduce(PairOfStringInt key, Iterable<PairOfStringFloat> values, Context context)
	        throws IOException, InterruptedException {
	      // Sum up values.
	      Iterator<PairOfStringFloat> iter = values.iterator();
	     
	      while(iter.hasNext())
	      {
	    	TEMP=iter.next().clone();
	    	MAP.put(TEMP.getLeftElement(), TEMP.getRightElement());	 
	      }
	      context.write(key, MAP);
	     }
	  }
  
  private static class MyReducer4 extends Reducer<Text, String2FloatOpenHashMapWritable, Text, FloatWritable> {

	    // Reuse objects.

	    private final static String2FloatOpenHashMapWritable MAP = new String2FloatOpenHashMapWritable();
	    private static FloatWritable TEMP = new FloatWritable();
	    private final static int window=10;
	    private static ArrayList<String2FloatOpenHashMapWritable> List = new ArrayList<String2FloatOpenHashMapWritable>();

	    @Override
	    public void reduce(Text key, Iterable<String2FloatOpenHashMapWritable> values, Context context)
	        throws IOException, InterruptedException {
	      // Sum up values.
	      Iterator<String2FloatOpenHashMapWritable> iter = values.iterator();
	      double sim=0;
	      while(iter.hasNext())
	      {
	    	List.add(iter.next()); 
	      }
	      int cardV=List.size();
	      for(int i=0;i<cardV;i++)
	      {
	    	  float normi=0;
    		  for(String2FloatOpenHashMapWritable.Entry<String> e : List.get(i).object2FloatEntrySet())
    		  {
    			 normi=normi+List.get(i).get(e)*List.get(i).get(e);
    		  }
	    	  for(int j=i+1;j<cardV;j++)
	    	  {
	    		  float inner=0;
	    		  float normj=0;
	    		  for(String2FloatOpenHashMapWritable.Entry<String> e : List.get(j).object2FloatEntrySet())
	    		  {
	    			 if(List.get(i).containsKey(e))
	    			 {
	    				 inner=inner+List.get(i).get(e)*List.get(j).get(e);
	    			 }
	    			 normj=normj+List.get(j).get(e)*List.get(j).get(e);
	    		  }
	    		  sim=sim+inner/(Math.sqrt(normj)*Math.sqrt(normi));
	    	  }
	      }
	      sim=2*sim/(cardV*(cardV-1));
	      TEMP.set((float)sim);
	      context.write(key, TEMP);
	     }
	  }

  private static class MyReducer5 extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	    // Reuse objects.
	    private final static FloatWritable VALUE = new FloatWritable();
	    //private float marginal =0.0f;

	    @Override
	    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
	        throws IOException, InterruptedException {
	      // Sum up values.
	    	float sim=0;
	    	Iterator<FloatWritable> iter = values.iterator();
	    	while (iter.hasNext()){
	    		sim+=iter.next().get();
	    	}
	    	VALUE.set(sim);
	    	context.write(key, VALUE);
	    }
	  }
  
  /**
   * Creates an instance of this tool.
   */
  public TextSpamming() {}

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

    LOG.info("Tool: " + TextSpamming.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Configuration conf = getConf();
    Job job1 = Job.getInstance(conf);
    job1.setJobName(TextSpamming.class.getSimpleName());
    job1.setJarByClass(TextSpamming.class);

    job1.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job1, new Path(inputPath));
    FileOutputFormat.setOutputPath(job1, new Path(outputPath+"temp"));

    job1.setMapOutputKeyClass(PairOfStrings.class);
    job1.setMapOutputValueClass(Text.class);
    
    //job1.setOutputKeyClass(PairOfStrings.class);
    //job1.setOutputValueClass(PairOfIntString.class);
    
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);

    job1.setMapperClass(MyMapper.class);
    //job1.setPartitionerClass(MyPartitioner.class);
    job1.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath+"temp");
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    LOG.info("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    
    Job job2 = Job.getInstance(conf);
    job2.setJobName(TextSpamming.class.getSimpleName());
    job2.setJarByClass(TextSpamming.class);

    job2.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job2, new Path(outputPath+"temp"));
    FileOutputFormat.setOutputPath(job2, new Path(outputPath+"temp2"));

    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(PairOfFloats.class);
    
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(PairOfFloats.class);
    
    job2.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job2.setOutputFormatClass(SequenceFileOutputFormat.class);

    job2.setMapperClass(MyMapper2.class);
    job2.setPartitionerClass(MyPartitioner2.class);
    //job2.setCombinerClass(MyReducer2.class);
    job2.setReducerClass(MyReducer2.class);

    // Delete the output directory if it exists already.
    Path outputDir2 = new Path(outputPath+"temp2");
    FileSystem.get(conf).delete(outputDir2, true);

    long startTime2 = System.currentTimeMillis();
    job2.waitForCompletion(true);
    LOG.info("Job2 Finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");
    
    
    Job job3 = Job.getInstance(conf);
    job3.setJobName(TextSpamming.class.getSimpleName());
    job3.setJarByClass(TextSpamming.class);

    job3.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job3, new Path(outputPath+"temp2"));
    FileOutputFormat.setOutputPath(job3, new Path(outputPath+"temp3"));

    job3.setMapOutputKeyClass(PairOfStringInt.class);
    job3.setMapOutputValueClass(PairOfStringFloat.class);
    
    job3.setOutputKeyClass(PairOfStringInt.class);
    job3.setOutputValueClass(String2FloatOpenHashMapWritable.class);
    
    job3.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job3.setOutputFormatClass(SequenceFileOutputFormat.class);

    job3.setMapperClass(MyMapper3.class);
    job3.setPartitionerClass(MyPartitioner3.class);
    //job2.setCombinerClass(MyReducer2.class);
    job3.setReducerClass(MyReducer3.class);

    // Delete the output directory if it exists already.
    Path outputDir3 = new Path(outputPath+"temp3");
    FileSystem.get(conf).delete(outputDir3, true);

    long startTime3 = System.currentTimeMillis();
    job3.waitForCompletion(true);
    LOG.info("Job3 Finished in " + (System.currentTimeMillis() - startTime3) / 1000.0 + " seconds");
    
    Job job4 = Job.getInstance(conf);
    job4.setJobName(TextSpamming.class.getSimpleName());
    job4.setJarByClass(TextSpamming.class);

    job4.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job4, new Path(outputPath+"temp3"));
    FileOutputFormat.setOutputPath(job4, new Path(outputPath+"temp4"));

    job4.setMapOutputKeyClass(Text.class);
    job4.setMapOutputValueClass(String2FloatOpenHashMapWritable.class);
    
    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(FloatWritable.class);
    
    job4.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job4.setOutputFormatClass(SequenceFileOutputFormat.class);

    job4.setMapperClass(MyMapper4.class);
    //job4.setPartitionerClass(MyPartitioner3.class);
    //job2.setCombinerClass(MyReducer2.class);
    job4.setReducerClass(MyReducer4.class);

    // Delete the output directory if it exists already.
    Path outputDir4 = new Path(outputPath+"temp4");
    FileSystem.get(conf).delete(outputDir4, true);

    long startTime4 = System.currentTimeMillis();
    job4.waitForCompletion(true);
    LOG.info("Job4 Finished in " + (System.currentTimeMillis() - startTime4) / 1000.0 + " seconds");
    
    
    Job job5 = Job.getInstance(conf);
    job5.setJobName(TextSpamming.class.getSimpleName());
    job5.setJarByClass(TextSpamming.class);

    job5.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job5, new Path(outputPath+"temp4"));
    FileOutputFormat.setOutputPath(job5, new Path(outputPath));

    job5.setMapOutputKeyClass(Text.class);
    job5.setMapOutputValueClass(FloatWritable.class);
    
    job5.setOutputKeyClass(Text.class);
    job5.setOutputValueClass(FloatWritable.class);
    
    job5.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    //job5.setOutputFormatClass(SequenceFileOutputFormat.class);

    job5.setMapperClass(MyMapper5.class);
    //job4.setPartitionerClass(MyPartitioner3.class);
    //job2.setCombinerClass(MyReducer2.class);
    job5.setReducerClass(MyReducer5.class);

    // Delete the output directory if it exists already.
    Path outputDir5 = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir5, true);

    long startTime5 = System.currentTimeMillis();
    job4.waitForCompletion(true);
    LOG.info("Job5 Finished in " + (System.currentTimeMillis() - startTime5) / 1000.0 + " seconds");
    
    
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new TextSpamming(), args);
  }
}