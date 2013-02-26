import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
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
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfWritables;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfWritablesM;
import edu.umd.cloud9.io.array.ArrayListWritable;
import cern.colt.Arrays;

import java.util.Set;
import java.util.Stack;

public class InvertedIndex extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(InvertedIndex.class);

  // Mapper: emits (token, 1) for every word occurrence.
  private static class MyMapper extends Mapper<LongWritable, Text, Text, PairOfInts> {

    // Reuse objects to save overhead of object creation.
    //private final static IntWritable Count = new IntWritable(0);
    private final static PairOfInts FileCount = new PairOfInts();
    private final static Text WORD = new Text();
    //private static Hashtable<String,Integer> ht = new Hashtable<String,Integer>();  //in mapper combiner 
    private static int linenum=0;

    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      StringTokenizer itr = new StringTokenizer(line);
      Hashtable<String,Integer> ht = new Hashtable<String,Integer>();
      linenum=linenum+1;
      while (itr.hasMoreTokens()){
    	  String cur=itr.nextToken();
    	  if(!ht.containsKey(cur)&&cur.length()!=0)  
    	  {
    		  ht.put(cur, new Integer(1));
    	  }
    	  else if(cur.length()!=0){
    		  ht.put(cur,(Integer)ht.get(cur)+1);
    		  //System.out.print(cur+ht.get(cur)+"\n");
    	  }
    	  else{
    		  continue;
    	  }
      }
      //System.out.print(ht.toString()+"\n\n\n\n");
      for(Enumeration<String> e = ht.keys();e.hasMoreElements();){
  		String fkey=e.nextElement();
  		WORD.set(fkey);
  		FileCount.set((int)key.get(), ht.get(fkey));
  		//System.out.print(key.get()+"\n\n");
  		context.write(WORD,FileCount);   
     }
        
      
    }
    
  /*  @Override
    public void cleanup(Context context)
        throws IOException, InterruptedException {
    	for(Enumeration<String> e = ht.keys();e.hasMoreElements();){
    		String fkey=e.nextElement();
    		WORD.set(fkey);
    		ONE.set((Integer)ht.get(fkey));
    		context.write(WORD,ONE);   
       }
    }*/
  }
  


  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<Text, PairOfInts, Text, PairOfWritables<IntWritable,ArrayListWritable<PairOfInts>>> {

    // Reuse objects.
    private final static IntWritable SUM = new IntWritable();
    private final static ArrayListWritable<PairOfInts> ARRAY = new ArrayListWritable<PairOfInts>();
    //private final static PairOfWritables<IntWritable,ArrayListWritable<PairOfInts>> VALUE = new PairOfWritables<IntWritable,ArrayListWritable<PairOfInts>>();

    @Override
    public void reduce(Text key, Iterable<PairOfInts> values, Context context)
        throws IOException, InterruptedException {

      Iterator<PairOfInts> iter = values.iterator();
      int sumcount = 0;
      ARRAY.clear();
      while (iter.hasNext()) {
        sumcount ++;
        ARRAY.add(iter.next().clone());
      }
      Collections.sort(ARRAY);
      SUM.set(sumcount);
      context.write(key, new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(SUM, ARRAY));
    }
  }
  
  
  

  /**
   * Creates an instance of this tool.
   */
  public InvertedIndex() {}

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

    LOG.info("Tool: " + InvertedIndex.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(InvertedIndex.class.getSimpleName());
    job.setJarByClass(InvertedIndex.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfInts.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfWritables.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    //job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new InvertedIndex(), args);
  }
}