import java.io.IOException;
import java.util.Iterator;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;

public class ExtractHourlyCountsEgypt extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractHourlyCountsEgypt.class);

  // Mapper: emits (token, 1) for every word occurrence.
  private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Reuse objects to save overhead of object creation.
    private final static IntWritable ONE = new IntWritable(1);
    private final static Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
    	String line = ((Text) value).toString();
    	String[] terms =line.split("\\t+");
    	int flag=0;
    	if(terms.length>=4)
    	{	
    		//System.out.print(terms[2]+"\n");
    		String[] date = terms[1].split("\\s+");
    		System.out.print(date[1]+"\n");
    		//System.out.print(date[2]+"\n");
    		if(date[1].equals("Jan"))
    		{
    			System.out.print(date[2]+"\n");
    			if(Integer.parseInt(date[2])>=23)
    			{
    				String[] tweet=terms[3].split("\\s+");
    				for(int i=0;i<tweet.length;i++)
    				{
    					if(tweet[i].matches(".*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*"))
    					{
    	    				System.out.print(date[3]+"\n");
    	    				String time="1/"+date[2]+" "+date[3].substring(0, 2);
    	    				WORD.set(time);
    	    				context.write(WORD, ONE);
    						break;
    					}
    				}
	
    			}
    		}
    		else if(date[1].equals("Feb"))
    		{
    			if(Integer.parseInt(date[2])<=8)
    			{
    				String[] tweet=terms[3].split("\\s+");
    				for(int i=0;i<tweet.length;i++)
    				{
    					if(tweet[i].matches(".*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*"))
    					{
    	    				System.out.print(date[3]+"\n");
    	    				String time="2/"+date[2]+" "+date[3].substring(0, 2);
    	    				WORD.set(time);
    	    				context.write(WORD, ONE);
    						break;
    					}
    				}
    			}
    		}
    	}
      //for (int i=0;i<terms.length;i++) {
      //  WORD.set(terms[i]);
      //  context.write(WORD, ONE);
      //}
    }
  }

  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // Reuse objects.
    private final static IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public ExtractHourlyCountsEgypt() {}

  //private static final String INPUT = "input";
  //private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

   // options.addOption(OptionBuilder.withArgName("path").hasArg()
    //    .withDescription("input path").create(INPUT));
    //options.addOption(OptionBuilder.withArgName("path").hasArg()
    //    .withDescription("output path").create(OUTPUT));
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

    /*if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }*/

    String inputPath = "/user/shared/tweets2011/tweets2011.txt";  //cmdline.getOptionValue(INPUT);
    String outputPath = "skyqu-egypt/";  // cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + ExtractHourlyCountsEgypt.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(ExtractHourlyCountsEgypt.class.getSimpleName());
    job.setJarByClass(ExtractHourlyCountsEgypt.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
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
    ToolRunner.run(new ExtractHourlyCountsEgypt(), args);
  }
}