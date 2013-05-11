import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.cloud9.io.pair.PairOfFloats;

public class RatingSpamming extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RatingSpamming.class);

  /*
   * Mapper1 reads records from the review dataset and emits <(member id, product id), rating> as
   * key-value pairs.
   */
  private static class MyMapper1 extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    private final static IntWritable Rate = new IntWritable();
    private final static PairOfStrings Pair = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String line = value.toString();
      String[] terms = line.split("\t");
      if (terms.length > 5) {
        String uid = terms[0].trim();
        String pid = terms[1].trim();
        int rate = Integer.parseInt(terms[5].trim().substring(0, 1));
        Rate.set(rate);
        Pair.set(pid, uid);
        context.write(Pair, Rate);
        Pair.set(pid, "\0");
        context.write(Pair, Rate);
      }
    }
  }

  private static class MyMapper2 extends Mapper<PairOfStrings, PairOfFloats, Text, PairOfFloats> {
    private final static Text UID = new Text();
    @Override
    public void map(PairOfStrings key, PairOfFloats value, Context context) throws IOException,
        InterruptedException {
      UID.set(key.getRightElement());
      context.write(UID, value);
    }
  }

  protected static class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> {
    @Override
    public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private static class MyReducer1 extends
      Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfFloats> {
    private final static PairOfFloats Evalue = new PairOfFloats();
    private final static ArrayList<Integer> List = new ArrayList<Integer>();
    private static float avgRate;

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      List.clear();
      Iterator<IntWritable> iter = values.iterator();
      if (key.getRightElement().equals("\0")) {
        int count = 0;
        avgRate = 0;
        String product = key.getLeftElement();
        while (iter.hasNext()) {
          avgRate += iter.next().get();
          count++;
        }
        avgRate = avgRate / count;
      } else {
        while (iter.hasNext()) {
          List.add(iter.next().get());
        }
        if (List.size() > 1) {
          int similarity = 0;
          float rateDev = 0;
          float rateSum = 0;
          for (int i = 0; i < List.size(); i++) {
            rateSum += List.get(i); 
            for (int j = i + 1; j < List.size(); j++) {
              similarity += Math.abs((List.get(i) - List.get(j)) / 5);
            }
          }
          //We get the average of a user's ratings towards a product.
          //We divide the rating by 5 in order to make it between 0 and 1.
          rateSum /= 5 * List.size();
          rateDev = Math.abs(avgRate - rateSum);
          similarity = 1 - 2 * similarity / (List.size() * (List.size() - 1));
          Evalue.set(similarity * List.size(), rateDev); // (Spamming rate, Rate deviation)
          context.write(key, Evalue);
        }
      }
    }
  }

  private static class MyReducer2 extends Reducer<Text, PairOfFloats, Text, PairOfFloats> {
    private final static PairOfFloats SUM = new PairOfFloats();

    @Override
    public void reduce(Text key, Iterable<PairOfFloats> values, Context context)
        throws IOException, InterruptedException {
      Iterator<PairOfFloats> iter = values.iterator();
      float sum1 = 0;
      float sum2 = 0;
      int counter=0;
      while (iter.hasNext()) {
        PairOfFloats next = iter.next();
        sum1 += next.getLeftElement();
        sum2 += next.getRightElement();
        counter ++;
      }
      sum2=sum2/counter;
      SUM.set(sum1, sum2);
      context.write(key, SUM);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public RatingSpamming() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path")
        .create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path")
        .create(OUTPUT));
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

    String inputPath = "xzzqskfinal/reviewsNew.txt";          //cmdline.getOptionValue(INPUT);
    String outputPath ="xzzqskfinal/RSScore";         //cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
        .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + RatingSpamming.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Configuration conf = getConf();
    
    conf.set("mapreduce.map.memory.mb", "2048");
    conf.set("mapreduce.map.java.opts", "-Xmx2048m");
    conf.set("mapreduce.reduce.memory.mb", "2048");
    conf.set("mapreduce.reduce.java.opts", "-Xmx2048m");
    
    Job job1 = Job.getInstance(conf);
    job1.setJobName(RatingSpamming.class.getSimpleName());
    job1.setJarByClass(RatingSpamming.class);

    job1.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job1, new Path(inputPath));
    FileOutputFormat.setOutputPath(job1, new Path(outputPath + "temp"));
    
    job1.setMapOutputKeyClass(PairOfStrings.class);
    job1.setMapOutputValueClass(IntWritable.class);
    
    
    job1.setOutputKeyClass(PairOfStrings.class);
    job1.setOutputValueClass(PairOfFloats.class);
    
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);

    job1.setMapperClass(MyMapper1.class);
    job1.setPartitionerClass(MyPartitioner.class);
    job1.setReducerClass(MyReducer1.class);

    // Delete the output directory if it exists already.
    //Path outputDir = new Path(outputPath);
    //FileSystem.get(conf).delete(outputDir, true);
    Path outputDir = new Path(outputPath + "temp");
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    LOG.info("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    Job job2 = Job.getInstance(conf);
    job2.setJobName(RatingSpamming.class.getSimpleName());
    job2.setJarByClass(RatingSpamming.class);

    job2.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job2, new Path(outputPath + "temp"));
    FileOutputFormat.setOutputPath(job2, new Path(outputPath));

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(PairOfFloats.class);
    
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(PairOfFloats.class);

    job2.setInputFormatClass(SequenceFileInputFormat.class);
    //job2.setOutputFormatClass(TextOutputFormat.class);
    job2.setOutputFormatClass(SequenceFileOutputFormat.class);

    job2.setMapperClass(MyMapper2.class);
    job2.setReducerClass(MyReducer2.class);

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