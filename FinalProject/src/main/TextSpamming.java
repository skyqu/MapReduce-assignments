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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import edu.umd.cloud9.io.map.String2FloatOpenHashMapWritable;
import edu.umd.cloud9.io.map.String2IntOpenHashMapWritable;
import edu.umd.cloud9.io.pair.PairOfIntFloat;
import edu.umd.cloud9.io.pair.PairOfStringFloat;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.cloud9.io.pair.PairOfFloats;
import edu.umd.cloud9.io.pair.PairOfIntString;
import edu.umd.cloud9.mapreduce.lib.input.NonSplitableSequenceFileInputFormat;

public class TextSpamming extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(TextSpamming.class);

  // Mapper: pack user id and product id into key, and comment into value
  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, Text> {

    // Reuse objects to save overhead of object creation.
    private final static Text COMMENT = new Text();
    private final static PairOfStrings PAIR = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {

      String line = value.toString();
      String[] terms = line.split("\t");
      if (terms.length > 7) {
        String uid = terms[0].trim();
        String pid = terms[1].trim();
        String comment = terms[6] + " " + terms[7];
        comment = comment.trim();
        COMMENT.set(comment);
        PAIR.set(pid, uid);
        context.write(PAIR, COMMENT);
      } // (<PID, UID>, <Comment>)
    }
  }

  // Mapper: emit Bigrams of each comment
  private static class MyMapper2 extends
      Mapper<PairOfStrings, PairOfIntString, PairOfStrings, PairOfIntFloat> {
    private final static PairOfStrings BIGRAM = new PairOfStrings();
    private final static PairOfIntFloat TF = new PairOfIntFloat();
    private final static String2IntOpenHashMapWritable MAP = new String2IntOpenHashMapWritable();

    @Override
    public void map(PairOfStrings key, PairOfIntString value, Context context) throws IOException,
        InterruptedException {
      MAP.clear();
      String uid = key.getRightElement();
      String pid = key.getLeftElement();
      int rid = value.getLeftElement();
      // need to rewrite the split
      String[] comments = value.getRightElement().split("[^a-zA-Z]+");
      for (int i = 0; i < comments.length - 1; i++) {
        if (!comments[i].equals("") && !comments[i+1].equals("")) {
          String temp = comments[i].toLowerCase() + "," + comments[i + 1].toLowerCase();
          if (!MAP.containsKey(temp)) { // buffer all bigrams before emit
            MAP.put(temp, 1);
          } else {
            MAP.increment(temp);
          }
        }
      }

      for (String2IntOpenHashMapWritable.Entry<String> e : MAP.object2IntEntrySet()) {
        String bigram = e.getKey();
        BIGRAM.set(bigram, "*");
        TF.set(rid, 1);
        context.write(BIGRAM, TF);
        BIGRAM.set(bigram, pid + ',' + uid);
        TF.set(rid, (float) e.getValue() / (float) (comments.length - 1));
        context.write(BIGRAM, TF); // (<bigram as string; PID,UID as string>,<RID as int; TF as
                                   // float>)
      }
    }
  }

  // Emit ID Triples and Bigrams
  private static class MyMapper3 extends
      Mapper<PairOfStrings, PairOfIntFloat, PairOfStringInt, PairOfStringFloat> {

    // Reuse objects to save overhead of object creation.
    private final static PairOfStringInt IDPAIR = new PairOfStringInt();
    private final static PairOfStringFloat BIGRAMTFIDF = new PairOfStringFloat();

    @Override
    public void map(PairOfStrings key, PairOfIntFloat value, Context context) throws IOException,
        InterruptedException {
      String piduid = key.getRightElement();
      String bigram = key.getLeftElement();
      float tfidf = value.getRightElement();
      int rid = value.getLeftElement();
      IDPAIR.set(piduid, rid);
      BIGRAMTFIDF.set(bigram, tfidf);
      context.write(IDPAIR, BIGRAMTFIDF); // <<piduid, rid>, <bigram, tfidf>>
    }
  }

  private static class MyMapper4
      extends
      Mapper<PairOfStringInt, String2FloatOpenHashMapWritable, Text, String2FloatOpenHashMapWritable> {
    private final static Text PIDUID = new Text();

    // private final static PairOfStringFloat BigramPair = new PairOfStringFloat();

    @Override
    public void map(PairOfStringInt key, String2FloatOpenHashMapWritable value, Context context)
        throws IOException, InterruptedException {
      PIDUID.set(key.getLeftElement());
      context.write(PIDUID, value); // RID is not necessary as the bigrams from one document are now
                                    // regrouped
    }
  }

  private static class MyMapper5 extends Mapper<Text, FloatWritable, Text, FloatWritable> {
    private final static Text UID = new Text();

    // private final static PairOfStringFloat BigramPair = new PairOfStringFloat();

    @Override
    public void map(Text key, FloatWritable value, Context context) throws IOException,
        InterruptedException {
      String[] IDs = key.toString().split(",");
      UID.set(IDs[1]);
      context.write(UID, value);
    }
  }

  protected static class MyPartitioner2 extends Partitioner<PairOfStrings, PairOfIntFloat> {
    @Override
    public int getPartition(PairOfStrings key, PairOfIntFloat value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  // Reducer: rule out all UID PID pairs which has only one comment entrance
  private static class MyReducer extends
      Reducer<PairOfStrings, Text, PairOfStrings, PairOfIntString> {
    // The left element is the sequential number of the review with the same uid and pid, and
    // the right element is the review text.
    private final static PairOfIntString TAGANDTEXT = new PairOfIntString();

    @Override
    public void reduce(PairOfStrings key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      String firstValue = "";
      Iterator<Text> iter = values.iterator();
      int counter = 0;
      // buffer the first value
      if (iter.hasNext()) {
        firstValue = iter.next().toString();
        counter++;
      }
      // if there are additional comments, start emit these key-value pair
      while (iter.hasNext()) {
        TAGANDTEXT.set(counter, iter.next().toString());
        context.write(key, TAGANDTEXT);
        counter++;
      }
      // if there are more than one comments for this key, we emit the first.
      if (counter > 1) {
        TAGANDTEXT.set(0, firstValue);
        context.write(key, TAGANDTEXT);
      }
    }
  }

  // compute TFIDF
  private static class MyReducer2 extends
      Reducer<PairOfStrings, PairOfIntFloat, PairOfStrings, PairOfIntFloat> {
    private static final long DNUM = 5838923; // total documents number
    private static final PairOfIntFloat TFIDF = new PairOfIntFloat();
    private static double IDF;

    @Override
    public void reduce(PairOfStrings key, Iterable<PairOfIntFloat> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<PairOfIntFloat> iter = values.iterator();
      if (key.getRightElement().equals("*")) {
        IDF = 0;
        while (iter.hasNext()) {
          IDF = IDF + 1;
          iter.next();
        }
        IDF = Math.log((double) DNUM / IDF);
      } else {
        while (iter.hasNext()) {
          PairOfIntFloat next = iter.next();
          TFIDF.set(next.getLeftElement(), (float) IDF * next.getRightElement()); // rid and tfidf
          context.write(key, TFIDF); // <<bigram, piduid>, <rid, tfidf>>
        }
      }
    }
  }

  // get a list of bigram tfidf for each review
  private static class MyReducer3 extends
      Reducer<PairOfStringInt, PairOfStringFloat, PairOfStringInt, String2FloatOpenHashMapWritable> {
    private final static String2FloatOpenHashMapWritable MAP = new String2FloatOpenHashMapWritable();

    @Override
    public void reduce(PairOfStringInt key, Iterable<PairOfStringFloat> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<PairOfStringFloat> iter = values.iterator();
      MAP.clear();
      while (iter.hasNext()) {
        PairOfStringFloat next = iter.next();
        MAP.put(next.getLeftElement(), next.getRightElement());
      }
      context.write(key, MAP);
    }
  }

  // Compute the cosine distance of tdidf vector for reviews from same uid towards same product
  private static class MyReducer4 extends
      Reducer<Text, String2FloatOpenHashMapWritable, Text, FloatWritable> {
    // private final static String2FloatOpenHashMapWritable MAP = new
    // String2FloatOpenHashMapWritable();
    private static FloatWritable SIMILARITY = new FloatWritable();
    // private final static int window=10;
    private static ArrayList<String2FloatOpenHashMapWritable> tfidfVectorList = new ArrayList<String2FloatOpenHashMapWritable>();

    @Override
    public void reduce(Text key, Iterable<String2FloatOpenHashMapWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<String2FloatOpenHashMapWritable> iter = values.iterator();
      float cosineSimilarity = 0;
      tfidfVectorList.clear();
      // buffer all the bigram-sequence of the same PID UID pairs together
      while (iter.hasNext()) {
        String2FloatOpenHashMapWritable next = (String2FloatOpenHashMapWritable) iter.next().clone();
        tfidfVectorList.add(next);
      }
      int counteri=0;
      int counterj=0;
      // now loop around the array list
      for (int i = 0; i < tfidfVectorList.size(); i++) {
        float normi = 0;
        // for every entry compute the L2 norm
        for (String2FloatOpenHashMapWritable.Entry<String> e : tfidfVectorList.get(i)
            .object2FloatEntrySet()) {
          normi += (float) Math.pow(e.getValue(), 2);
        }
        if(normi==0)
        {
        	counteri++;
        	continue;
        }

        // the inner loop takes every entry after i and computes inner product and L2 norm
        for (int j = i + 1; j < tfidfVectorList.size(); j++) {
          float innerProduct = 0;
          float normj = 0;
          for (String2FloatOpenHashMapWritable.Entry<String> e : tfidfVectorList.get(j)
              .object2FloatEntrySet()) {
            // L2 norm of j
            normj += (float) Math.pow(e.getValue(), 2);
            // if i also has the key, this key entry contributes to inner product
            // it could be better if you get the vector sorted according to bigram order beforehand
            if (tfidfVectorList.get(i).containsKey(e.getKey())) {
              innerProduct += tfidfVectorList.get(i).get(e.getKey()) * e.getValue();
            }
          }
          if(normj==0)
          {
        	  counterj++;
        	  continue;
          }
          // sum over all the cosine distances
          cosineSimilarity += innerProduct / (Math.sqrt(normj) * Math.sqrt(normi));
        }
      }

      float averageSimilarityTimesCount = 2 * cosineSimilarity / (tfidfVectorList.size()-1-counteri);
      SIMILARITY.set(averageSimilarityTimesCount);
      context.write(key, SIMILARITY); // <piduid, averageSimilarityTimesCount>
    }
  }

  private static class MyReducer5 extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private final static FloatWritable VALUE = new FloatWritable();

    // private float marginal =0.0f;

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      float sim = 0;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sim += iter.next().get();
      }
      if(!(sim>0&&sim<100000)){sim=0;}
      VALUE.set(sim);
      context.write(key, VALUE); // <uid, score>
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public TextSpamming() {
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

   /* if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }*/

    String inputPath = "xzzqskfinal/reviewsNew.txt";
    //String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = "xzzqskfinal/TSScore"; // cmdline.getOptionValue(OUTPUT);
    //String inputPath = cmdline.getOptionValue(INPUT);
    //String outputPath = cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
        .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + TextSpamming.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Configuration conf = getConf();

    conf.set("mapreduce.map.memory.mb", "2048");
    conf.set("mapreduce.map.java.opts", "-Xmx2048m");
    conf.set("mapreduce.reduce.memory.mb", "2048");
    conf.set("mapreduce.reduce.java.opts", "-Xmx2048m");

    Job job1 = Job.getInstance(conf);
    job1.setJobName(TextSpamming.class.getSimpleName());
    job1.setJarByClass(TextSpamming.class);

    job1.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job1, new Path(inputPath));
    FileOutputFormat.setOutputPath(job1, new Path(outputPath + "temp"));

    job1.setMapOutputKeyClass(PairOfStrings.class);
    job1.setMapOutputValueClass(Text.class);

    job1.setOutputKeyClass(PairOfStrings.class);
    job1.setOutputValueClass(PairOfIntString.class);

    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
//    job1.setOutputFormatClass(TextOutputFormat.class);
    job1.setMapperClass(MyMapper.class);
    job1.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath + "temp");
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    LOG.info("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    Job job2 = Job.getInstance(conf);
    job2.setJobName(TextSpamming.class.getSimpleName());
    job2.setJarByClass(TextSpamming.class);

    job2.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job2, new Path(outputPath + "temp/part*"));
    FileOutputFormat.setOutputPath(job2, new Path(outputPath + "temp2"));

    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(PairOfIntFloat.class);

    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(PairOfIntFloat.class);

    job2.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job2.setOutputFormatClass(SequenceFileOutputFormat.class);
    job2.setMapperClass(MyMapper2.class);
    job2.setPartitionerClass(MyPartitioner2.class);
    job2.setReducerClass(MyReducer2.class);

    // Delete the output directory if it exists already.
    Path outputDir2 = new Path(outputPath + "temp2");
    FileSystem.get(conf).delete(outputDir2, true);

    long startTime2 = System.currentTimeMillis();
    job2.waitForCompletion(true);
    LOG.info("Job2 Finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");
    //Delete outputDir1 to save space since it will not be used anymore.
    FileSystem.get(conf).delete(outputDir, true);

    Job job3 = Job.getInstance(conf);
    job3.setJobName(TextSpamming.class.getSimpleName());
    job3.setJarByClass(TextSpamming.class);

    job3.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job3, new Path(outputPath + "temp2/part*"));
    FileOutputFormat.setOutputPath(job3, new Path(outputPath + "temp3"));

    job3.setMapOutputKeyClass(PairOfStringInt.class);
    job3.setMapOutputValueClass(PairOfStringFloat.class);

    job3.setOutputKeyClass(PairOfStringInt.class);
    job3.setOutputValueClass(String2FloatOpenHashMapWritable.class);

    job3.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job3.setOutputFormatClass(SequenceFileOutputFormat.class);

    job3.setMapperClass(MyMapper3.class);
    job3.setReducerClass(MyReducer3.class);

    // Delete the output directory if it exists already.
    Path outputDir3 = new Path(outputPath + "temp3");
    FileSystem.get(conf).delete(outputDir3, true);

    long startTime3 = System.currentTimeMillis();
    job3.waitForCompletion(true);
    LOG.info("Job3 Finished in " + (System.currentTimeMillis() - startTime3) / 1000.0 + " seconds");
    FileSystem.get(conf).delete(outputDir2, true);
    
    Job job4 = Job.getInstance(conf);
    job4.setJobName(TextSpamming.class.getSimpleName());
    job4.setJarByClass(TextSpamming.class);

    job4.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job4, new Path(outputPath + "temp3/part*"));
    FileOutputFormat.setOutputPath(job4, new Path(outputPath + "temp4"));

    job4.setMapOutputKeyClass(Text.class);
    job4.setMapOutputValueClass(String2FloatOpenHashMapWritable.class);

    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(FloatWritable.class);

    job4.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job4.setOutputFormatClass(SequenceFileOutputFormat.class);

    job4.setMapperClass(MyMapper4.class);
    job4.setReducerClass(MyReducer4.class);

    // Delete the output directory if it exists already.
    Path outputDir4 = new Path(outputPath + "temp4");
    FileSystem.get(conf).delete(outputDir4, true);

    long startTime4 = System.currentTimeMillis();
    job4.waitForCompletion(true);
    LOG.info("Job4 Finished in " + (System.currentTimeMillis() - startTime4) / 1000.0 + " seconds");
    
    FileSystem.get(conf).delete(outputDir3, true);

    Job job5 = Job.getInstance(conf);
    job5.setJobName(TextSpamming.class.getSimpleName());
    job5.setJarByClass(TextSpamming.class);

    job5.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job5, new Path(outputPath + "temp4/part*"));
    FileOutputFormat.setOutputPath(job5, new Path(outputPath));

    job5.setMapOutputKeyClass(Text.class);
    job5.setMapOutputValueClass(FloatWritable.class);

    job5.setOutputKeyClass(Text.class);
    job5.setOutputValueClass(FloatWritable.class);

    job5.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job5.setOutputFormatClass(SequenceFileOutputFormat.class);
    //job5.setOutputFormatClass(TextOutputFormat.class);
    
    job5.setMapperClass(MyMapper5.class);
    job5.setReducerClass(MyReducer5.class);

    // Delete the output directory if it exists already.
    Path outputDir5 = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir5, true);
      
    long startTime5 = System.currentTimeMillis();
    job5.waitForCompletion(true);
    LOG.info("Job5 Finished in " + (System.currentTimeMillis() - startTime5) / 1000.0 + " seconds");
    
    //FileSystem.get(conf).delete(outputDir4, true);
    
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new TextSpamming(), args);
  }
}