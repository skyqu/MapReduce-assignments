
/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

//package edu.umd.cloud9.example.pagerank;

import java.io.IOException;
import java.util.Arrays;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.map.String2IntOpenHashMapWritable;
import edu.umd.cloud9.io.pair.PairOfFloats;
import edu.umd.cloud9.io.pair.PairOfIntString;
import edu.umd.cloud9.io.pair.PairOfStrings;
//import pagerank.PageRankNodeMultiSrc;

//import pagerank.PageRankNodeMultiSrc;

/**
 * <p>
 * Driver program that takes a plain-text encoding of a directed graph and builds corresponding
 * Hadoop structures for representing the graph.
 * </p>
 *
 */
public class RecordExtracting extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RecordExtracting.class);

  //private static final String NODE_CNT_FIELD = "node.cnt";
  private static final String NODE_SRC="spamming users";

  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, Text> {
    private static final IntWritable nid = new IntWritable();
    //private static final PageRankNodeMultiSrc node = new PageRankNodeMultiSrc();
    private static String SrcNode;
    private static int[] src ;
    private static final String2IntOpenHashMapWritable MAP = new String2IntOpenHashMapWritable();
    private static final PairOfStrings UIDPID = new PairOfStrings();
    private static final Text RateComment = new Text();
    @Override
    public void setup(Mapper<LongWritable, Text, PairOfStrings, Text>.Context context) {
      //int n = context.getConfiguration().getInt(NODE_CNT_FIELD, 0);
      SrcNode = context.getConfiguration().get(NODE_SRC);
      String[] sources = SrcNode.split(",");
      MAP.clear();
      for(int i=0;i<sources.length;i++)
      {
    	  MAP.put(sources[i], 1);
      }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
    	
        String line = value.toString();
        String[] terms = line.split("\t");
        if (terms.length > 7) {
          String uid = terms[0].trim();
          String pid = terms[1].trim();
          int rate = Integer.parseInt(terms[5].trim().substring(0, 1));
          String comment = rate+"\n"+terms[6]+" "+terms[7]+"\n"; 
          //if the ID exists on the list then emit;
          if(MAP.containsKey(terms[0]))
          {
          	UIDPID.set(uid,pid);
          	RateComment.set(comment);
          	context.write(UIDPID, RateComment);
          }
        }

      
    }
  }
  
  protected static class MyPartitioner extends Partitioner<PairOfStrings, Text> {
  @Override
  public int getPartition(PairOfStrings key, Text value, int numReduceTasks) {
  		return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }
}
  
  
  private static class MyReducer extends Reducer<PairOfStrings, Text, PairOfStrings, Text> {

	    // Reuse objects.
	    private final static PairOfIntString RID = new PairOfIntString();
	    //private final static ArrayListWritable<Text> List = new ArrayListWritable<Text> ();
	    private final static Text first= new Text();

	    @Override
	    public void reduce(PairOfStrings key, Iterable<Text> values, Context context)
	        throws IOException, InterruptedException {
	      // Sum up values.
	      //List.clear();
	      String comments=new String();
	      Iterator<Text> iter = values.iterator();
	      int counter=0;
	      if(iter.hasNext())
	      {
	    	  comments=iter.next().toString();      //buffer the first value of the current PID UID pairs
	    	  counter++;
	      }
	      while (iter.hasNext()) 					// if there are additional comments, start emit these key-value pair
	      {
	    	  comments=comments+iter.next().toString();
	    	  counter++;
	      }
	      if(counter>1)
	      {
	    	  //List.add(first);
	    	  first.set(comments);
	    	  //key.set(" ", "  ");
	    	  context.write(key, first);
	      }
	    }
	  }

  public RecordExtracting() {}

  //private static final String INPUT = "input";
  //private static final String OUTPUT = "output";
  //private static final String NUM_NODES = "numNodes";
  private static final String SOURCES = "sources";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

   /* options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes").create(NUM_NODES));
    //parsing more than 1 integer later;*/
    options.addOption(OptionBuilder.withArgName("src").hasArg()
            .withDescription("spamming users").create(SOURCES));
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

   /* if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(NUM_NODES)||!cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }*/

    String inputPath = "xzzqskfinal/reviewsNew.txt";//cmdline.getOptionValue(INPUT);
    String outputPath = "xzzqskfinal/SpammingRecord";//cmdline.getOptionValue(OUTPUT);
    //int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
    //Change to array later
    String src = cmdline.getOptionValue(SOURCES);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
            .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool name: " + RecordExtracting.class.getSimpleName());
    LOG.info(" - inputDir: " + inputPath);
    LOG.info(" - outputDir: " + outputPath);
    //LOG.info(" - numNodes: " + n);

    Configuration conf = getConf();
    
    conf.set("mapreduce.map.memory.mb", "2048");
    conf.set("mapreduce.map.java.opts", "-Xmx2048m");
    conf.set("mapreduce.reduce.memory.mb", "2048");
    conf.set("mapreduce.reduce.java.opts", "-Xmx2048m");
    //conf.setInt(NODE_CNT_FIELD, n);
    //more to be set later;
    conf.set(NODE_SRC, src);
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

    Job job = Job.getInstance(conf);
    job.setJobName(RatingSpamming.class.getSimpleName());
    job.setJarByClass(RecordExtracting.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(TextInputFormat.class);
    //job.setOutputFormatClass(TextOutputFormat.class);
    //job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(PairOfStrings.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(PairOfStrings.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);
    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RecordExtracting(), args);
  }
}
