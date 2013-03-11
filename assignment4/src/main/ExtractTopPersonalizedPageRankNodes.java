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

//import PairOfStringFloat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


//import PairsPMI.MyPartitioner;

import pagerank.PageRankNodeMultiSrc;
import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.io.pair.PairOfIntFloat;



public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
 // private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);
  private static final String TOP_PG = "top_pg";
  
  private static class MyMapper extends
      Mapper<IntWritable, PageRankNodeMultiSrc, PairOfIntFloat, IntWritable> {
    //private TopNScoredObjects<Integer> queue;
	  private static PairOfIntFloat KEY = new PairOfIntFloat();
	  //private static IntWritable VALUE = new IntWritable();
 /*@Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      queue = new TopNScoredObjects<Integer>(k);
    }*/

    @Override
    public void map(IntWritable nid, PageRankNodeMultiSrc node, Context context) throws IOException,
        InterruptedException {
    	int[] PGlist = node.getSrc();
    	HMapIFW PGmap = node.getPageRankM();
        for(int i=0;i<PGlist.length;i++)
        {
        	//in order to get largest in ascending order
        	KEY.set(PGlist[i], 1-PGmap.get(PGlist[i]));
        	context.write(KEY, nid);
        }
    }

    /*@Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();

      for (PairOfObjectFloat<Integer> pair : queue.extractAll()) {
        key.set(pair.getLeftElement());
        value.set(pair.getRightElement());
        context.write(key, value);
      }
    }*/
  }
  
  protected static class MyPartitioner extends Partitioner<PairOfIntFloat, IntWritable> {
	    @Override
	    public int getPartition(PairOfIntFloat key, IntWritable value, int numReduceTasks) {
	   
	    		return 0;
	    	
	    }
	  }

  private static class MyReducer extends
      Reducer<PairOfIntFloat, IntWritable, FloatWritable, IntWritable> {
    //private static TopNScoredObjects<Integer> queue;
	  //private PairOfIntFloat KEY= new PairOfIntFloat();
	  private int top;
	  private final static FloatWritable KEY =new FloatWritable();
	  private final static IntWritable VALUE =new IntWritable();
      //private static float intermediatePG1 = 0.0f; 
      //private static float intermediatePG2 = 0.0f;
      private static float intermediateSRC1 = 0.0f;
      private static float intermediateSRC2 = 1.0f;
      private static int flag=0;
    @Override
    public void setup(Context context) throws IOException {
    	top = context.getConfiguration().getInt(TOP_PG, 0);
    }

    @Override
    public void reduce(PairOfIntFloat PageRank, Iterable<IntWritable> iterable, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = iterable.iterator();
      //queue.add(nid.get(), iter.next().get());
      intermediateSRC1=PageRank.getLeftElement();
      if(intermediateSRC1!=intermediateSRC2)
      {
          //SysOut.add("\n"+"Source: "+PageRank.getKey()+"\n");
    	  KEY.set(0);
    	  VALUE.set(PageRank.getKey());
    	  context.write(KEY,VALUE);
          flag=0;
      }
      if(flag<top)
      {
    	  while(iter.hasNext())//&&flag<top)
      	  {
    		  int nodeid =iter.next().get();
    	      //SysOut.add(String.format("%.5f %d", 1-PageRank.getRightElement(),nodeid));
    		  //SysOut.add("\n");
        	  KEY.set(1-PageRank.getRightElement());
        	  VALUE.set(nodeid);
        	  context.write(KEY,VALUE);
    	      //System.out.print(str+"\n");
    		  flag++;
      	  }
      }
      intermediateSRC2=intermediateSRC1;
     
    }

    /*@Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();

      for (PairOfObjectFloat<Integer> pair : queue.extractAll()) {
        key.set(pair.getLeftElement());
        value.set(pair.getRightElement());
        context.write(key, value);
      }
    }*/
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  //private static String SysOut=new String();
  //private static Queue<String> SysOut=new LinkedList<String>();
  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SRC = "sources";

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
        .withDescription("top n").create(TOP));
    options.addOption(OptionBuilder.withArgName("src").hasArg()
            .withDescription("source node").create(SRC));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(TOP)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = "abc";//cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));

    //LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    //LOG.info(" - input: " + inputPath);
    //LOG.info(" - output: " + outputPath);
    //LOG.info(" - top: " + n);

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt(TOP_PG, n);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(PairOfIntFloat.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setPartitionerClass(MyPartitioner.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);
    
    InputStream fis=new FileInputStream(outputPath+"/part-r-00000");
    BufferedReader br=new BufferedReader(new InputStreamReader(fis));
    String s;
    float    key;
    int		value;
    while((s=br.readLine())!=null)
    {
    	String[] sources=s.split("\\s+");
    	key=Float.parseFloat(sources[0]);
    	value=Integer.parseInt(sources[1]);
    	if(key==0.0f)
    	{
    		System.out.print("\n"+"Source: "+value+"\n");
    	}
    	else{
    		System.out.print(String.format("%.5f %d", key,value)+"\n");
    	}
    }
    
    
    
    //while(!SysOut.isEmpty())
    //{
    //	System.out.print(SysOut.poll());
    //}

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
}
