import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.pair.PairOfFloats;
import edu.umd.cloud9.io.pair.PairOfWritables;

public class ScoreNormalize2 extends Configured implements Tool {
  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  
  public ScoreNormalize2() {}
  @Override
  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    
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
      formatter.printHelp(ScoreNormalize.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }*/
    
    String inPath = "xzzqskfinal/TSScore";///part-r-00000";//cmdline.getOptionValue(INPUT);
    String outPath = "xzzqskfinal/TSScoreN"; //cmdline.getOptionValue(OUTPUT);
      
    FileSystem fs = FileSystem.get(getConf());
    List<PairOfWritables<Text, FloatWritable>> list = 
        SequenceFileUtils.readDirectory(new Path(inPath), fs, Integer.MAX_VALUE);
    
    double sum = 0;
    long count = 0;
    //long i=Integer.parseInt(cmdline.getOptionValue(INPUT));
    for (PairOfWritables<Text, FloatWritable> pair : list) {
       //if(i>=0){
       //System.out.print(sum+"\n");}
       //i--;
    	if(pair.getRightElement().get()<10000&&pair.getRightElement().get()>=0)
    	{
    		sum = sum+ (double) pair.getRightElement().get();
    		count++;
    	}
       //if(i==-1)
       //{ System.out.print(pair.getRightElement().get()+"\n");}
       //count++;
    }
    //System.out.print(i+"\n");
    System.out.print(count+"\n");
    
    float mean = (float) (sum / count);
    sum = 0;
    for (PairOfWritables<Text, FloatWritable> pair : list) {
    if(pair.getRightElement().get()<10000&&pair.getRightElement().get()>=0)
    {
      sum += Math.pow(pair.getRightElement().get() - mean, 2);}
    }
    
    float sqrtVar = (float) Math.sqrt(sum /count);
    @SuppressWarnings("deprecation")
	Writer writer = SequenceFile.createWriter(fs, getConf(), new Path(outPath),
      Text.class, FloatWritable.class);
    FloatWritable normalizedScore = new FloatWritable();
    for (PairOfWritables<Text, FloatWritable> pair : list) {
      String uid = pair.getLeftElement().toString();
      float score = pair.getRightElement().get();
      float newScore = (score - mean) / sqrtVar;
      //System.out.println(uid + " " + newScore);
      normalizedScore.set(newScore);
      writer.append(pair.getLeftElement(), normalizedScore);
    }
    writer.close();
    System.out.println(mean);
    System.out.println(sqrtVar);
    

    return 0;
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ScoreNormalize2(), args);
  }
}

