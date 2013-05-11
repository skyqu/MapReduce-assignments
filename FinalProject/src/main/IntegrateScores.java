import java.io.Writer;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.pair.PairOfFloats;
import edu.umd.cloud9.io.pair.PairOfWritables;


public class IntegrateScores extends Configured implements Tool {
  private static final String INPUT1 = "input1";
  private static final String INPUT2 = "input2";
  private static final String OUTPUT = "output";
  public IntegrateScores() {}
  @SuppressWarnings("static-access")
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT1));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT2));
//    options.addOption(OptionBuilder.withArgName("path").hasArg()
//        .withDescription("output path").create(OUTPUT));
    
    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

  /*  if (!cmdline.hasOption(INPUT1) || !cmdline.hasOption(INPUT2)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(ScoreNormalize.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }*/
    
    String inPath1 = "xzzqskfinal/RSScoreN";//cmdline.getOptionValue(INPUT1);
    String inPath2 = "xzzqskfinal/TSScoreN";//cmdline.getOptionValue(INPUT2);
//    String outPath = cmdline.getOptionValue(OUTPUT);
    
    FileSystem fs = FileSystem.get(getConf());
    //rating spam score and deviation score
    List<PairOfWritables<Text, PairOfFloats>> list1 = 
        SequenceFileUtils.readDirectory(new Path(inPath1), fs, Integer.MAX_VALUE);
    //Text spam score
    List<PairOfWritables<Text, FloatWritable>> list2 = 
        SequenceFileUtils.readDirectory(new Path(inPath2), fs, Integer.MAX_VALUE);
    
    Hashtable<String, Float> hash = new Hashtable<String, Float>();
    for (PairOfWritables<Text, FloatWritable> pair : list2) {
      hash.put(pair.getLeftElement().toString(), pair.getRightElement().get() * 0.75f);
    }
    
    for (PairOfWritables<Text, PairOfFloats> pair: list1) {
      String key = pair.getLeftElement().toString();
      float deviation = pair.getRightElement().getRightElement();
      float ratingSpamScore = pair.getRightElement().getLeftElement();
      if (!hash.containsKey(key)) {
        hash.put(key, ratingSpamScore * 0.25f + deviation * 0.0f);
      } else {
        hash.put(key, hash.get(key) + ratingSpamScore * 0.25f + deviation * 0.0f);
      }
    }
    
    for (Entry<String, Float> entry : hash.entrySet()) {
    	if(entry.getValue()>Float.parseFloat(cmdline.getOptionValue(INPUT1)))
    	{ System.out.println(entry.getKey() + " " + entry.getValue());}
    }
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new IntegrateScores(), args);
  }
}
