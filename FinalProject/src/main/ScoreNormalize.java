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

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.pair.PairOfFloats;
import edu.umd.cloud9.io.pair.PairOfWritables;

public class ScoreNormalize extends Configured implements Tool {
  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  
  public ScoreNormalize() {}
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
    
    String inPath = "xzzqskfinal/RSScore";//cmdline.getOptionValue(INPUT);
    String outPath = "xzzqskfinal/RSScoreN"; //cmdline.getOptionValue(OUTPUT);
   
  
    
    FileSystem fs = FileSystem.get(getConf());
    List<PairOfWritables<Text, PairOfFloats>> list = 
        SequenceFileUtils.readDirectory(new Path(inPath), fs, Integer.MAX_VALUE);
    
    double sum = 0;
    long count = 0;
    for (PairOfWritables<Text, PairOfFloats> pair : list) {
       sum += pair.getRightElement().getLeftElement();
       count++;
    }
    
    float mean = (float) (sum / count);
    sum = 0;
    for (PairOfWritables<Text, PairOfFloats> pair : list) {
      sum += Math.pow(pair.getRightElement().getLeftElement() - mean, 2);
    }
    
    float sqrtVar = (float) Math.sqrt(sum /count);
    @SuppressWarnings("deprecation")
	Writer writer = SequenceFile.createWriter(fs, getConf(), new Path(outPath),
      Text.class, PairOfFloats.class);
    PairOfFloats normalizedScore = new PairOfFloats();
    for (PairOfWritables<Text, PairOfFloats> pair : list) {
      String uid = pair.getLeftElement().toString();
      float score = pair.getRightElement().getLeftElement();
      float newScore = (score - mean) / sqrtVar;
      //System.out.println(uid + " " + newScore);
      normalizedScore.set(newScore, pair.getRightElement().getRightElement());
      writer.append(pair.getLeftElement(), normalizedScore);
    }
    System.out.println(mean);
    System.out.println(sqrtVar);
    
//    Text uid = new Text();
//    PairOfFloats value = new PairOfFloats();
//    double sum = 0;
//    long count = 0;
//    for (FileStatus f : fs.listStatus(new Path(inPath))) {
//      if (f.getPath().getName().contains("_"))
//        continue;
//      @SuppressWarnings("deprecation")
//      SequenceFile.Reader reader = new SequenceFile.Reader(fs, f.getPath(), config);
//      while (reader.next(uid, value)) {
//          count++;
//          sum += value.getLeftElement();
//      }
//      reader.close();
//    }
//    double mean = sum / count;
//    sum = 0;
//    for (FileStatus f : fs.listStatus(new Path(inPath))) {
//      if (f.getPath().getName().contains("_"))
//        continue;
//      @SuppressWarnings("deprecation")
//      SequenceFile.Reader reader = new SequenceFile.Reader(fs, f.getPath(), config);
//      while (reader.next(uid, value)) {
//          sum += Math.pow(value.getLeftElement() - mean, 2);
//      }      
//      reader.close();
//    }
//    sum /= count;
//    double sqrtVariance = Math.sqrt(sum);
//    @SuppressWarnings("deprecation")
//    Writer writer = SequenceFile.createWriter(fs, getConf(), new Path(outPath),
//        Text.class, PairOfFloats.class);
//    for (FileStatus f : fs.listStatus(new Path(inPath))) {
//      if (f.getPath().getName().contains("_"))
//        continue;
//      SequenceFile.Reader reader = new SequenceFile.Reader(fs, f.getPath(), config);
//      while (reader.next(uid, value)) {
//        value.set((float) ((value.getLeftElement() - mean) / sqrtVariance), value.getRightElement());
//        writer.append(uid, value);
//      }      
//      reader.close();
//    }
    writer.close();
    return 0;
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ScoreNormalize(), args);
  }
}
