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

//package edu.umd.cloud9.example.ir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfVInts;
import edu.umd.cloud9.io.pair.PairOfWritables;
import edu.umd.cloud9.util.fd.Int2IntFrequencyDistribution;
import edu.umd.cloud9.util.fd.Int2IntFrequencyDistributionEntry;

public class LookupPostingsCompressed extends Configured implements Tool {
  private static final String INDEX = "index";
  private static final String COLLECTION = "collection";

  private LookupPostingsCompressed() {}

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INDEX));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(COLLECTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INDEX) || !cmdline.hasOption(COLLECTION)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(LookupPostingsCompressed.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String indexPath = cmdline.getOptionValue(INDEX);
    String collectionPath = cmdline.getOptionValue(COLLECTION);

    if (collectionPath.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      System.exit(-1);
    }

    Configuration config = new Configuration();
    FileSystem fs = FileSystem.get(config);
    MapFile.Reader reader = new MapFile.Reader(new Path(indexPath + "/part-r-00000"), config);

    FSDataInputStream collection = fs.open(new Path(collectionPath));
    BufferedReader d = new BufferedReader(new InputStreamReader(collection));

    Text key = new Text();
    PairOfWritables<VIntWritable, BytesWritable> value =
        new PairOfWritables<VIntWritable, BytesWritable>();

    System.out.println("Looking up postings for the term \"starcross'd\"");
    key.set("starcross'd");

    reader.get(key, value);

    BytesWritable postings = value.getRightElement();
    ByteArrayInputStream buffer = new ByteArrayInputStream(postings.copyBytes());
    DataInputStream in = new DataInputStream(buffer);   
    int OFFSET=0;
    int count;
    while (in.available()!=0) {
      OFFSET = OFFSET + WritableUtils.readVInt(in);
      count = WritableUtils.readVInt(in);
      System.out.print("("+OFFSET+", "+count+")");
      collection.seek(OFFSET);
      System.out.println(d.readLine());
    }

    OFFSET=0;
    key.set("gold");
    reader.get(key, value);
    postings = value.getRightElement();
    buffer = new ByteArrayInputStream(postings.copyBytes());
    in = new DataInputStream(buffer);
    System.out.println("Complete postings list for 'gold': ("+value.getLeftElement()+", [");
    while(in.available()!=0)
    {
        OFFSET = OFFSET + WritableUtils.readVInt(in);
        count = WritableUtils.readVInt(in);
        System.out.print("("+OFFSET+", "+count+")");
        collection.seek(OFFSET);
        System.out.println(d.readLine());
        System.out.print(", ");
    }
    System.out.print("])\n");

    Int2IntFrequencyDistribution goldHist = new Int2IntFrequencyDistributionEntry();    
    buffer.reset();
    
    OFFSET=0;
    while(in.available()!=0)
    {
        OFFSET = OFFSET + WritableUtils.readVInt(in);
        count = WritableUtils.readVInt(in);
        goldHist.increment(count);
    }

    System.out.println("histogram of tf values for gold");
    for (PairOfInts pair : goldHist) {
      System.out.println(pair.getLeftElement() + "\t" + pair.getRightElement());
    }
    
    buffer.close();
    //Silver

    key.set("silver");
    reader.get(key, value);
    postings = value.getRightElement();
    buffer = new ByteArrayInputStream(postings.copyBytes());
    in = new DataInputStream(buffer);
    System.out.println("Complete postings list for 'silver': ("+value.getLeftElement()+", [");
    while(in.available()!=0)
    {
        OFFSET = OFFSET + WritableUtils.readVInt(in);
        count = WritableUtils.readVInt(in);
        System.out.print("("+OFFSET+", "+count+")");
        collection.seek(OFFSET);
        System.out.println(d.readLine());
        System.out.print(", ");
    }
    System.out.print("])\n");

    Int2IntFrequencyDistribution silverHist = new Int2IntFrequencyDistributionEntry();    
    buffer.reset();
    
    OFFSET=0;
    while(in.available()!=0)
    {
        OFFSET = OFFSET + WritableUtils.readVInt(in);
        count = WritableUtils.readVInt(in);
        silverHist.increment(count);
    }

    System.out.println("histogram of tf values for silver");
    for (PairOfInts pair : goldHist) {
      System.out.println(pair.getLeftElement() + "\t" + pair.getRightElement());
    }
    
    buffer.close();

    key.set("bronze");
    Writable w = reader.get(key, value);

    if (w == null) {
      System.out.println("the term bronze does not appear in the collection");
    }

    collection.close();
    reader.close();

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new LookupPostingsCompressed(), args);
  }
}
