import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.*;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  public static final Logger logger = Logger.getLogger(WordCountMapper.class);
  
  private static final IntWritable one = new IntWritable(1);
  private Text word = new Text();
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	  String line = value.toString();
	  
	  String[] splitStrings = line.split("\t");
	  for(String s : splitStrings){
	  word.set(s);
	  context.write(word, one);
	  }
    
  }
}
