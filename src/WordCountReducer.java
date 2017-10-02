import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.*;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  public static final Logger logger = Logger.getLogger(WordCountReducer.class);
  private IntWritable result = new IntWritable(); 
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

	  int sum = 0;
	  for(IntWritable value: values){
		  sum += value.get();
	  }
	  
	  result.set(sum);
	  context.write(key, result);

  }
}