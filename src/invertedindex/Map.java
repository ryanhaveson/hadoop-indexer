package invertedindex;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Map extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		FileSplit currentSplit = ((FileSplit) context.getInputSplit());
		String filenameStr = currentSplit.getPath().getName();
		Text Filename = new Text(filenameStr);
		
		System.out.println("Procesing file: " + filenameStr);
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		Text word = new Text();
		while(tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			context.write(word, Filename);
		}
		
	}
}
