package invertedindex;
import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	
	StringBuilder stringBuilder = new StringBuilder();
	
	@Override
	public void reduce(final Text key,
						final Iterable<Text> values, 
						final Context context )
			throws IOException, InterruptedException{

		for(Text value : values ) {
			stringBuilder.append(value.toString());
			if(values.iterator().hasNext()) {
				stringBuilder.append(" | ");
			}
			
		}
		
		context.write(key, new Text(stringBuilder.toString()));
	}

}



