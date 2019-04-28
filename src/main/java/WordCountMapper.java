import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/1/29 16:44
 * @Description: 读取采用空格分隔的字符
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private enum Counters {LINES,WORDS}
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(Counters.LINES).increment(1);
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            context.getCounter(Counters.WORDS).increment(1);
            System.out.println(word);
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
