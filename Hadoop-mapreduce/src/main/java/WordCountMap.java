import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *  LongWritable key 每一行的起始偏移量  无实际用处
 *  Text 输入value 类型
 *   Text 输出 key value 类型
 *   IntWritable 输出value 类型
 */
public class WordCountMap extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       // 对输入value 进行切分
        String[] split = value.toString().split(" ");
        for (String word:split){
            // （key,1 ） 发送到reduce 端
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
