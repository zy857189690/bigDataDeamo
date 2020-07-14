import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Text,IntWritable  从map 端接收的 key value 类型
 *
 * Text,IntWritable reduce端输出的 key value 类型
 */
public class WordCountReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    // 进行reduce 操作

    /**
     * key : hello
     * value : List(1,1,1,1)
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       // super.reduce(key, values, context);
        // 对每个单词进行计数
        int sum=0;
        for (IntWritable value :values){
            sum=sum+value.get();
        }
        context.write(key,new IntWritable(sum));

    }
}
