import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 启动类
 * 本地运行的话 要讲maperd-site.xml 配置改成  mapreduce.framework.name   => local
 * 并且讲core-site.xml  hdfs-site.xml 修改
 */
public class WordCountMain {


    public static void main(String[] args) throws Exception {

        if (args.length != 2 || null == args) {
            System.out.println("参数错误");
            System.exit(0);
        }
        // 配置类
        Configuration configuration = new Configuration();
        // 任务实例化                            任务起名字 可以根本任务具体起名
        Job job = Job.getInstance(configuration, WordCountMain.class.getSimpleName());
        // 打包jar
        job.setJarByClass(WordCountMain.class);
        // 设置 输入、输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置map /reduce 阶段的类
        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);

        // 设置最终输出的 key  value 的类型

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置 job 输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // org.apache.hadoop.io.nativeio.NativeIO
        // 提交作业
        job.waitForCompletion(true);
    }
}
