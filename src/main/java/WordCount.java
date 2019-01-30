import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static void main(String[] args) throws Exception {
        if(args.length!=2)
        {
            System.err.println("使用格式：WordCount <input path> <output path>");
            System.exit(-1);
        }
        //Configuration类代表作业的配置，该类会加载mapred-site.xml、hdfs-site.xml、core-site.xml等配置文件。
        Configuration conf =new Configuration();

        Path outPath = new Path(args[1]);
        //FileSystem里面包括很多系统，不局限于hdfs
        FileSystem fileSystem = outPath.getFileSystem(conf);
        if(fileSystem.exists(outPath))
        {
            fileSystem.delete(outPath,true);
        }

        Job job = Job.getInstance(conf,"word count"); // new Job(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(WordCountMapper.class);
        //Combiner最终不能影响reduce输出的结果
//        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        //一般情况下mapper和reducer的输出的数据类型是一样的，所以我们用上面两条命令就行，如果不一样，我们就可以用下面两条命令单独指定mapper的输出key、value的数据类型
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(IntWritable.class);
        //hadoop默认的是TextInputFormat和TextOutputFormat,所以说我们这里可以不用配置。
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //指定的这个路径可以是单个文件、一个目录或符合特定文件模式的一系列文件。
        //从方法名称可以看出，可以通过多次调用这个方法来实现多路径的输入。
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
