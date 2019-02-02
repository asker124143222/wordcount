import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class WordCount {

    private static String HDFSUri = "hdfs://bigdata-senior01.home.com:9000";

    public static void main(String[] args) throws Exception {
        if(args.length!=2)
        {
            System.err.println("使用格式：WordCount <input path> <output path>");
            System.exit(-1);
        }

        long startTime = System.currentTimeMillis();
        //Configuration类代表作业的配置，该类会加载mapred-site.xml、hdfs-site.xml、core-site.xml等配置文件。
        Configuration conf =new Configuration();
        //本地运行
        //是否运行为本地模式，就是看这个参数值是否为local，默认就是local
//        conf.set("mapreduce.framework.name", "local");

        //本地模式运行mr程序时，输入输出的数据可以在本地，也可以在hdfs上
        //到底在哪里，就看以下两行配置你用哪行，默认就是file:///
        conf.set("fs.defaultFS","hdfs://bigdata-senior01.home.com:9000");
//        conf.set("fs.defaultFS", "file:///");

        //运行集群模式，就是把程序提交到yarn中去运行
        //要想运行为集群模式，以下3个参数要指定为集群上的值
        //如果是把程序打包成jar,hadoop jar运行，不需要写下面，因为hadoop jar脚本自动把集群中配置好的配置文件加载给该程序
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("yarn.resourcemanager.hostname", "bigdata-senior01.home.com");
//        conf.set("fs.defaultFS","hdfs://bigdata-senior01.home.com:9000");


        //如果实在非hadoop用户环境下提交任务
//        System.setProperty("HADOOP_USER_NAME","hadoop");
//        System.out.println("HADOOP_USER_NAME: "+System.getProperty("HADOOP_USER_NAME"));

        Path outPath = new Path(args[1]);
        //FileSystem里面包括很多系统，不局限于hdfs
//        FileSystem fileSystem = outPath.getFileSystem(conf);
        FileSystem fileSystem = FileSystem.get(URI.create(HDFSUri),conf);
        //删除输出路径
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
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //指定的这个路径可以是单个文件、一个目录或符合特定文件模式的一系列文件。
        //从方法名称可以看出，可以通过多次调用这个方法来实现多路径的输入。
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
//        job.submit();

        boolean result = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        long timeSpan = endTime - startTime;
        System.out.println("运行耗时："+timeSpan+"毫秒。");

        System.exit( result ? 0 : 1);

    }
}
