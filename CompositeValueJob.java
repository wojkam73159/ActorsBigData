import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;



public class CompositeValueJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int res = ToolRunner.run(new CompositeValueJob(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "CompositeValueJob");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(CompositeValueJob.CompositeValueMapper.class);
        job.setCombinerClass(CompositeValueJob.CompositeCombiner.class);
        job.setReducerClass(CompositeValueJob.CompositeValueReducer.class);
        //job.setNumReduceTasks(0);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TwoValueWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class CompositeValueMapper extends Mapper<LongWritable, Text, Text, TwoValueWritable> {
        private final Text nconstPersonIdentifier = new Text();
        private TwoValueWritable compositeValue=new TwoValueWritable();
        Set<String> roles=new HashSet<String>(Arrays.asList("actor","actress","self"));

        public void map(LongWritable offset, Text lineText, Context context) {

            //Logger log = Logger.getLogger(CompositeValueJob.class.getName());
            try {
                if (offset.get() != 0) {
                    String line = lineText.toString();
                    int i = 0;
                    for (String word : line.split("\t"))
                    {

                        if (i == 2) {
                            nconstPersonIdentifier.set(word);
                            //log.debug(word);
                        }
                        if (i == 3)
                        {//if actor in
                            if ( roles.contains(word)) {
                                compositeValue.set( new IntWritable(1), new IntWritable(0));
                            }
                            if (Objects.equals(word, "director")) {
                                compositeValue.set(new IntWritable(0), new IntWritable(1));
                            }

                        }
                        i++;
                    }

                    //zr
                    //compositeValue.set(firstVal,secondVal);
                    //log.debug(firstVal);
                    //log.debug(secondVal);
                    //log.debug(compositeValue);
                    context.write(nconstPersonIdentifier,compositeValue);

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }
    public static class CompositeValueReducer extends Reducer<Text, TwoValueWritable, Text, TwoValueWritable> {

        //private final private final TwoValueWritable resultValue = new TwoValueWritable();
        //IntWritable sum_acting=new IntWritable(0);
        //IntWritable sum_director=new IntWritable(0);

        @Override
        public void reduce(Text key, Iterable<TwoValueWritable> values,
                           Context context) throws IOException, InterruptedException {
            TwoValueWritable resultValue = new TwoValueWritable();
            IntWritable sum_acting=new IntWritable(0);
            IntWritable sum_director=new IntWritable(0);
            Text resultKey = new Text( key+"," );

            for (TwoValueWritable val : values) {
                sum_acting.set(sum_acting.get()+val.getFirst().get());
                sum_director.set(sum_director.get()+val.getSecond().get());
            }

            resultValue.set(sum_acting,sum_director);
            //DONE: write result pair to the context
            context.write(resultKey, resultValue);

        }
    }

    public static class CompositeCombiner extends Reducer<Text, TwoValueWritable, Text, TwoValueWritable> {


        //private final TwoValueWritable sum = new TwoValueWritable(0, 0);
        @Override
        public void reduce(Text key, Iterable<TwoValueWritable> values, Context context) throws IOException, InterruptedException {
            TwoValueWritable sum = new TwoValueWritable(0, 0);
            sum.set(new IntWritable(0), new IntWritable(0));

            for (TwoValueWritable val : values) {
                sum.set(new IntWritable(sum.getFirst().get()+val.getFirst().get()),new IntWritable(sum.getSecond().get()+val.getSecond().get()));
            }
            context.write(key, sum);
        }
    }
}
