import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OutlierDetection {

    public static class OutlierMapper extends Mapper<Object, Text, Text, Text> {
        Integer min = 0;
        Integer max = 10000;
        int r;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            r = Integer.parseInt(conf.get("r"));
        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int maxVal = 10000;
            int gridSize = 100;
            //creating grid
            List<String> grids = new ArrayList<>();
            int count =1;
            for (int i = 0; i < maxVal; i = i + gridSize) {
                for (int j = 0; j < maxVal; j = j + gridSize) {
                    String grid = "g";
                    int gX = i +gridSize;
                    int gY = j +gridSize;
                    grid = grid + String.valueOf(count) + "," + String.valueOf(i) + ","
                            + String.valueOf(j) + "," + String.valueOf(gX) + "," + String.valueOf(gY);
                    grids.add(grid);
                    count++;
                }

            }

            //moving the grid
            String[] point = value.toString().split(",");
            int x = Integer.parseInt(point[0]);
            int y = Integer.parseInt(point[1]);

            for (String grid : grids) {
                String[] gridSplit = grid.split(",");
                String gridNum = gridSplit[0];
                float X1 = 0, Y1 = 0, X2 = 0, Y2 = 0;
                if (Integer.parseInt(gridSplit[1]) != min) X1 = (Integer.parseInt(gridSplit[1])) - r;
                if (Integer.parseInt(gridSplit[2]) != min) Y1 = (Integer.parseInt(gridSplit[2])) - r;
                if (Integer.parseInt(gridSplit[3]) != max) X2 = (Integer.parseInt(gridSplit[3])) + r;
                if (Integer.parseInt(gridSplit[4]) != max) Y2 = Integer.parseInt(gridSplit[4]) + r;

                if ((x >= (Integer.parseInt(gridSplit[1])))
                        && (x <= (Integer.parseInt(gridSplit[3]))) &&
                        (y >= (Integer.parseInt(gridSplit[2]))) &&
                        (y <= (Integer.parseInt(gridSplit[4])))) {
                    context.write(new Text(gridNum), new Text("g-" + String.valueOf(value)));
                }
                if ((x >= X1) && (x <= X2) && (y >= Y1) && (y <= Y2) &&
                        ((x < (Integer.parseInt(gridSplit[1]))) ||
                                (x > (Integer.parseInt(gridSplit[3]))) ||
                                (y < (Integer.parseInt(gridSplit[2]))) ||
                                (y > (Integer.parseInt(gridSplit[4]))))) {
                    context.write(new Text(gridNum), new Text("a-" + String.valueOf(value)));
                }

            }

        }
    }

    public static class OutlierReducer extends Reducer<Text, Text, Text, NullWritable> {
        int r, k;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            r = Integer.parseInt(conf.get("r"));
            k = Integer.parseInt(conf.get("k"));
        }
        public void reduce(Text key, Iterable<Text> text, Context context) throws IOException, InterruptedException {
            List<String> points = new ArrayList<>();
            List<String> vertices = new ArrayList<>();
            String[] values;

            for (Text val : text) {
                values = val.toString().split("-");
                if (values[0].equals("g")) {
                    vertices.add(values[1]);
                }
                points.add(values[1]);
            }
            for (String coor : vertices) {
                List<String> distanceList = new ArrayList<>();
                String[] coordinates = coor.split(",");
                float x = Float.parseFloat(coordinates[0]);
                float y = Float.parseFloat(coordinates[1]);

                for (String neighbour : points) {
                    String[] Neighbors = neighbour.split(",");
                    float x1 = Float.parseFloat(Neighbors[0]);
                    float y1 = Float.parseFloat(Neighbors[1]);
                    double distance=Math.sqrt((x1-x)*(x1-x) + (y1-y)*(y1-y));
                    if (distance <= r) {
                        distanceList.add(neighbour);
                    }
                }
                if ((distanceList.size() - 1) < k) {
                    context.write(new Text(coor), NullWritable.get());
                }
            }
        }
    }

    public void debug(String[] input) throws IOException, InterruptedException, ClassNotFoundException {
        System.setProperty("hadoop.home.dir", "C://hadoop-3.3.4//");
        Configuration conf = new Configuration();
        conf.addResource(new Path("C:/hadoop-3.3.4/etc/hadoop/core-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        conf.set("k",input[2]);
        conf.set("r",input[3]);
        Job job = Job.getInstance(conf, "OutlierDetection");

        FileSystem.get(conf).delete(new Path(input[1]), true);

        job.setJarByClass(OutlierDetection.class);
        job.setJobName("Outliers");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(OutlierDetection.OutlierMapper.class);
        job.setReducerClass(OutlierDetection.OutlierReducer.class);
        FileInputFormat.addInputPath(job, new Path(input[0]));
        FileOutputFormat.setOutputPath(job, new Path(input[1]));
        job.waitForCompletion(true);
    }

    public static void main(String [] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C://hadoop-3.3.4//");
        Configuration conf = new Configuration();
        conf.set("k",args[2]);
        conf.set("r",args[3]);
        Job job = Job.getInstance(conf, "OutlierDetection");

        FileSystem.get(conf).delete(new Path(args[1]), true);

        job.setJarByClass(OutlierDetection.class);
        job.setJobName("Outliers");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(OutlierDetection.OutlierMapper.class);
        job.setReducerClass(OutlierDetection.OutlierReducer.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

}
