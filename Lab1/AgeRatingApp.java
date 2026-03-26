import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.net.URI;
import java.util.HashMap;

public class AgeRatingApp {

    public static class AgeMapper extends Mapper<Object, Text, Text, Text> {
        private HashMap<String, String> userAgeGroup = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    // Tìm đúng file users.txt trong cache
                    if (uri.toString().contains("users.txt")) {
                        FileSystem fs = FileSystem.get(context.getConfiguration());
                        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
                        String line;
                        while ((line = br.readLine()) != null) {
                            String[] parts = line.split(",");
                            // Cột 0 là UserID, Cột 2 là Tuổi (Age)
                            if (parts.length >= 3) {
                                try {
                                    int age = Integer.parseInt(parts[2].trim());
                                    String group = (age <= 18) ? "0-18" : (age <= 35) ? "18-35" : (age <= 50) ? "35-50" : "50+";
                                    userAgeGroup.put(parts[0].trim(), group);
                                } catch (Exception e) {}
                            }
                        }
                        br.close();
                    }
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                String userId = parts[0].trim();
                String movieId = parts[1].trim();
                String rating = parts[2].trim();
                String ageGroup = userAgeGroup.getOrDefault(userId, "Unknown");
                
                context.write(new Text(movieId), new Text(ageGroup + "_" + rating));
            }
        }
    }

    public static class AgeReducer extends Reducer<Text, Text, Text, Text> {
        private HashMap<String, String> movieMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    // Tìm đúng file movies.txt trong cache
                    if (uri.toString().contains("movies.txt")) {
                        FileSystem fs = FileSystem.get(context.getConfiguration());
                        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
                        String line;
                        while ((line = br.readLine()) != null) {
                            String[] parts = line.split(",");
                            if (parts.length >= 3) {
                                String id = parts[0].trim();
                                // Xử lý tên phim đề phòng có dấu phẩy bên trong
                                StringBuilder title = new StringBuilder();
                                for(int i=1; i<parts.length-1; i++) {
                                    title.append(parts[i]).append(i == parts.length-2 ? "" : ",");
                                }
                                movieMap.put(id, title.toString().trim());
                            }
                        }
                        br.close();
                    }
                }
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double s18 = 0, s35 = 0, s50 = 0, sOld = 0;
            int c18 = 0, c35 = 0, c50 = 0, cOld = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("_");
                if (parts.length == 2) {
                    try {
                        double r = Double.parseDouble(parts[1]);
                        switch (parts[0]) {
                            case "0-18": s18 += r; c18++; break;
                            case "18-35": s35 += r; c35++; break;
                            case "35-50": s50 += r; c50++; break;
                            case "50+": sOld += r; cOld++; break;
                        }
                    } catch (Exception e) {}
                }
            }
            
            String title = movieMap.getOrDefault(key.toString(), "Unknown Movie (" + key.toString() + ")");
            String result = String.format("[0-18: %.2f, 18-35: %.2f, 35-50: %.2f, 50+: %.2f]",
                    c18 > 0 ? s18/c18 : 0, c35 > 0 ? s35/c35 : 0, 
                    c50 > 0 ? s50/c50 : 0, cOld > 0 ? sOld/cOld : 0);
            context.write(new Text(title), new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 4: Age Rating");
        job.setJarByClass(AgeRatingApp.class);
        job.setMapperClass(AgeMapper.class);
        job.setReducerClass(AgeReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Nạp 2 file vào Cache
        job.addCacheFile(new Path(args[0]).toUri()); // movies.txt
        job.addCacheFile(new Path(args[1]).toUri()); // users.txt
        
        FileInputFormat.addInputPath(job, new Path(args[2])); // ratings_1.txt
        FileInputFormat.addInputPath(job, new Path(args[3])); // ratings_2.txt
        FileOutputFormat.setOutputPath(job, new Path(args[4])); // output_task4

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
