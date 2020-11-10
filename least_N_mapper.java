import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class least_N_mapper extends Mapper < Object,
Text,
Text,
LongWritable > {

  private HashMap < String,
  Long > hmap;

  @Override
  public void setup(Context context) throws IOException,
  InterruptedException {
    hmap = new HashMap < String,
    Long > ();
  }

  @Override
  public void map(Object key, Text value, Context context) throws IOException,
  InterruptedException {

    StringTokenizer st = new StringTokenizer(value.toString());

    String token;
    long no_of_views;

    while (st.hasMoreTokens()) {
      token = st.nextToken();
      if (hmap.containsKey(token)) {
        no_of_views = hmap.get(token) + 1;
      } else {
        no_of_views = 1;
      }
      hmap.put(token, no_of_views);
    }

  }

  @Override
  public void cleanup(Context context) throws IOException,
  InterruptedException {
    for (Map.Entry < String, Long > entry: hmap.entrySet()) {

      long count = entry.getValue();
      String name = entry.getKey();

      context.write(new Text(name), new LongWritable(count));
    }
  }
}
