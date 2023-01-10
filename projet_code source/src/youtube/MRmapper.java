package youtube;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class MRmapper extends Mapper <LongWritable,Text,LongWritable,Text> {
    static String IFS=",";
    static String OFS=",";
    static int NF=11;
    int bad_record_counter = 0;
    public void map(LongWritable key, Text value, Context context) 
                    throws IOException, InterruptedException {
        /** USvideos.csv
        video_id
        title
        channel_title
        category_id
        tags
        views
        likes
        dislikes
        comment_total
        thumbnail_link
        date
        */
        
        
        int category_id = 0;
        long num_of_views = 0;
        long num_of_likes = 0;
        long num_of_dislikes = 0;
        String thumbnail = null;
        String video_id = null;
        int bad_record_count = 0;
        
        if(key.get() == 0 && value.toString().contains("video_id")){
        	return;
        }
        else{
        	String [] data = value.toString().split(",");
        	if(data.length!=11){
        		bad_record_count++;
        		return;
        	}
        	else{
        		video_id = data[0];
        		category_id = Integer.parseInt(data[3]);
        		num_of_views = Long.parseLong(data[5]);
        		num_of_likes = Long.parseLong(data[6]);
        		num_of_dislikes = Long.parseLong(data[7]);
        		thumbnail = data[9];
        	}
        }
        
        String output_string = num_of_views+"_ "+num_of_likes + "_ " + num_of_dislikes + "_ " + thumbnail + "_ " + video_id;
        LongWritable newKey = new LongWritable();
        newKey.set(Long.valueOf(category_id));
        context.write(newKey, new Text(output_string));
             
        
    }
}
