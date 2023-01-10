package youtube;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class MRreducer  extends Reducer <LongWritable,Text,Text,Text> {
    public static String IFS=",";
    public static String OFS=",";
    
    public void reduce(LongWritable key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
    	final Logger logger = Logger.getLogger("Mylog");
    	String temp_string = null;
    	long temp_num_views = 0;
    	long temp_num_likes = 0;
    	long temp_num_dislikes = 0;
    	String temp_thumbnail_views = null;
    	String temp_thumbnail_likes = null;
    	String temp_thumbnail_dislikes = null;
    	String video_id_views = null;
    	String video_id_likes = null;
    	String video_id_dislikes = null;
    	
    	for(Text value:values){
    		temp_string = value.toString();
    		String [] stringArray = temp_string.split("_ ");
    		//temp_num_views = Long.parseLong(stringArray[0]);
    		//temp_num_likes = Long.parseLong(stringArray[1]);
    		//temp_num_dislikes = Long.parseLong(stringArray[2]);
    		//temp_thumbnail = stringArray[3];
    		if(temp_num_views<Long.parseLong(stringArray[0])){
    			temp_num_views = Long.parseLong(stringArray[0]);
    			temp_thumbnail_views = stringArray[3];
    			video_id_views = stringArray[4];
    		}
    		if(temp_num_likes<Long.parseLong(stringArray[1])){
    			temp_num_likes = Long.parseLong(stringArray[1]);
    			temp_thumbnail_likes = stringArray[3];
    			video_id_likes = stringArray[4];
    		}
    		if(temp_num_dislikes<Long.parseLong(stringArray[2])){
    			temp_num_dislikes = Long.parseLong(stringArray[2]);
    			temp_thumbnail_dislikes = stringArray[3];
    			video_id_dislikes = stringArray[4];
    			
    		}	
    	}
    	Text keyText = new Text("\nmost views:" + video_id_views + OFS + temp_thumbnail_views + "\nmost likes:" + video_id_likes + OFS + temp_thumbnail_likes + "\nmost dislikes:" + video_id_dislikes + OFS + temp_thumbnail_dislikes);
    	context.write(new Text("category_id:"+key), keyText);
        logger.info("Reducer completed");
       
   }
}
