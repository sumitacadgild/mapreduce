package dist_cache;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class EmployeeMapper extends Mapper<LongWritable,Text, Text, Text>{
	
	private URI[] files;
	/*
	 * Declare Transaction Map to store all the records from department mapper as 
	 * key value pairs 
	 */
	private HashMap<String,String> CustMap = new HashMap<String,String>();

	/*
	 * Setup method will be executed once per input split , this  method 
	 * is  executed before map method  
	 */
	@Override
	public void setup(Context context) throws IOException
	{
		// Access the cache files 
		files = DistributedCache.getCacheFiles(context.getConfiguration());
		System.out.println("files:"+ files);
		Path path = new Path(files[0]);
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		/*
		 * Create the input stream and read the file from the cache and store
		 * in Hash map
		 */
		FSDataInputStream in = fs.open(path);
		BufferedReader br  = new BufferedReader(new InputStreamReader(in));
        String line="";
        while((line = br.readLine())!=null)
        {
                String splits[] = line.split(",");
                //splits[0] is the CustID no and splits[1 ] is the Transactions 
               CustMap.put(splits[0], splits[1]);

        }
        
		br.close();
		in.close();
		
		
	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 * 
	 * Map method will be executed after setup method, and map method will execute per every 
	 * record from the input file
	 */
	@Override
	public void map(LongWritable key, Text val,Context context) throws IOException,InterruptedException
	{
		
		String[] splits = val.toString().split(",");
		/*
		 * Department Id is joining key in this data sets, 
		 * join the data based on the department Id
		 */
		int count=0;
		long sum=0;
		if(CustMap.containsKey(splits[0]))
			
		{
			 String Transtions=splits[3].replaceAll("[\\[ \\]]", "");
            
			String []trans=Transtions.split(";");
			for(int i=0;i<trans.length;i++){
				count++;
				sum+=Integer.parseInt(trans[i].trim());
			}
			context.write(new Text(CustMap.get(splits[0])), new Text("Number of Trans:"+count+","+"Total Amount:"+sum));
		}
		
		
		
	}

}


