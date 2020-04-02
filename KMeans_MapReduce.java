import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class KMeans_MapReduce {
		
	
	public static float[] centroids; 
	public static int number_of_k;

	// this is the mapper class
	public static class Map_KMeans extends Mapper<Object,Text, DoubleWritable, DoubleWritable>{	
		
		// this function is to calculate the distance between the points
		public static double getDistance(float point1, float point2){
			Double distance;
			distance = Math.sqrt(Math.pow((point1-point2),2) + Math.pow((point1+point2),2));
			return distance;

	 	}
		
		// this is the function to read centroids value from the file
		// value of centroids is stored in centroids array 	
		// this function is called every time the mapper class is called 
		@Override
		public void setup(Context context) throws IOException {
			try{
    			BufferedReader br = null;
    			String line="";	
    			// create a configuration from the context 
    			Configuration conf = context.getConfiguration();
    			
    			// get the parameters passed on from Main function
    			String get_variable = conf.get("number of clusters");
    			String[] v = get_variable.split(",");
    			
    			// get the number of centroids to be implemented
    			number_of_k = Integer.parseInt(v[0]);
    			
    			// get the number of iterations
    			int itr = Integer.parseInt(v[1]);
    			
    			// initialise the array to store the centroids
    			centroids =  new float[number_of_k];
    			//System.out.print("Number of clusters : " + number_of_k);
    			//System.out.print("Current iteration :  " + itr);
    			
    		int j =0;
			if (itr == 0)
			{
				// for the first iteration, centroids are randomly initiliaised and stored in centroids array
				for (int i =0;i<number_of_k;i++)
				{
					centroids[j]=i;
					j++;
				}
			}
			else{
				// for the subsequent iterations, centroids are taken from the file of previous iterations
				// read the output file from distributed cache and store the centroids in the 
				Path[] localfiles = DistributedCache.getLocalCacheFiles(conf);      
    			br = new BufferedReader(new FileReader(localfiles[0].toString()));
    			while((line=br.readLine()) != null)
    			{
    				String[] s= line.split("\t| ");
    				float c = Float.parseFloat(s[0]);
    				// System.out.print("Centroid is  :  " + c);
    				centroids[j]=c;
				j++;
    			}
			}
			}
			catch(Exception e){
				System.out.print("Exception caught at setup function " + e.getMessage());
				System.out.print("Exception caught at setup function " + e.getStackTrace());
			}		
    		}

		// this is the map function. According to the algorithm
		// this takes the centroids and assigns data points to nearest centroids
    		public void map(Object key, Text value, Context context
	  		) throws IOException, InterruptedException {
			try {	  		
			double distance= 0;
			double distance_between_points = 0;
			
			// initilialising a large number. generally this number should be larger than any of the data points
			double min_distance = Math.pow(10,10);
	  		float new_centroid=0;
	  		int i=0;
			double key_value = Double.parseDouble(key.toString());
	  		int count = 0;
	  		
	  		// get the points and convert them to string 
			float p = Float.parseFloat(value.toString());			
			float[] current_centers= new float[centroids.length]; 
			float center;
			
			// go through each centroid and calculate the distance between each data point to the centroids
	  		for ( i=0; i< centroids.length; i++ ) {	    
	      			distance_between_points =  getDistance(p,centroids[i]);
					for (int j =0;j<centroids.length;j++)
					{
						center = centroids[j];
						current_centers[j] = center;
					}	      
					distance = p- centroids[i]; 			
					// assign a new centroid if distance is less than the min_distance
					if ( Math.abs(distance) < Math.abs(min_distance) ) {
		    	  			new_centroid=centroids[i];
		    	  			min_distance = distance;	      	  
		      		}
		      		count++;
	  		}
	  		DoubleWritable new_Centroid = new DoubleWritable(new_centroid);
			DoubleWritable datapoints = new DoubleWritable(Double.parseDouble(value.toString()));
			
			// write the output in the context where key is the new centroid and the value is the datapoints
	  		context.write(new_Centroid,datapoints);
			}
			catch(Exception e){
				System.out.print("Exception caught at map function " + e.getMessage() );
				System.out.print("Exception caught at map function " + e.getStackTrace() );
			}
	 
    		}

		
	}

	// this is the reducer class
	public static class Reduce_KMeans extends Reducer<DoubleWritable,DoubleWritable,DoubleWritable,Text> {

		// this is the reduce function
		// according to the algorithm, it calculates new centroids of the clusters and emits the new centroids of the clusters to output
   	 	public void reduce(DoubleWritable clusterid, Iterable<DoubleWritable> points, Context context  ) 
			throws IOException, InterruptedException {
			try{  
      			String data = "";
      			int count = 0;
		 		float centroid=0;
				DoubleWritable cluster_id = new DoubleWritable();
				cluster_id = clusterid;  
				// iterate through each cluster and calulcate the average of all the points of that cluster
				// the calculated average will be the new centroid of that cluster 
		  		for (DoubleWritable point : points) {								
		      			double p = point.get();
		      			data =data + " " + "," +  Double.toString(p);
		      			centroid += p ;
		      			count++;
		  		}
				System.out.print("Cluster id for this iteration is: " + cluster_id);			
				centroid = centroid/count;
				DoubleWritable new_centroid = new DoubleWritable(centroid);
				Text data_points = new Text(data);
				// Reduce function writes output in where key is the new centroid and value is the datapoints
		  		context.write(new_centroid,data_points);
			}
			catch(Exception e){
				System.out.print("Exception caught at reduce function " + e.getMessage() );
				System.out.print("Exception caught at reduce function " + e.getStackTrace());
			}
    		}
	}

	// this function returns an array of centroids of each cluster from the output file
	// this function is called when convergence condition is checked
	public static Double[] getCentroids(Path file, int number_of_clusters) {
		Double[] centroids = new Double[number_of_clusters];		
		try {
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file)));		
		String line = " ";
		int i =0;
		while ((line=br.readLine()) != null) {
			String[] centroid_temp = line.split("\t| ");
			double c = Double.parseDouble(centroid_temp[0]);
			centroids[i] = c;
			i++;	
					
		}
		return centroids;
		}
		catch (Exception e){
			System.out.print("Exception caught at getCenters function " +e.getMessage() );
			System.out.print("Exception caught at getCenters function " +e.getStackTrace() );
			return centroids;
		}

	}


	// this function is used to check the convergence and returns a bool value isConverged
	// it compares centroids of clusters of current iteration and previous iterations
	// it returns true if both centroids are the same
	public static boolean isConverged(String output, String new_input, String centroidPath, int number_of_clusters) {
		boolean isConverged = false;
		try{	
		
		Double[] centroids_pres = new Double[number_of_clusters];
		Double[] centroids_pred = new Double[number_of_clusters];
		String predecessor;
		predecessor = centroidPath;		
		Path predecessor_file = new Path(predecessor);
		Path present = new Path(output + "/part-r-00000");
		centroids_pres = getCentroids(present , number_of_clusters);
		centroids_pred = getCentroids(predecessor_file, number_of_clusters);
		if (Math.abs(centroids_pres[0] -centroids_pred[0]) <= 0  ){
			if (Math.abs(centroids_pres[1] -centroids_pred[1]) <=0){
				isConverged = true;		
			}
		}
		else
		{
			isConverged = false;
		}
			
		return isConverged;
		}
		catch(Exception e){
			System.out.print("Exception caught at isConverged function " +e.getMessage());
			System.out.print("Exception caught at isConverged function " +e.getStackTrace());	
			return isConverged;
		}
	}

	// this the main and entry function of the implementation
	// it sets mapper and reducer class and sets up the job for the MapReduce 
	public static void main(String[] args) throws Exception {
		boolean isconverged = false;
		long startTime = System.currentTimeMillis();
		try {	
		int itr= 0;	
		int output_count = 1;
		
		// getting the value of number of centroids from the command line
		String number_of_clusters = args[0];	
		
		// create separate output path for each iteration
		String output_path = args[2] + "Iteration" + output_count;
		
		// input for current iteration is read from the output of the previous iterations
		String new_input_path = output_path;			
		String predecessor ;
		// this runs until the convergence condition is reached 
		while(isconverged==false){
			
			// creating a configuration object
			Configuration conf = new Configuration(); 
			
			//sends the value of k and iteration to the mapper function
			conf.set("number of clusters",number_of_clusters + "," + itr);						 
			Job job = new Job(conf, "KMeans_MapReduce");	
			
			//set the input path
			FileInputFormat.addInputPath(job, new Path(args[1]));
			
			// set the output path
			FileOutputFormat.setOutputPath(job, new Path(output_path));
			
			// setting the mapper and reducer class
			job.setJarByClass(KMeans_MapReduce.class);
			job.setMapperClass(Map_KMeans.class);
			job.setReducerClass(Reduce_KMeans.class);
			job.setInputFormatClass(TextInputFormat.class);			
			job.setMapOutputKeyClass(DoubleWritable.class);
			job.setMapOutputValueClass(DoubleWritable.class);			
			job.setOutputKeyClass(DoubleWritable.class);
			job.setOutputValueClass(Text.class);
			
			predecessor = "";
			if ( itr !=0){				
				// this output file acts like input for the subsequent iterations
				predecessor = new_input_path + "/part-r-00000";
				Path addCache = new Path(predecessor);
				// adding the output file to distributed cache for the subsequent iterations
				job.addCacheFile(addCache.toUri());
				job.createSymlink();	
			}
			job.waitForCompletion(true);
			
			// this function checks whether the centroids are convered or not 
			isconverged = isConverged(output_path,new_input_path,predecessor,Integer.parseInt(number_of_clusters));
			
			// output path will now be the new input path
			new_input_path = output_path;
			output_count++;
			output_path =args[2] + "Iteration"+ output_count;
			
			// if centroids are converged, it breaks the while loop
			if(isconverged == true) {
				long endTime = System.currentTimeMillis();
				long executionTime = endTime - startTime;
				
				// calculating the execution time of the program
				System.out.print("Execution time for the program is " + executionTime);
				break;
			}			
			itr++;			
		    job.waitForCompletion(true);
			}
		}
		catch(Exception e){
			System.out.print("Exception caught at main function" + e.getMessage());
			System.out.print("Exception caught at main function" + e.getStackTrace());
		}
	}
}