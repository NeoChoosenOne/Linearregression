package com.cbds.linear_regression;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LinearRegression {
  
  /*Class extends from Mapper Class Input params (Object, Text) usually Object Key is the offset of bytes 
   * and Text are the lines at input text, Output params (Text, IntWriteable).
   */

  public static class Map extends Mapper<Object, Text, Text, Text>{
	Text elementos = new Text("");
    //Method implementation for map function, input params (key,value) as types (Object, Text) and ouput values as context as type Context.
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //Split the input value delimited by commas into an array of Strings. 
      String [] puntos = value.toString().split(",");
      //Create a primitive double variable x and y from the previous array.
      double x = new Double(puntos[0]).doubleValue();
      double y = new Double(puntos[1]).doubleValue();
      //Create a string that contains the next format (sum_x, sum_y, sum_xy, sum_xx, sum_n, n) for the map method
      //the previous values are only the sum of it self, but this type of format will help us into the combiner fase.
      String valor = x + "," + y + "," + y*x + "," + x*x + "," + 1;
      //Set to the elementos object the previous valor object
      elementos.set(valor);
      //Send the output of Map (text, text)
      context.write(new Text("key"), elementos);
    }
  }
  /*Class extends from Reducer Class input 
   * 
   */
  public static class Reduce extends Reducer<Text,Text,Text,Text> {
	Text elementos_sumas = new Text("");
    //Method Implementation for reduce function, input params (key, Iterable<Text>) where Iterable<Text> have all the elements of the file.
    //At the combiner phase this will be very helpful because each mapper that is created by hadoop will do the partial sums into each node.
	//To have n elements into the reducer phase where n is the number of nodes to the mapper phase.  
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		//Create an array of Strings to get each sum of the mapper phase.
    	String [] elementos;
    	//Create each element to the predict formula.
    	double suma_x  = 0;
    	double suma_y  = 0;
    	double suma_xy = 0;
    	double suma_xx = 0;
    	double n       = 0;
    	//A loop to iterate each sum.
    	while(values.iterator().hasNext()){
    		elementos = values.iterator().next().toString().split(",");
    		suma_x  = suma_x  + new Double(elementos[0]).doubleValue();
    		suma_y  = suma_y  + new Double(elementos[1]).doubleValue();
    		suma_xy = suma_xy + new Double(elementos[2]).doubleValue();
    		suma_xx = suma_xx + new Double(elementos[3]).doubleValue();
    		n 		= n 	  + new Double(elementos[4]).doubleValue();
    	}
		String valor;
		//A conditional sentence that checks if key is "key_final" that indicate is a reduce phase, otherwise is combiner phase.
    	if(key.toString().equalsIgnoreCase("key_final")){
    		double betacero_estimador = ((suma_x*suma_y)-n*(suma_xy))/((suma_x*suma_x)-(n*suma_xx));
    		double betauno_estimador  = (suma_y-(betacero_estimador*suma_x))/n;
    		valor = betacero_estimador + "," + betauno_estimador;
    		elementos_sumas.set(valor);
        	context.write(null, elementos_sumas);
    	}else{
    		valor = suma_x + "," + suma_y + "," + suma_xy + "," + suma_xx + "," + n;
    		elementos_sumas.set(valor);
        	context.write(new Text("key_final"), elementos_sumas);
    	}
    }
  }
  //This example takes by default the inputTextFormat in input split.
  public static void main(String[] args) throws Exception {
	  
	//Hadoop configuration 
    Configuration conf = new Configuration();
    //Job Configuration set it a hadoop conf and Job name.
    Job job = Job.getInstance(conf, "linear_regression");
    //Set to job configuration the main class that contains main method.
    job.setJarByClass(LinearRegression.class);
    //Set to job configuration the class where the Mapper Implementation is.
    job.setMapperClass(Map.class);
    //Set to job configuration the class where the Combiner Implementation is.
    job.setCombinerClass(Reduce.class);
    //Set to job configuration the class where the Reducer Implementation is.
    job.setReducerClass(Reduce.class);
    //Set to job configuration the class 
    job.setOutputKeyClass(Text.class);
    //Set to job configuration the class 
    job.setOutputValueClass(Text.class);
    //Input path in HDFS to read files to InputSpliter and Record Reader 
    FileInputFormat.addInputPath(job, new Path(args[0]));
    //Output path in HDFS to put output result for this job
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //Wait until Job workflow finish.
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
