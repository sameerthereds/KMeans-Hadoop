package read_csv;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


// this file is to read csv file and store the data points in the input_data.txt

public class csv_reader {
	
	public static void main(String[] args)
	{
		String csvFile = "C:\\Users\\sneupane\\iris.csv";
		BufferedReader br = null;
		String line = "";
		
		try
		{
			List<Float> list_items= new ArrayList<Float>();
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) !=null)
			{
				String[] items = line.split(",");				
				list_items.add(Float.parseFloat(items[0])) ;
				list_items.add(Float.parseFloat(items[1])) ;
				list_items.add(Float.parseFloat(items[2])) ;
				list_items.add(Float.parseFloat(items[3])) ;				
			}
			
			String fileName= "C:\\Users\\sneupane\\input_data.txt";
		    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
			for( int i =0; i< list_items.size();i++)
			{				
			    writer.write(Float.toString(list_items.get(i)) + "\n");  
			    
			}
			writer.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}

}
