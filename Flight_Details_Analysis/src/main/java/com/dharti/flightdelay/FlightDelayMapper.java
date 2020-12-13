package com.dharti.flightdelay;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightDelayMapper extends Mapper<Object, Text, Text, FlightDelayTuple> {
	private FlightDelayTuple tuple = new FlightDelayTuple();
	boolean flag = true;

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] data = line.split(",");
		// Total flights and canceled working delayed not working always getting wrong fields
		if (data[0].equalsIgnoreCase("YEAR"))
		{
			return;
		}

		String year = data[0];
		String month = data[2];
		//month = String.format("%02d", Integer.parseInt(month));
		String date = data[5];
		//date = String.format("%02d", Integer.parseInt(date));
		try {
			System.out.println(data[15]);
			String delay = data[14];

			if (delay.startsWith("-")) {
				
				tuple.setDelayedFlightsCount(0);
			} else {
				
				tuple.setDelayedFlightsCount(1);
			}
		} catch (Exception e) {
			
			tuple.setDelayedFlightsCount(0);
		}
		try {

			if (data[22].equals("1.00")) {
			
				tuple.setCanceledFlightsCount(1);
			} else {
			
				tuple.setCanceledFlightsCount(0);
			}
		} catch (Exception e) {
			tuple.setCanceledFlightsCount(0);
		}
		tuple.setFlightsCount(1);

		context.write(new Text(year + month + date), tuple);
		
	}

}
