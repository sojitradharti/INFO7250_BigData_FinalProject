package com.dharti.average_flight;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AverageMapper extends Mapper<Object, Text, Text, AvgDisTimeTuple> {

	private AvgDisTimeTuple avgDistTimeTuple = new AvgDisTimeTuple();

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] data = value.toString().split(",");

		if (data[0].equals("YEAR"))
			return;
       // was 8
		String carrierCode = data[7];
		int distance = 0;
		int airTime = 0;

		try {
			distance = Integer.parseInt(data[24]);

			airTime = Integer.parseInt(data[17]);
		} catch (Exception e) {
			return;
		}
		avgDistTimeTuple.setTotalFlights(1);
		avgDistTimeTuple.setTotalDistance(distance);
		avgDistTimeTuple.setTotalAirtime(airTime);

		context.write(new Text(carrierCode), avgDistTimeTuple);
	}
}
