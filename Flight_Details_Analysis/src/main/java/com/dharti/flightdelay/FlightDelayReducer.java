package com.dharti.flightdelay;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.Text;

public class FlightDelayReducer extends Reducer<Text, FlightDelayTuple, Text, FlightDelayTuple> {

	private FlightDelayTuple tuple = new FlightDelayTuple();

	protected void reduce(Text key, Iterable<FlightDelayTuple> values, Context context)
			throws IOException, InterruptedException {
		int total = 0;
		int delayedTotal = 0;
		int cancelledTotal = 0;

		for (FlightDelayTuple dt : values) {
			total += dt.getFlightsCount();
			delayedTotal += dt.getDelayedFlightsCount();
			cancelledTotal += dt.getCanceledFlightsCount();
		}

		double delayPercentage = ((double) delayedTotal / total) * 100;
		double cancelledPercentage = ((double) cancelledTotal / total) * 100;

		tuple.setFlightsCount(total);
		tuple.setDelayedFlightsCount(delayedTotal);
		tuple.setDelayPercentage(delayPercentage);
		tuple.setCanceledFlightsCount(cancelledTotal);
		tuple.setCanceledPercentage(cancelledPercentage);

		context.write(key, tuple);
	}
}

