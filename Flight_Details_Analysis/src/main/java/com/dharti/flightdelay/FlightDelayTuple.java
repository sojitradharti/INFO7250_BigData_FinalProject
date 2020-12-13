package com.dharti.flightdelay;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class FlightDelayTuple implements Writable {

	private int flightsCount = 0;
	private int delayedFlightsCount = 0;
	private double delayPercentage = 0.0;
	private int canceledFlightsCount = 0;
	private double canceledPercentage = 0.0;

	public int getFlightsCount() {
		return flightsCount;
	}

	public void setFlightsCount(int flightsCount) {
		this.flightsCount = flightsCount;
	}

	public int getDelayedFlightsCount() {
		return delayedFlightsCount;
	}

	public void setDelayedFlightsCount(int delayedFlightsCount) {
		this.delayedFlightsCount = delayedFlightsCount;
	}

	public double getDelayPercentage() {
		return delayPercentage;
	}

	public void setDelayPercentage(double delayPercentage) {
		this.delayPercentage = delayPercentage;
	}

	public int getCanceledFlightsCount() {
		return canceledFlightsCount;
	}

	public void setCanceledFlightsCount(int canceledFlightsCount) {
		this.canceledFlightsCount = canceledFlightsCount;
	}

	public double getCanceledPercentage() {
		return canceledPercentage;
	}

	public void setCanceledPercentage(double canceledPercentage) {
		this.canceledPercentage = canceledPercentage;
	}

	@Override
	public String toString() {

		return "" + flightsCount + "," + delayedFlightsCount + "," + String.format("%.2f", delayPercentage) + ","
				+ canceledFlightsCount + "," + String.format("%.2f", canceledPercentage);
	}


	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(flightsCount);
		dataOutput.writeInt(delayedFlightsCount);
		dataOutput.writeDouble(delayPercentage);
		dataOutput.writeInt(canceledFlightsCount);
		dataOutput.writeDouble(canceledPercentage);

	}


	public void readFields(DataInput dataInput) throws IOException {

		flightsCount = dataInput.readInt();
		delayedFlightsCount = dataInput.readInt();
		delayPercentage = dataInput.readDouble();
		canceledFlightsCount = dataInput.readInt();
		canceledPercentage = dataInput.readDouble();

	}
}

