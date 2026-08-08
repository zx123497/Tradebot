package findata.model;

import java.io.Serializable;

public class Trade implements Serializable {
	private static final long serialVersionUID = 1L;

	public String symbol;
	public double price;
	public double volume;
	/** Event time in epoch millis (Kafka field: timestamp_ms). */
	public long timestamp;
}
