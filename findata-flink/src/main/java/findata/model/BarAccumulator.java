package findata.model;

import java.io.Serializable;

public class BarAccumulator implements Serializable {
	private static final long serialVersionUID = 1L;

	public String symbol;
	public double open = -1.0;
	public double high = -Double.MAX_VALUE;
	public double low = Double.MAX_VALUE;
	public double close = 0.0;
	public double volume = 0.0;
	public long tradeCount = 0;
	public double cumulativePriceVolume = 0.0;
}
