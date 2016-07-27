package org.spiget.existence;

import java.io.IOException;

public class Main {

	public static void main(String... args) throws IOException {
		SpigetExistence spigetExistence = new SpigetExistence();
		spigetExistence.init();
		spigetExistence.check();
	}

}
