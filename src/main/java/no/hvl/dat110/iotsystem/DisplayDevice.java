package no.hvl.dat110.iotsystem;

import no.hvl.dat110.client.Client;
import no.hvl.dat110.messages.Message;
import no.hvl.dat110.messages.PublishMsg;
import no.hvl.dat110.common.TODO;

public class DisplayDevice {

	private static final int COUNT = 10;

	public static void main(String[] args) {

		System.out.println("Display starting ...");

		// create a client object and use it to
		Client client = new Client("display", Common.BROKERHOST, Common.BROKERPORT);

		// - connect to the broker - use "display" as the username
		client.connect();

		// - create the temperature topic on the broker
		client.createTopic("temperature");

		// - subscribe to the topic
		client.subscribe("temperature");

		for (int i = 0; i < COUNT; i++) {
			// - receive messages on the topic
			Message msg = client.receive();
			if (msg instanceof PublishMsg) {
				PublishMsg pubmsg = (PublishMsg) msg;
				System.out.println("DISPLAY: " + pubmsg.getMessage());
			}

			// sleep before receiving the next message
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// - unsubscribe from the topic
		client.unsubscribe("temperature");

		// - disconnect from the broker
		client.disconnect();

		System.out.println("Display stopping ... ");

		throw new UnsupportedOperationException(TODO.method());

	}
}
