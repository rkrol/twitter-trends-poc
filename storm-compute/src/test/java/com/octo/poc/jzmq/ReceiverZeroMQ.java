package com.octo.poc.jzmq;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

public class ReceiverZeroMQ extends TestCase {
	static final Logger LOG = LoggerFactory.getLogger(ReceiverZeroMQ.class);

	public void testReceive() {
		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket receiver = context.socket(ZMQ.PULL);
		receiver.bind("tcp://127.0.0.1:4777");
		while (true) {
			System.out.println(new String(receiver.recv(0)));
		}
	}

}
