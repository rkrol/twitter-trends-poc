package com.octo.poc.jzmq;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

public class SenderZeroMQ extends TestCase {
	static final Logger LOG = LoggerFactory.getLogger(SenderZeroMQ.class);

	public void testSend() {
		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket sender = context.socket(ZMQ.PUSH);
		sender.connect("tcp://127.0.0.1:4777");
		sender.send("{ \"test\": \"0\" }".getBytes(), 0);
	}
}
