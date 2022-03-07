package com.qdspr.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.geneea.celery.Celery;
import com.geneea.celery.WorkerException;

public class CeleryTest {
	
	private static Celery client=null;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		 client = Celery.builder()
				.brokerUri("amqp://admin:admin@localhost/test")
				.backendUri("rpc://admin:admin@localhost:5672/test")
		        .build();
	}

	@Test
	public void testAddTask() throws InterruptedException, ExecutionException, IOException {
		
		assertEquals(110,
				client.submit("tasks.add",new Object[]{100, 10}).get());
		
	}
	
	@Test
	public void testDivTask() throws InterruptedException, ExecutionException, IOException {
		
		assertEquals(10.0,
				client.submit("tasks.div", new Object[]{100, 10}).get());
	}
	
	@Test(expected = WorkerException.class)
	public void testWorkerException() throws WorkerException {

		client.apply("tasks.div", new Object[] { 100, 0 }).get();
		
	}
	
	@Test
	public void testZeroDivErrorTest()  {				
		
		try {
			client.apply("tasks.div", new Object[]{100, 10}).get();
		} catch (WorkerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
	}
	

}
