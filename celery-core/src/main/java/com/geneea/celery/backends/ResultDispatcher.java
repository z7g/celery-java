package com.geneea.celery.backends;



public class ResultDispatcher {
	
	public interface ResultReceiver {
		
		void recvResult(TaskResult payload);
		
	}

	private static ResultDispatcher instance = new ResultDispatcher();
	


	private ResultReceiver recv = null;

	private ResultDispatcher() {
	}

	public static ResultDispatcher getInstance() {
		return instance;
	}
	
	public void setReceiver(ResultReceiver receiver) {
		this.recv=receiver;
	}
	
	public void dispatch(TaskResult payload) {		
		if(recv!=null) {
			recv.recvResult(payload);
		}
	}
}
