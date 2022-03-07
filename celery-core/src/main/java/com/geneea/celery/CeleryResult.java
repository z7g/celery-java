package com.geneea.celery;

import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.ListenableFuture;

public class CeleryResult {
	private ListenableFuture<?> _future = null;

	public CeleryResult(ListenableFuture<?> future) {
		_future = future;
	}

	public Object get() throws WorkerException {

		try {
			return _future.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			if (e.getCause() instanceof WorkerException) {
				throw (WorkerException) e.getCause();
			}
			else
				e.printStackTrace();
		}

		return null;
	}
}
