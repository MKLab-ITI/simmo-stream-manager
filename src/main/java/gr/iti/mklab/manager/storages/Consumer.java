package gr.iti.mklab.manager.storages;

import gr.iti.mklab.simmo.documents.Post;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

/**
 * Class for storing items to databases
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 * 
 * @author ailiakop
 * @email  ailiakop@iti.gr
 *
 */
public class Consumer extends Thread {
	
	private Logger _logger = Logger.getLogger(Consumer.class);
	
	private boolean isAlive = true;
	private List<Storage> storages = null;
	
	private BlockingQueue<Post> queue;
	
	public Consumer(BlockingQueue<Post> queue, List<Storage> storages) {
		this.storages = storages;
		this.queue = queue;
	}
	
	/**
	 * Stores an item if the latter is found waiting in the queue
	 */
	public void run() {			
		Post item = null;
		while (isAlive) {
			try {
				item = take();
				if (item == null) {
					_logger.error("Item is null.");
				} 
				else {
					process(item);
				}
			} catch(IOException e) {
				e.printStackTrace();
				_logger.error(e);
			}
		}
		
		//empty queue
		while ((item = poll()) != null) {
			try {
				process(item);
			} catch (IOException e) {
				e.printStackTrace();
				_logger.error(e);
			}
		}
	}
	
	/**
	 * Stores an item to all available databases
	 * @param item
	 * @throws IOException
	 */
	private void process(Post post) throws IOException {
		for(Storage storage : storages) {
			if (storage != null) {			
				storage.store(post);
			}
		}
	}
	
	/**
	 * Polls an item from the queue
	 * @return
	 */
	private Post poll() {			
		return queue.poll();		
	}
	
	/**
	 * Polls an item from the queue. Waits if the queue is empty. 
	 * @return
	 */
	private Post take() {				
		Post post = null;
		try {
			post = queue.take();
		} catch (InterruptedException e) {
			_logger.error(e);
		}	
		return post;
	}
	
	/**
	 * Stops the consumer thread
	 */
	public synchronized void die() {
		isAlive = false;
		try {
			this.interrupt();
		}
		catch(Exception e) {
			_logger.error(e);
		}
	}
}
