package gr.iti.mklab.sfc.storages;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import gr.iti.mklab.framework.common.domain.Item;

/**
 * Class for storing items to databases
 * 
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
	private Storage store = null;
	
	private BlockingQueue<Item> queue;
	
	public Consumer(BlockingQueue<Item> queue, Storage store) {
		this.store = store;
		this.queue = queue;
	}
	
	/**
	 * Stores an item if the latter is found waiting in the queue
	 */
	public void run() {			
		Item item = null;
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
	private void process(Item item) throws IOException {
		if (store != null) {
			
			store.store(item);
			//store.update(item);
			//store.delete(item.getId());

		}
	}
	
	/**
	 * Polls an item from the queue
	 * @return
	 */
	private Item poll() {			
		return queue.poll();		
	}
	
	/**
	 * Polls an item from the queue. Waits if the queue is empty. 
	 * @return
	 */
	private Item take() {				
		Item item = null;
		try {
			item = queue.take();
		} catch (InterruptedException e) {
			_logger.error(e);
		}	
		return item;
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
