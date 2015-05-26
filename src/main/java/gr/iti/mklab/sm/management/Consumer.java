package gr.iti.mklab.sm.management;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import gr.iti.mklab.sm.storages.Storage;

/**
 * Class for storing items to databases
 *
 * @author manosetro
 * @author ailiakop
 * @email manosetro@iti.gr
 * @email ailiakop@iti.gr
 */
public class Consumer extends Thread {

    private Logger _logger = Logger.getLogger(Consumer.class);

    private static int id = 0;

    private boolean isAlive = true;
    private List<Storage> storages = null;

    private BlockingQueue<gr.iti.mklab.simmo.core.Object> queue;

    public Consumer(BlockingQueue<gr.iti.mklab.simmo.core.Object> queue, List<Storage> storages) {
        this.storages = storages;
        this.queue = queue;
        this.setName("Consumer_" + (id++));
    }

    /**
     * Stores an item if the latter is found waiting in the queue
     */
    public void run() {
        gr.iti.mklab.simmo.core.Object post = null;
        while (isAlive) {
            try {
                post = take();
                if (post == null) {
                    _logger.error("Post is null.");
                } else {
                    process(post);
                }
            } catch (IOException e) {
                e.printStackTrace();
                _logger.error(e);
            }
        }

        //empty queue
        while ((post = poll()) != null) {
            try {
                process(post);
            } catch (IOException e) {
                e.printStackTrace();
                _logger.error(e);
            }
        }
    }

    /**
     * Stores an item to all available databases
     *
     * @param item
     * @throws IOException
     */
    private void process(gr.iti.mklab.simmo.core.Object post) throws IOException {
        if (storages != null) {
            for (Storage storage : storages) {
                storage.store(post);
            }
        }
    }

    /**
     * Polls an item from the queue
     *
     * @return
     */
    private gr.iti.mklab.simmo.core.Object poll() {
        return queue.poll();
    }

    /**
     * Polls an item from the queue. Waits if the queue is empty.
     *
     * @return
     */
    private gr.iti.mklab.simmo.core.Object take() {
        gr.iti.mklab.simmo.core.Object post = null;
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
        } catch (Exception e) {
            _logger.error(e);
        }
    }
}
