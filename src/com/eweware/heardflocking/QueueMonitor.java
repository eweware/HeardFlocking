package com.eweware.heardflocking;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;

import java.nio.file.ClosedDirectoryStreamException;
import java.util.Date;

/**
 * Created by weihan on 10/30/14.
 */
public class QueueMonitor {

    private CloudQueueClient queueClient;
    private CloudQueue strengthTaskQueue;
    private CloudQueue inboxTaskQueue;

    private long CHECK_PERIOD_MILLIS = 1000;

    public static void main(String[] args) {
        new QueueMonitor().execute();
    }

    private void execute() {
        try {
            initializeQueue();
            while (true) {
                System.out.print(new Date());
                System.out.print("\tstrength queue : " + strengthTaskQueue.getApproximateMessageCount());
                System.out.println("\t\t\tinbox queue : " + inboxTaskQueue.getApproximateMessageCount());

                Thread.sleep(CHECK_PERIOD_MILLIS);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initializeQueue() throws Exception {
        System.out.print("Initializing Azure Storage Queue service...");

        // Retrieve storage account from connection-string.
        CloudStorageAccount storageAccount =
                CloudStorageAccount.parse(AzureConstants.STORAGE_CONNECTION_STRING);

        // Create the queue client.
        queueClient = storageAccount.createCloudQueueClient();

        // Retrieve a reference to a queue.
        strengthTaskQueue = queueClient.getQueueReference(AzureConstants.STRENGTH_TASK_QUEUE);
        inboxTaskQueue = queueClient.getQueueReference(AzureConstants.INBOX_TASK_QUEUE);

        // Create the queue if it doesn't already exist.
        strengthTaskQueue.createIfNotExists();
        inboxTaskQueue.createIfNotExists();

        System.out.println("done");
    }
}
