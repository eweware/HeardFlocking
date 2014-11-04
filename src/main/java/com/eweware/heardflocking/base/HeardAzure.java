package com.eweware.heardflocking.base;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

/**
 * Created by weihan on 11/3/14.
 */
public class HeardAzure {
    public HeardAzure(String mode) throws URISyntaxException, InvalidKeyException, StorageException {
        String accountName;
        String accountKey;
        if (mode.equals("dev")) {
            accountName = AzureConst.DEV_ACCOUNT_NAME;
            accountKey = AzureConst.DEV_ACCOUNT_KEY;
        }
        else if (mode.equals("qa")) {
            accountName = AzureConst.QA_ACCOUNT_NAME;
            accountKey = AzureConst.QA_ACCOUNT_KEY;
        }
        else if (mode.equals("prod")) {
            accountName = AzureConst.PROD_ACCOUNT_NAME;
            accountKey = AzureConst.PROD_ACCOUNT_KEY;
        }
        else {
            accountName = AzureConst.DEV_ACCOUNT_NAME;
            accountKey = AzureConst.DEV_ACCOUNT_KEY;
        }
        String storageConnectionString = "DefaultEndpointsProtocol=http;AccountName=" + accountName + ";AccountKey=" + accountKey;
        initializeQueue(storageConnectionString);
    }

    private CloudQueueClient queueClient;
    private CloudQueue cohortTaskQueue;
    private CloudQueue strengthTaskQueue;
    private CloudQueue inboxTaskQueue;

    private void initializeQueue(String storageConnectionString) throws URISyntaxException, InvalidKeyException, StorageException {
        System.out.print("initialize Azure Storage Queue service...");

        // Retrieve storage account from connection-string.
        CloudStorageAccount storageAccount =
                CloudStorageAccount.parse(storageConnectionString);

        // Create the queue client.
        queueClient = storageAccount.createCloudQueueClient();

        // Retrieve a reference to queues
        cohortTaskQueue = queueClient.getQueueReference(AzureConst.COHORT_TASK_QUEUE);
        strengthTaskQueue = queueClient.getQueueReference(AzureConst.STRENGTH_TASK_QUEUE);
        inboxTaskQueue = queueClient.getQueueReference(AzureConst.INBOX_TASK_QUEUE);

        // Create the queue if it doesn't already exist.
        cohortTaskQueue.createIfNotExists();
        strengthTaskQueue.createIfNotExists();
        inboxTaskQueue.createIfNotExists();

        System.out.println("done");
    }

    public CloudQueue getInboxTaskQueue() {
        return inboxTaskQueue;
    }

    public CloudQueue getCohortTaskQueue() {
        return cohortTaskQueue;
    }

    public CloudQueue getStrengthTaskQueue() {
        return strengthTaskQueue;
    }
}
