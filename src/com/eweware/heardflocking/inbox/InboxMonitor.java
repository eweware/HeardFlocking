package com.eweware.heardflocking.inbox;

import com.eweware.heardflocking.AzureConstants;
import com.eweware.heardflocking.DBConstants;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.mongodb.*;
import com.mongodb.util.JSON;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by weihan on 10/28/14.
 */
public class InboxMonitor extends TimerTask {

    private CloudQueueClient queueClient;
    private CloudQueue inboxTaskQueue;

    private MongoClient mongoClient;
    private DB userDB;
    private DB infoDB;

    private DBCollection groupsCol;
    private DBCollection blahInfoCol;
    private DBCollection userGroupInfoCol;

    private HashMap<String, String> groupNames;

    public static void main(String[] args) {
        Timer timer = new Timer();
        Calendar cal = Calendar.getInstance();

        // set time to run
//        cal.set(Calendar.HOUR_OF_DAY, 20);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        // set period
        int periodHours = 24;

        System.out.println("InboxMonitor set to run once for every " + periodHours + " hours, starting at "  + cal.getTime().toString());

        timer.schedule(new InboxMonitor(), cal.getTime(), TimeUnit.HOURS.toMillis(periodHours));
    }

    @Override
    public void run() {
        try {
            initializeQueue();
            initializeMongoDB();
            scanGroups();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initializeQueue() throws Exception {
        System.out.print("Initializing Azure Storage Queue service... ");

        // Retrieve storage account from connection-string.
        CloudStorageAccount storageAccount =
                CloudStorageAccount.parse(AzureConstants.STORAGE_CONNECTION_STRING);

        // Create the queue client.
        queueClient = storageAccount.createCloudQueueClient();

        // Retrieve a reference to a queue.
        inboxTaskQueue = queueClient.getQueueReference(AzureConstants.INBOX_TASK_QUEUE);

        // Create the queue if it doesn't already exist.
        inboxTaskQueue.createIfNotExists();

        System.out.println("done");
    }

    private void initializeMongoDB() throws UnknownHostException {
        System.out.print("Initializing MongoDB connection... ");

        mongoClient = new MongoClient(DBConstants.DEV_DB_SERVER, DBConstants.DEV_DB_SERVER_PORT);
        userDB = mongoClient.getDB("userdb");
        infoDB = mongoClient.getDB("infodb");

        groupsCol = userDB.getCollection("groups");

        blahInfoCol = infoDB.getCollection("blahInfo");
        userGroupInfoCol = infoDB.getCollection("userGroupInfo");

        System.out.println("done");

        // get group names
        groupNames = new HashMap<String, String>();
        DBCursor cursor = groupsCol.find();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            groupNames.put(obj.getObjectId("_id").toString(), obj.getString("N"));
        }
        cursor.close();
    }

    private void scanGroups() throws StorageException {
        System.out.println("### Start scanning groups...");
        // only look at blahs created within certain number of months
        Cursor cursor = groupsCol.find();

        while (cursor.hasNext()) {
            BasicDBObject group = (BasicDBObject) cursor.next();
            String groupId = group.getObjectId(DBConstants.Groups.ID).toString();

            System.out.print("Checking groupd <" + groupId + "> ... ");

            if (groupIsActive(group)) {
                System.out.print("active, producing task... ");

                // produce inbox generation task
                BasicDBObject task = new BasicDBObject();
                task.put(AzureConstants.InboxTask.TYPE, AzureConstants.InboxTask.GENERATE_INBOX);
                task.put(AzureConstants.InboxTask.GROUP_ID, groupId);

                // enqueue
                CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
                inboxTaskQueue.addMessage(message);
                System.out.println("done");
            }
            else {
                System.out.println("inactive, passed");
            }
        }
        cursor.close();
        System.out.println("### Finish group scanning.\n");
    }

    private boolean groupIsActive(BasicDBObject group) {
        return true;
    }
}