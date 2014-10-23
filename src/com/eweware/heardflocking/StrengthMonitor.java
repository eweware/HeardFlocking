package com.eweware.heardflocking;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.queue.*;
import com.mongodb.*;
import com.mongodb.util.JSON;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by weihan on 10/22/14.
 */
public class StrengthMonitor extends TimerTask {

    private CloudQueueClient queueClient;
    private CloudQueue strengthTaskQueue;

    private MongoClient mongoClient;
    private DB userDB;
    private DB infoDB;

    private DBCollection groupsCol;
    private DBCollection blahInfoCol;
    private DBCollection cohortInfoCol;
    private DBCollection generationInfoCol;
    private DBCollection userGroupInfoCol;
    private DBCollection userBlahInfoCol;

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

        System.out.println("StrengthMonitor set to run once for every " + periodHours + " hours, starting at "  + cal.getTime().toString());

        timer.schedule(new StrengthMonitor(), cal.getTime(), TimeUnit.HOURS.toMillis(periodHours));
    }

    @Override
    public void run() {
        try
        {
            initializeQueue();
            initializeMongoDB();

            scanBlahs();
            scanUsers();

//            /////////////////////////////////////////////////
//            // Create a message and add it to the queue.
//            for (int i = 0; i < 100; i++) {
//                CloudQueueMessage message = new CloudQueueMessage("Hello, World " + i);
//                strengthTaskQueue.addMessage(message);
//            }
//
//            /////////////////////////////////////////////////
//            // Peek at the next message.
////            CloudQueueMessage peekedMessage = queue.peekMessage();
////
////            // Output the message value.
////            if (peekedMessage != null)
////            {
////                System.out.println(peekedMessage.getMessageContentAsString());
////            }
//
//            /////////////////////////////////////////////////
//            // Download the approximate message count from the server.
//            strengthTaskQueue.downloadAttributes();
//
//            System.out.println("Name : " + strengthTaskQueue.getName());
//            System.out.println("Count : " + strengthTaskQueue.getApproximateMessageCount());
//            System.out.println("Storage Url : " + strengthTaskQueue.getStorageUri());
//            System.out.println("Url : " + strengthTaskQueue.getUri());
//
//            /////////////////////////////////////////////////
//
//            // Retrieve the first visible message in the queue.
//            while (true) {
//                CloudQueueMessage retrievedMessage = strengthTaskQueue.retrieveMessage();
//
//                if (retrievedMessage != null) {
//                    // Process the message in less than 30 seconds, and then delete the message.
//
//                    System.out.println(retrievedMessage.getMessageContentAsString());
//
//                    strengthTaskQueue.deleteMessage(retrievedMessage);
//                }
//                else {
//                    break;
//                }
//            }
        }
        catch (Exception e)
        {
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

        // Create the queue if it doesn't already exist.
        strengthTaskQueue.createIfNotExists();

        System.out.println("done");
    }

    private void initializeMongoDB() throws UnknownHostException {
        mongoClient = new MongoClient(DBConstants.DEV_DB_SERVER, DBConstants.DB_SERVER_PORT);
        userDB = mongoClient.getDB("userdb");
        infoDB = mongoClient.getDB("infodb");

        groupsCol = userDB.getCollection("groups");

        blahInfoCol = infoDB.getCollection("blahinfo");
        cohortInfoCol = infoDB.getCollection("cohortinfo");
        generationInfoCol = infoDB.getCollection("generationinfo");
        userGroupInfoCol = infoDB.getCollection("usergroupnfo");
        userBlahInfoCol = infoDB.getCollection("userblahinfo");

        // get group names
        groupNames = new HashMap<String, String>();
        DBCursor cursor = groupsCol.find();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            groupNames.put(obj.getObjectId("_id").toString(), obj.getString("N"));
        }
        cursor.close();
    }

    private void scanBlahs() throws StorageException {
        // only look at blahs created within certain number of months
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -12);
        Date earliestRelevantDate = cal.getTime();

        // query MongoDB
        BasicDBObject query = new BasicDBObject(DBConstants.BlahInfo.CREATE_TIME, new BasicDBObject("$gt", earliestRelevantDate)).append(DBConstants.BlahInfo.NEW_ACTIVITY, true);
        Cursor cursor = blahInfoCol.find(query);
        while (cursor.hasNext()) {
            BasicDBObject blahInfo = (BasicDBObject) cursor.next();
            if (!checkBlahStability(blahInfo)) {
                String blahId = (String) blahInfo.get(DBConstants.BlahInfo.BLAH_ID);
                // produce re-compute strength task
                BasicDBObject task = new BasicDBObject(AzureConstants.StrengthTask.TYPE, AzureConstants.StrengthTask.COMPUTE_BLAH_STRENGTH);
                task.append(AzureConstants.StrengthTask.BLAH_ID, blahId);
                // enqueue
                CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
                strengthTaskQueue.addMessage(message);
            }
        }
        cursor.close();
    }

    private void scanUsers() throws StorageException {
        // query MongoDB
        BasicDBObject query = new BasicDBObject(DBConstants.UserGroupInfo.NEW_ACTIVITY, true);
        Cursor cursor = userGroupInfoCol.find(query);
        while (cursor.hasNext()) {
            BasicDBObject userGroupInfo = (BasicDBObject) cursor.next();
            if (!checkUserStability(userGroupInfo)) {
                String userId = (String) userGroupInfo.get(DBConstants.UserGroupInfo.USER_ID);
                String groupId = (String) userGroupInfo.get(DBConstants.UserGroupInfo.GROUP_ID);
                // produce re-compute strength task
                BasicDBObject task = new BasicDBObject(AzureConstants.StrengthTask.TYPE, AzureConstants.StrengthTask.COMPUTE_USER_STRENGTH);
                task.append(AzureConstants.StrengthTask.USER_ID, userId);
                task.append(AzureConstants.StrengthTask.GROUP_ID, groupId);
                // enqueue
                CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
                strengthTaskQueue.addMessage(message);
            }
        }
        cursor.close();
    }

    private boolean checkBlahStability(BasicDBObject blahInfo) {
        return true;
    }

    private boolean checkUserStability(BasicDBObject userGroupInfo) {
        return true;
    }
}
