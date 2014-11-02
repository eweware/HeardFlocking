package com.eweware.heardflocking.inbox;

import com.eweware.heardflocking.AzureConstants;
import com.eweware.heardflocking.DBConstants;
import com.eweware.heardflocking.ServiceProperties;
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
    public InboxMonitor(String server, Date startTime, int periodHours) {
        DB_SERVER = server;
        this.startTime = startTime;
        this.periodHours = periodHours;
    }

    private final Date startTime;
    private int periodHours;

    private CloudQueueClient queueClient;
    private CloudQueue inboxTaskQueue;

    private String DB_SERVER;
    private MongoClient mongoClient;
    private DB userDB;
    private DB infoDB;

    private DBCollection groupsCol;
    private DBCollection blahInfoCol;
    private DBCollection userGroupInfoCol;

    private HashMap<String, String> groupNames;

    private final boolean TEST_ONLY_TECH = ServiceProperties.TEST_ONLY_TECH;

    private String servicePrefix = "[InboxMonitor] ";

    public static void execute(String server) {
        Timer timer = new Timer();
        Calendar cal = Calendar.getInstance();

        // set time to run
//        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        // set period
        int PERIOD_HOURS = ServiceProperties.InboxMonitor.PERIOD_HOURS;

        System.out.println("[InboxMonitor] start running, period=" + PERIOD_HOURS + " (hours), time : "  + new Date());

        timer.schedule(new InboxMonitor(server, cal.getTime(), PERIOD_HOURS), cal.getTime(), TimeUnit.HOURS.toMillis(PERIOD_HOURS));
    }

    @Override
    public void run() {
        try {
            System.out.println(startTime);

            initializeQueue();
            initializeMongoDB();
            scanGroups();

            Calendar nextTime = Calendar.getInstance();
            nextTime.setTime(startTime);
            nextTime.add(Calendar.HOUR, periodHours);
            System.out.println(servicePrefix + "next scan in less than " + periodHours + " hours at time : " + nextTime.getTime());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initializeQueue() throws Exception {
        System.out.print(servicePrefix + "initialize Azure Storage Queue service... ");

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
        System.out.print(servicePrefix + "initialize MongoDB connection...");

        mongoClient = new MongoClient(DB_SERVER, DBConstants.DB_SERVER_PORT);
        userDB = mongoClient.getDB("userdb");
        infoDB = mongoClient.getDB("infodb");

        groupsCol = userDB.getCollection("groups");

        blahInfoCol = infoDB.getCollection("blahInfo");
        userGroupInfoCol = infoDB.getCollection("userGroupInfo");

        System.out.println("done");

        // get group names
        groupNames = new HashMap<>();
        DBCursor cursor = groupsCol.find();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            groupNames.put(obj.getObjectId("_id").toString(), obj.getString("N"));
        }
        cursor.close();
    }

    private void scanGroups() throws StorageException {
        System.out.println(servicePrefix + "start scanning groups...");
        System.out.println();
        // only look at blahs created within certain number of months
        Cursor cursor = groupsCol.find();

        while (cursor.hasNext()) {
            BasicDBObject group = (BasicDBObject) cursor.next();
            String groupId = group.getObjectId(DBConstants.Groups.ID).toString();

            if (TEST_ONLY_TECH && !groupId.equals("522ccb78e4b0a35dadfcf73f")) continue;

            System.out.print(servicePrefix + "check group " + groupNames.get(groupId) + "' ... ");

            if (groupIsActive(group)) {
                produceInboxTask(groupId);
            }
            else {
                System.out.println("inactive, passed");
            }
        }
        cursor.close();
        System.out.println();
        System.out.println(servicePrefix + "finish group scanning\n");
    }

    private boolean groupIsActive(BasicDBObject group) {
        return true;
    }

    private void produceInboxTask(String groupId) throws StorageException {
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
}
