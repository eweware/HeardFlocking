package com.eweware.heardflocking.inbox;

import com.eweware.heardflocking.base.AzureConst;
import com.eweware.heardflocking.base.DBConst;
import com.eweware.heardflocking.ServiceProperties;
import com.eweware.heardflocking.base.HeardAzure;
import com.eweware.heardflocking.base.HeardDB;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by weihan on 10/28/14.
 */
public class InboxMonitor extends TimerTask {
    private String servicePrefix = "[InboxMonitor] ";

    public static void execute(HeardDB db, HeardAzure azure) {
        Timer timer = new Timer();
        Calendar cal = Calendar.getInstance();

        // set period
        int PERIOD_HOURS = ServiceProperties.InboxMonitor.PERIOD_HOURS;

        System.out.println("[InboxMonitor] start running, period=" + PERIOD_HOURS + " (hours), time : "  + new Date());

        timer.schedule(new InboxMonitor(db, azure, cal.getTime(), PERIOD_HOURS), cal.getTime(), TimeUnit.HOURS.toMillis(PERIOD_HOURS));
    }

    public InboxMonitor(HeardDB db, HeardAzure azure, Date startTime, int periodHours) {
        this.db = db;
        this.azure = azure;
        this.startTime = startTime;
        this.periodHours = periodHours;
        getGroups();
    }

    private HeardDB db;
    private HeardAzure azure;
    private final Date startTime;
    private int periodHours;

    private HashMap<String, String> groupNames;

    private final boolean TEST_ONLY_TECH = ServiceProperties.TEST_ONLY_TECH;


    @Override
    public void run() {
        try {
            System.out.println(startTime);

            scanGroups();

            printFinishInfo();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void getGroups() {
        groupNames = new HashMap<>();
        DBCursor cursor = db.getGroupsCol().find();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            groupNames.put(obj.getObjectId("_id").toString(), obj.getString("N"));
        }
        cursor.close();
    }

    private void scanGroups() throws StorageException {
        System.out.println(servicePrefix + "start scanning groups...");
        System.out.println();

        Cursor cursor = db.getGroupsCol().find();

        while (cursor.hasNext()) {
            BasicDBObject group = (BasicDBObject) cursor.next();
            String groupId = group.getObjectId(DBConst.Groups.ID).toString();

            if (TEST_ONLY_TECH && !groupId.equals("522ccb78e4b0a35dadfcf73f")) continue;

            System.out.print(servicePrefix + "check group " + groupNames.get(groupId) + "' ... ");

            if (groupNeedNewInbox(group)) {
                String generationId = getCurrentGeneration(groupId);
                if (generationId == null) {
                    System.out.println("no generation available, skipped");
                    continue;
                }
                System.out.print("active, ");
                produceInboxTask(groupId, generationId);
            }
            else {
                System.out.println("inactive, passed");
            }
        }
        cursor.close();
        System.out.println();
        System.out.println(servicePrefix + "finish group scanning\n");
    }


    private String getCurrentGeneration(String groupId) {
        BasicDBObject query = new BasicDBObject(DBConst.Groups.ID, new ObjectId(groupId));
        BasicDBObject group = (BasicDBObject) db.getGroupsCol().findOne(query);
        return group.getString(DBConst.Groups.CURRENT_GENERATION, null);
    }

    // TODO
    private boolean groupNeedNewInbox(BasicDBObject group) {
        return true;
    }

    private void produceInboxTask(String groupId, String generationId) throws StorageException {
        System.out.print("active, produce task : RECLUSTER... ");

        // produce inbox generation task
        BasicDBObject task = new BasicDBObject();
        task.put(AzureConst.InboxTask.TYPE, AzureConst.InboxTask.GENERATE_INBOX);
        task.put(AzureConst.InboxTask.GROUP_ID, groupId);
        task.put(AzureConst.InboxTask.GENERATION_ID, generationId);

        // enqueue
        CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
        azure.getInboxTaskQueue().addMessage(message);
        System.out.println("done");
    }
    private void printFinishInfo() {
        Calendar nextTime = Calendar.getInstance();
        nextTime.setTime(startTime);
        nextTime.add(Calendar.HOUR, periodHours);
        System.out.println(servicePrefix + "next scan in less than " + periodHours + " hours at time : " + nextTime.getTime());
    }
}
