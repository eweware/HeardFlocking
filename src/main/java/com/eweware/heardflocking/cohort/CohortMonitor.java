package com.eweware.heardflocking.cohort;

import com.eweware.heardflocking.*;
import com.eweware.heardflocking.base.AzureConst;
import com.eweware.heardflocking.base.DBConst;
import com.eweware.heardflocking.base.HeardAzure;
import com.eweware.heardflocking.base.HeardDB;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DBCursor;
import com.mongodb.util.JSON;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by weihan on 11/3/14.
 */
public class CohortMonitor extends TimerTask {
    private String servicePrefix = "[CohortMonitor] ";

    public static void execute(HeardDB db, HeardAzure azure) {
        Timer timer = new Timer();
        Calendar cal = Calendar.getInstance();

        // set time to run
        cal.set(Calendar.HOUR_OF_DAY, ServiceProperties.CohortMonitor.START_HOUR);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        // set period
        int periodHours = ServiceProperties.CohortMonitor.PERIOD_HOURS;
        System.out.println("[CohortMonitor] start running, period=" + periodHours + " (minutes), time : "  + new Date());

        timer.schedule(new CohortMonitor(db, azure, cal.getTime(), periodHours), cal.getTime(), TimeUnit.HOURS.toMillis(periodHours));
    }

    private HeardDB db;
    private HeardAzure azure;

    private Date startTime;
    private int periodHours;

    private HashMap<String, String> groupNames;

    private boolean TEST_ONLY_TECH = ServiceProperties.TEST_ONLY_TECH;

    public CohortMonitor(HeardDB db, HeardAzure azure, Date startTime, int periodHours) {
        this.db = db;
        this.azure = azure;
        this.startTime = startTime;
        this.periodHours = periodHours;
        getGroups();
    }

    @Override
    public void run() {
        try {
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


            String groupPrefix = "[" + groupNames.get(groupId) + "]";
            System.out.print(servicePrefix + groupPrefix + " check need for re-clustering : ");

            if (groupNeedReclustering(group)) {
                System.out.println("YES");
                produceCohortTask(groupId);
            }
            else {
                System.out.println("NO");
            }
        }
        cursor.close();
        System.out.println();
        System.out.println(servicePrefix + "group scanning finished\n");
    }

    private boolean groupNeedReclustering(BasicDBObject group) {
        return true;
    }

    private void produceCohortTask(String groupId) throws StorageException {
        String groupPrefix = "[" + groupNames.get(groupId) + "] ";
        System.out.print(servicePrefix + groupPrefix + "produce cohort task... ");

        // produce inbox generation task
        BasicDBObject task = new BasicDBObject();
        task.put(AzureConst.CohortTask.TYPE, AzureConst.CohortTask.RECLUSTER);
        task.put(AzureConst.CohortTask.GROUP_ID, groupId);

        // enqueue
        CloudQueueMessage message = new CloudQueueMessage(JSON.serialize(task));
        azure.getCohortTaskQueue().addMessage(message);
        System.out.println("done");
    }

    private void printFinishInfo() {
        Calendar nextTime = Calendar.getInstance();
        nextTime.setTime(startTime);
        nextTime.add(Calendar.HOUR, periodHours);

        System.out.println(servicePrefix + "next scan in less than " + periodHours + " hours at time : " + nextTime.getTime());
    }
}
