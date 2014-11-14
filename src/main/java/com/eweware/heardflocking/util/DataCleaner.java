package com.eweware.heardflocking.util;

import com.eweware.heardflocking.ServiceProperties;
import com.eweware.heardflocking.base.DBConst;
import com.eweware.heardflocking.base.HeardAzure;
import com.eweware.heardflocking.base.HeardDB;
import com.microsoft.azure.storage.StorageException;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DBCursor;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by weihan on 11/13/14.
 */
public class DataCleaner extends TimerTask {
    private String servicePrefix = "[DataCleaner] ";

    public static void execute(HeardDB db, HeardAzure azure) {
        Timer timer = new Timer();
        Calendar cal = Calendar.getInstance();

        // set period
        int PERIOD_HOURS = ServiceProperties.DataCleaner.PERIOD_HOURS;

        System.out.println("[DataCleaner] start running, period=" + PERIOD_HOURS + " (hours), time : "  + new Date());

        timer.schedule(new DataCleaner(db, azure, cal.getTime(), PERIOD_HOURS), cal.getTime(), TimeUnit.HOURS.toMillis(PERIOD_HOURS));
    }

    public DataCleaner(HeardDB db, HeardAzure azure, Date startTime, int periodHours) {
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

    private final int RECENT_GENERATIONS_KEPT = ServiceProperties.DataCleaner.RECENT_GENERATIONS_KEPT;

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

        for (String groupId : groupNames.keySet()) {
            if (TEST_ONLY_TECH && !groupId.equals("522ccb78e4b0a35dadfcf73f")) continue;

            System.out.print(servicePrefix + "cleaning group '" + groupNames.get(groupId) + "' ... ");

            cleanUpGroup(groupId);
        }
        cursor.close();
        System.out.println();
        System.out.println(servicePrefix + "finish group scanning\n");
    }

    private void cleanUpGroup(String groupId) {
        List<GenerationInfo> cleanGenInfoList = getCleanGenIdList(groupId);
        if (cleanGenInfoList == null) {
            System.out.println("no generation needs to be cleaned");
            return;
        }
        for (GenerationInfo generationInfo : cleanGenInfoList) {
            cleanUpGeneration(generationInfo);
        }
    }

    private List<GenerationInfo> getCleanGenIdList(String groupId) {
        List<GenerationInfo> cleanGenList = new ArrayList<>();
        DBCursor cursor = db.getGenerationInfoCol().find(new BasicDBObject(DBConst.GenerationInfo.GROUP_ID, new ObjectId(groupId)));
        while (cursor.hasNext()) {
            cleanGenList.add(new GenerationInfo((BasicDBObject)cursor.next()));
        }
        // if less than certain number, just return null, nothing needs to be cleaned
        if (cleanGenList.size() <= RECENT_GENERATIONS_KEPT) return null;
        // sort by create time, ascending order, newer generations are in the end of the list
        Collections.sort(cleanGenList);
        // delete last ones
        for (int i = 0; i < RECENT_GENERATIONS_KEPT; i++) {
            cleanGenList.remove(cleanGenList.size() - 1);
        }
        return cleanGenList;
    }

    private class GenerationInfo implements Comparable<GenerationInfo> {
        ObjectId generationId;
        Date createTime;
        ObjectId groupId;
        List<ObjectId> cohortList;
        ObjectId defaultCohortId;

        GenerationInfo(BasicDBObject generationInfo) {
            generationId = generationInfo.getObjectId(DBConst.GenerationInfo.ID);
            createTime = generationInfo.getDate(DBConst.GenerationInfo.CREATE_TIME);
            groupId = generationInfo.getObjectId(DBConst.GenerationInfo.GROUP_ID);
            cohortList = (List<ObjectId>) generationInfo.get(DBConst.GenerationInfo.COHORT_LIST);
            defaultCohortId = generationInfo.getObjectId(DBConst.GenerationInfo.DEFAULT_COHORT);
        }

        @Override
        public int compareTo(GenerationInfo o) {
            return createTime.compareTo(o.createTime);
        }
    }

    private void cleanUpGeneration(GenerationInfo genInfo) {
        // add default cohort to cohortList for conveniently deleting documents/fields
        genInfo.cohortList.add(genInfo.defaultCohortId);

        // get lastInboxNumber and lastSafeInboxNumber of each cohort
        HashMap<ObjectId, Integer> lastInboxMap = new HashMap<>();
        HashMap<ObjectId, Integer> lastSafeInboxMap = new HashMap<>();
        for (ObjectId cohortId : genInfo.cohortList) {
            BasicDBObject cohort = (BasicDBObject) db.getCohortInfoCol().findOne(new BasicDBObject(DBConst.CohortInfo.ID, cohortId));
            if (cohort == null) {
                lastInboxMap.put(cohortId, -1);
                lastSafeInboxMap.put(cohortId, -1);
            }
            else {
                lastInboxMap.put(cohortId, cohort.getInt(DBConst.CohortInfo.LAST_INBOX, -1));
                lastSafeInboxMap.put(cohortId, cohort.getInt(DBConst.CohortInfo.LAST_SAFE_INBOX, -1));
            }
        }

        // delete document in cohortInfo collection
        db.getCohortInfoCol().remove(new BasicDBObject(DBConst.CohortInfo.ID, new BasicDBObject("$in", genInfo.cohortList)));

        // delete sub-document fields in blahInfo collection
        for (ObjectId cohortId : genInfo.cohortList) {
            BasicDBObject query = new BasicDBObject(DBConst.BlahInfo.STRENGTH + "." + cohortId.toString(), new BasicDBObject("$exists", true));
            BasicDBObject unset = new BasicDBObject("$unset", new BasicDBObject(DBConst.BlahInfo.STRENGTH + "." + cohortId.toString(), true));
            db.getBlahInfoCol().update(query, unset, false, true);
        }

        // delete sub-document fields in userGroupInfo collection
        // delete generationId
        BasicDBObject query = new BasicDBObject(DBConst.UserGroupInfo.COHORT_GENERATIONS + "." + genInfo.generationId.toString(), new BasicDBObject("$exists", true));
        BasicDBObject unset = new BasicDBObject("$unset", new BasicDBObject(DBConst.UserGroupInfo.COHORT_GENERATIONS + "." + genInfo.generationId.toString(), true));
        db.getUserGroupInfoCol().update(query, unset, false, true);
        // delete cohort-strength
        for (ObjectId cohortId : genInfo.cohortList) {
            query = new BasicDBObject(DBConst.UserGroupInfo.STRENGTH + "." + cohortId.toString(), new BasicDBObject("$exists", true));
            unset = new BasicDBObject("$unset", new BasicDBObject(DBConst.UserGroupInfo.STRENGTH + "." + cohortId.toString(), true));
            db.getUserGroupInfoCol().update(query, unset, false, true);
        }

        // delete collections in inboxdb
        for (ObjectId cohortId : genInfo.cohortList) {
            // normal inbox
            if (lastInboxMap.get(cohortId) != -1) {
                for (int i = 0; i <= lastInboxMap.get(cohortId); i++) {
                    String inboxName = genInfo.groupId.toString() + "-" + cohortId.toString() + "-" + String.format("%07d", i);
                    db.getInboxCollection(inboxName).drop();
                }
            }
            // safe inbox
            if (lastSafeInboxMap.get(cohortId) != -1) {
                for (int i = 0; i <= lastSafeInboxMap.get(cohortId); i++) {
                    String inboxName = genInfo.groupId.toString() + "-" + cohortId.toString() + "-safe-" + String.format("%07d", i);
                    db.getInboxCollection(inboxName).drop();
                }
            }
        }

        // delete document in generationInfo collection
        db.getGenerationInfoCol().remove(new BasicDBObject(DBConst.GenerationInfo.ID, genInfo.generationId));
    }

    private void printFinishInfo() {
        Calendar nextTime = Calendar.getInstance();
        nextTime.setTime(startTime);
        nextTime.add(Calendar.HOUR, periodHours);
        System.out.println(servicePrefix + "next scan in less than " + periodHours + " hours at time : " + nextTime.getTime());
    }
}
