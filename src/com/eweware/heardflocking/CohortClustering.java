package com.eweware.heardflocking;

import java.net.UnknownHostException;
import java.util.*;

import com.mongodb.*;

/**
 * Created by weihan.kong on 10/7/2014.
 */
public class CohortClustering {
    public static void main(String[] args) throws UnknownHostException {
        //KMeansClustering.test();
        instance.run();
    }
    static private CohortClustering instance = new CohortClustering();

    private void run() throws UnknownHostException {
        // open mongo connection
        initDatabase();

        // get group names
        getGroupNames();

        for (String id : groupNames.keySet()) { System.out.println(id + " : " + groupNames.get(id)); }

        // count number of blahs in each group
        // assign a vector index for each blah
        countAndIndexBlahInGroup();

        System.out.println("#blah");
        for (String groupId : countBlahInGroup.keySet()) {
            System.out.println(groupId + " : " + countBlahInGroup.get(groupId) + "\t" + groupNames.get(groupId));
        }


        // count number of users in each group
        // assign a index for each user
        countAndIndexUserInGroup();

        System.out.println("#users");
        for (String groupId : countUserInGroup.keySet()) {
            System.out.println(groupId + " : " + countUserInGroup.get(groupId) + "\t" + groupNames.get(groupId));
        }


        // compute user-blah interest
        computeInterestAll();

        /*
        System.out.println("#blah - check");
        for (String groupId : countBlahInEachGroupCheck.keySet()) {
            System.out.println(groupId + " : " + countBlahInEachGroupCheck.get(groupId) + "\t" + groupNames.get(groupId));
        }
        */


        // for each group, do k-means
        for (String groupId : groupNames.keySet()) {
            // convert interestBlahInUserInGroup into vectors, using indexBlahInGroup
            double[][] data = getInterestVectors(groupId);

            // k-means to cluster users
            System.out.println("Clustering for : " + groupNames.get(groupId));
            int[] cluster = KMeansClustering.run(data, numCohort);
            if (cluster == null) {
                System.out.println("Error : k-means return null");
                return;
            }
            // put result into cohort hashmap
            produceCohortHashMap(groupId, cluster);
        }


        // write cohort info into mongo
        outputCohortToMongo();
    }

    // mongo server and databases
    final String mongoServer = "localhost";
    final int mongoServerPort = 21191;
    MongoClient mongoClient;
    DB userDB;
    DB blahDB;

    // weights for user-blah interest
    double wO = 1.0;
    double wC = 5.0;
    double wP = 10.0;

    // assumed number of cohort for K-means clustering
    int numCohort = 2;

    HashMap<String, String> groupNames = new HashMap<String, String>();

    // interestBlahInUserInGroup : groupId -> (userId -> (blahId -> interest))
    HashMap<String, HashMap<String, HashMap<String, Double>>>
            interestBlahInUserInGroup = new HashMap<String, HashMap<String, HashMap<String, Double>>>();

    // indexBlahInGroup : groupId -> (blahId -> vectorIndex)
    HashMap<String, HashMap<String, Integer>>
            indexBlahInGroup = new HashMap<String, HashMap<String, Integer>>();
    // countBlahInGroup : groupId -> number of blahs in the group
    HashMap<String, Integer>
            countBlahInGroup = new HashMap<String, Integer>();

    HashMap<String, HashMap<String, Integer>>
            indexBlahInEachGroupCheck = new HashMap<String, HashMap<String, Integer>>();
    HashMap<String, Integer>
            countBlahInEachGroupCheck = new HashMap<String, Integer>();

    // indexUserInGroup : groupId -> (userId -> vectorIndex)
    HashMap<String, HashMap<String, Integer>>
            indexUserInGroup = new HashMap<String, HashMap<String, Integer>>();
    // countUserInGroup : groupId -> number of users in the group
    HashMap<String, Integer>
            countUserInGroup = new HashMap<String, Integer>();

    // cohort clustering result
    // groupId -> (userId -> cohortId)
    HashMap<String, HashMap<String, Integer>> cohort = new HashMap<String, HashMap<String, Integer>>();

    private void initDatabase() throws UnknownHostException {
        mongoClient = new MongoClient(mongoServer, mongoServerPort);
        userDB = mongoClient.getDB("userdb");
        blahDB = mongoClient.getDB("blahdb");
    }

    private void getGroupNames() {
        DBCollection groupsColl = userDB.getCollection("groups");
        DBCursor cursor = groupsColl.find();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            groupNames.put(obj.getObjectId("_id").toString(), obj.getString("N"));
        }
        cursor.close();
    }

    private void countAndIndexBlahInGroup() {
        DBCollection blahsColl = blahDB.getCollection("blahs");
        DBCursor cursor = blahsColl.find();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            String groupId = (String) obj.get("G");
            String blahId = obj.getObjectId("_id").toString();

            // count blah in each group
            // indexBlah : blahId -> vectorIndex
            HashMap<String, Integer> indexBlah = indexBlahInGroup.get(groupId);
            if (indexBlah == null) {
                indexBlahInGroup.put(groupId, new HashMap<String, Integer>());
                indexBlah = indexBlahInGroup.get(groupId);
                countBlahInGroup.put(groupId, 0);
            }
            // generate index for each blah
            if (indexBlah.get(blahId) == null) {
                int count = countBlahInGroup.get(groupId);
                indexBlah.put(blahId, count);
                count++;
                countBlahInGroup.put(groupId, count);
            }
        }
        cursor.close();
    }

    private void countAndIndexUserInGroup() {
        DBCollection userGroupsColl = userDB.getCollection("usergroups");
        DBCursor cursor = userGroupsColl.find();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();

            String status = (String) obj.get("S");
            if (!status.equals("A")) continue; // only cluster active user

            String groupId = (String) obj.get("G");
            String userId = (String) obj.get("U");

            // count user in each group
            // indexUser: userId -> index
            HashMap<String, Integer> indexUser = indexUserInGroup.get(groupId);
            if (indexUser == null) {
                indexUserInGroup.put(groupId, new HashMap<String, Integer>());
                indexUser = indexUserInGroup.get(groupId);
                countUserInGroup.put(groupId, 0);
            }

            // generate index for each user
            if (indexUser.get(userId) == null) {
                int count = countUserInGroup.get(groupId);
                indexUser.put(userId, count);
                count++;
                countUserInGroup.put(groupId, count);
            }
        }
        cursor.close();
    }

    private void computeInterestAll() {
        DBCollection userBlahInfoColl = userDB.getCollection("userBlahInfo");

        DBCursor cursor = userBlahInfoColl.find();

        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();

            double interest = computeInterest(obj);
            if (interest != 0) {
                //System.out.println(obj);
                //System.out.println(" : " + interest);
            }

            String groupId = (String) obj.get("G");
            String userId = (String) obj.get("U");
            String blahId = (String) obj.get("B");

            // interestBlahInUser: userId -> (blahId -> interest)
            HashMap<String, HashMap<String, Double>> interestBlahInUser = interestBlahInUserInGroup.get(groupId);
            if (interestBlahInUser == null) {
                interestBlahInUserInGroup.put(groupId, new HashMap<String, HashMap<String, Double>>());
                interestBlahInUser = interestBlahInUserInGroup.get(groupId);
            }

            // interestBlah : blahId -> interest
            HashMap<String, Double> interestBlah = interestBlahInUser.get(userId);
            if (interestBlah == null) {
                interestBlahInUser.put(userId, new HashMap<String, Double>());
                interestBlah = interestBlahInUser.get(userId);
            }

            // insert user-blah interest
            interestBlah.put(blahId, computeInterest(obj));

            // count blah in each user, check
            // indexBlah : blahId -> vectorIndex
            HashMap<String, Integer> indexBlah = indexBlahInEachGroupCheck.get(groupId);
            if (indexBlah == null) {
                indexBlahInEachGroupCheck.put(groupId, new HashMap<String, Integer>());
                indexBlah = indexBlahInEachGroupCheck.get(groupId);
                countBlahInEachGroupCheck.put(groupId, 0);
            }
            // generate index for each blah
            if (indexBlah.get(blahId) == null) {
                int count = countBlahInEachGroupCheck.get(groupId);
                indexBlah.put(blahId, count);
                count++;
                countBlahInEachGroupCheck.put(groupId, count);
            }
        }
        cursor.close();
    }

    private double computeInterest(BasicDBObject userBlah) {
        // open
        Long O = (Long)userBlah.get("O");
        O = O == null ? 0 : O;

        // comment
        Long C = (Long)userBlah.get("C");
        C = C == null ? 0 : C;

        // promote
        Long P = (Long)userBlah.get("P");
        P = P == null ? 0 : P;

        // compute interest strength
        return O * wO + C * wC + P * wP * Math.signum(P); // downvote shows big interest and disagree
        //return O * wO + C * wC + P * wP; // downvote shows no interest or dislike
    }

    private double[][] getInterestVectors(String groupId) {
        // default set to 0
        double[][] data = new double[countUserInGroup.get(groupId)][countBlahInGroup.get(groupId)];

        // interestBlahInUser : userId -> (blahId -> interest)
        HashMap<String, HashMap<String, Double>> interestBlahInUser = interestBlahInUserInGroup.get(groupId);

        // indexBlah : blahId -> index of the blah
        HashMap<String, Integer> indexBlah = indexBlahInGroup.get(groupId);

        // indexUser : userId -> index of the user
        HashMap<String, Integer> indexUser = indexUserInGroup.get(groupId);

        // for each user
        for (String userId : indexUserInGroup.get(groupId).keySet()) {
            // put user-blah interest into vectors
            // interestBlah : (blahId -> interest)
            HashMap<String, Double> interestBlah = interestBlahInUser.get(userId);
            // skip if user has no activity in this group
            if (interestBlah == null) continue;
            // for each blah that the user has interacted with
            for (String blahId : interestBlah.keySet()) {
                if (indexUser.get(userId) != null && indexBlah.get(blahId) != null)
                    data[indexUser.get(userId)][indexBlah.get(blahId)] = interestBlah.get(blahId);
            }
        }
        return data;
    }

    private void produceCohortHashMap(String groupId, int[] cluster) {
        // userId -> cohort
        HashMap<String, Integer> indexUser = indexUserInGroup.get(groupId);
        HashMap<String, Integer> userCohort = new HashMap<String, Integer>();
        for (String userId : indexUser.keySet()) {
            userCohort.put(userId, cluster[indexUser.get(userId)]);
        }
        cohort.put(groupId, userCohort);
    }

    private void outputCohortToMongo() {
        DBCollection cohortColl = userDB.getCollection("cohort");
        BulkWriteOperation builder = cohortColl.initializeUnorderedBulkOperation();

        for (String groupId : cohort.keySet()) {
            HashMap<String, Integer> userCohort = cohort.get(groupId);
            for (String userId : userCohort.keySet()) {
                builder.find(new BasicDBObject("U", userId).append("G", groupId)).upsert().replaceOne(
                        new BasicDBObject("U", userId).append("G", groupId).append("CH", userCohort.get(userId))
                );
            }
        }
        System.out.print("Writing cohort information to database...");
        builder.execute();
        System.out.println("done");
    }

}
