package com.eweware.heardflocking.base;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;

/**
 * Created by weihan on 11/3/14.
 */
public class HeardDB {
    public HeardDB(String mode) throws UnknownHostException {
        String server;
        if (mode.equals("dev")) {
            server = DBConst.DEV_DB_SERVER;
        }
        else if (mode.equals("qa")) {
            server = DBConst.QA_DB_SERVER;
        }
        else if (mode.equals("prod")) {
            server = DBConst.PROD_DB_SERVER;
        }
        else {
            server = DBConst.DEV_DB_SERVER;
        }
        initializeMongoDB(server);
    }

    private String mode;

    private MongoClient mongoClient;
    private DB userDB;
    private DB blahDB;
    private DB infoDB;
    private DB statsDB;
    private DB inboxDB;

    private DBCollection groupsCol;
    private DBCollection userGroupCol;
    private DBCollection userBlahInfoOldCol;

    private DBCollection blahsCol;

    private DBCollection blahInfoCol;

    private DBCollection userGroupInfoCol;
    private DBCollection cohortInfoCol;
    private DBCollection generationInfoCol;
    private DBCollection userBlahStatsCol;


    private void initializeMongoDB(String server) throws UnknownHostException {
        System.out.print("initialize MongoDB...");

        mongoClient = new MongoClient(server, DBConst.DB_SERVER_PORT);

        userDB = mongoClient.getDB("userdb");
        blahDB = mongoClient.getDB("blahdb");
        infoDB = mongoClient.getDB("infodb");
        statsDB = mongoClient.getDB("statsdb");
        inboxDB = mongoClient.getDB("inboxdb");

        groupsCol = userDB.getCollection("groups");
        userGroupCol = userDB.getCollection("usergroups");
        userBlahInfoOldCol = userDB.getCollection("userBlahInfo");

        blahsCol = blahDB.getCollection("blahs");

        blahInfoCol = infoDB.getCollection("blahInfo");
        userGroupInfoCol = infoDB.getCollection("userGroupInfo");
        cohortInfoCol = infoDB.getCollection("cohortInfo");
        generationInfoCol = infoDB.getCollection("generationInfo");

        userBlahStatsCol = statsDB.getCollection("userblahstats");

        System.out.println("done");
    }

    public DBCollection getInboxCollection(String name) {
        return inboxDB.getCollection(name);
    }

    public DBCollection getGroupsCol() { return groupsCol; }

    public DBCollection getUserGroupCol() {
        return userGroupCol;
    }

    public DBCollection getUserBlahInfoOldCol() {
        return userBlahInfoOldCol;
    }

    public DBCollection getBlahsCol() {
        return blahsCol;
    }

    public DBCollection getBlahInfoCol() {
        return blahInfoCol;
    }

    public DBCollection getUserGroupInfoCol() {
        return userGroupInfoCol;
    }

    public DBCollection getCohortInfoCol() {
        return cohortInfoCol;
    }

    public DBCollection getGenerationInfoCol() {
        return generationInfoCol;
    }

    public DBCollection getUserBlahStatsCol() {
        return userBlahStatsCol;
    }
}
