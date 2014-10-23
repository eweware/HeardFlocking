package com.eweware.heardflocking;

/**
 * Created by weihan on 10/22/14.
 */
public interface DBConstants {

    public final String DEV_DB_SERVER = "localhost";
    public final String QA_DB_SERVER = "qa.db.blahgua.com";
    public final int DB_SERVER_PORT = 21191;

    public interface BlahInfo {
        public static final String BLAH_ID = "_id";
        public static final String CREATE_TIME = "c";
        public static final String NEW_ACTIVITY = "N";
    }

    public interface UserGroupInfo {
        public static final String ID = "_id";
        public static final String USER_ID = "U";
        public static final String GROUP_ID = "G";
        public static final String NEW_ACTIVITY = "N";

        public static final String OPEN_COUNT = "O";
    }


}
