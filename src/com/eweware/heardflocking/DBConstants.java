package com.eweware.heardflocking;

/**
 * Created by weihan on 10/22/14.
 */
public interface DBConstants {

    public final String DEV_DB_SERVER = "localhost";
    public final String QA_DB_SERVER = "qa.db.blahgua.com";
    public final String PROD_DB_SERVER = "";
    public final int DEV_DB_SERVER_PORT = 21191;
    public final int QA_DB_SERVER_PORT = 21191;
    public final int PROD_DB_SERVER_PORT = 21191;

    public interface BlahInfo {
        public static final String ID = "_id";
        public static final String CREATE_TIME = "c";
        public static final String AUTHOR_ID = "A";
        public static final String GROUP_ID = "G";
        public static final String STRENGTH_UPDATE_TIME = "Su";
        public static final String STRENGTH = "S";

        public static final String NEW_ACTIVITY = "N";
        public static final String NEW_VIEWS = "N.V";
        public static final String NEW_OPENS = "N.O";
        public static final String NEW_COMMENTS = "N.C";
        public static final String NEW_UPVOTES = "N.U";
        public static final String NEW_DOWNVOTES = "N.D";
        public static final String NEW_COMMENT_UPVOTES = "N.CU";
        public static final String NEW_COMMENT_DOWNVOTES = "N.CD";

        public static final String NEXT_CHECK_TIME = "k";

        public static final String TEXT = "T";
        public static final String TYPE_ID = "Y";
        public static final String IMAGE_IDS = "M";
        public static final String BADGE_IDS = "B";
        public static final String MATURE_FLAG = "XXX";
    }

    public interface UserGroupInfo {
        public static final String ID = "_id";
        public static final String USER_ID = "U";
        public static final String GROUP_ID = "G";
        public static final String COHORT_LIST = "CH";
        public static final String NEXT_GENERATION_COHORT_LIST = "CHN";
        public static final String STRENGTH_UPDATE_TIME = "Su";
        public static final String STRENGTH = "S";

        public static final String NEW_ACTIVITY = "N";
        public static final String NEW_VIEWS = "N.V";
        public static final String NEW_OPENS = "N.O";
        public static final String NEW_COMMENTS = "N.C";
        public static final String NEW_UPVOTES = "N.U";
        public static final String NEW_DOWNVOTES = "N.D";
        public static final String NEW_COMMENT_UPVOTES = "N.CU";
        public static final String NEW_COMMENT_DOWNVOTES = "N.CD";

        public static final String NEXT_CHECK_TIME = "k";
    }

    public interface CohortInfo {
        public static final String ID = "_id";
        public static final String NUM_USERS = "N";
        public static final String USER_LIST = "U";
    }

    public interface GenerationInfo {
        public static final String ID = "_id";
        public static final String GROUP_ID = "G";
        public static final String CREATE_TIME = "c";
        public static final String COHORT_INFO = "CHI";
        public static final String FIRST_INBOX = "F";
        public static final String LAST_INBOX = "L";
    }

    public interface UserBlahStats {
        public static final String ID = "_id";
        public static final String USER_ID = "U";
        public static final String BLAH_ID = "B";
        public static final String VIEWS = "V";
        public static final String OPENS = "O";
        public static final String COMMENTS = "C";
        public static final String PROMOTION = "P";
    }

    public interface Blahs {
        public static final String ID = "_id";
        public static final String GROUP_ID = "G";
    }

    public interface Groups {
        public static final String ID = "_id";
        public static final String NAME = "N";
        public static final String CURRENT_GENERATION = "CG";
        public static final String NEXT_GENERATION = "NG";
    }


}
