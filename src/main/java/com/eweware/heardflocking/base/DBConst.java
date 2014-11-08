package com.eweware.heardflocking.base;

/**
 * Created by weihan on 10/22/14.
 */
public interface DBConst {

    public final String DEV_DB_SERVER = "localhost";
    public final String QA_DB_SERVER = "qa.db.blahgua.com";
    public final String PROD_DB_SERVER = "";
    public final int DB_SERVER_PORT = 21191;

    public interface BlahInfo {
        public static final String ID = "_id";
        public static final String CREATE_TIME = "c";
        public static final String AUTHOR_ID = "A";
        public static final String GROUP_ID = "G";
        public static final String STRENGTH_UPDATE_TIME = "Su";
        public static final String STRENGTH = "S";

        public static final String NEW_ACTIVITY = "N";
        public static final String NEW_VIEWS = "V";
        public static final String NEW_OPENS = "O";
        public static final String NEW_COMMENTS = "C";
        public static final String NEW_UPVOTES = "P";
        public static final String NEW_DOWNVOTES = "N";
        public static final String NEW_COMMENT_UPVOTES = "CP";
        public static final String NEW_COMMENT_DOWNVOTES = "CN";

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
        public static final String COHORT_GENERATIONS = "CHG";
        public static final String STRENGTH_UPDATE_TIME = "Su";
        public static final String STRENGTH = "S";

        public static final String NEW_ACTIVITY = "N";
        public static final String NEW_VIEWS = "V";
        public static final String NEW_OPENS = "O";
        public static final String NEW_COMMENTS = "C";
        public static final String NEW_UPVOTES = "P";
        public static final String NEW_DOWNVOTES = "N";
        public static final String NEW_COMMENT_UPVOTES = "CP";
        public static final String NEW_COMMENT_DOWNVOTES = "CN";

        public static final String NEXT_CHECK_TIME = "k";
    }

    public interface CohortInfo {
        public static final String ID = "_id";
        public static final String NUM_USERS = "N";
        public static final String FIRST_INBOX = "F";
        public static final String LAST_INBOX = "L";
        public static final String FIRST_SAFE_INBOX = "FS";
        public static final String LAST_SAFE_INBOX = "LS";

//        public static final String USER_LIST = "U";
    }

    public interface GenerationInfo {
        public static final String ID = "_id";
        public static final String GROUP_ID = "G";
        public static final String CREATE_TIME = "c";
        public static final String COHORT_LIST = "CH";
        public static final String DEFAULT_COHORT = "D";
    }

    public interface UserBlahStats {
        public static final String ID = "_id";
        public static final String USER_ID = "U";
        public static final String BLAH_ID = "B";
        public static final String YEAR = "Y";
        public static final String MONTH = "M";
        public static final String DAY = "D";
        public static final String VIEWS = "V";
        public static final String OPENS = "O";
        public static final String COMMENTS = "C";
        public static final String UPVOTES = "P";
        public static final String DOWNVOTES = "N";
        public static final String COMMENT_UPVOTES = "CP";
        public static final String COMMENT_DOWNVOTES = "CN";
    }

    public interface Blahs {
        public static final String ID = "_id";
        public static final String GROUP_ID = "G";
    }

    public interface Groups {
        public static final String ID = "_id";
        public static final String NAME = "N";
        public static final String CURRENT_GENERATION = "CG";
    }


}
