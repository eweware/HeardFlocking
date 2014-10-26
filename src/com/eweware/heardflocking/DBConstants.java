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
        public static final String AUTHOR_ID = "A";
        public static final String GROUP_ID = "G";
        public static final String STRENGTH_UPDATE_TIME = "Su";
        public static final String STRENGTH = "S";
        public static final String NEW_VIEWS = "V";
        public static final String NEW_OPENS = "O";
        public static final String NEW_COMMENTS = "C";
        public static final String NEW_UPVOTES = "P";
        public static final String NEW_DOWNVOTES = "D";
        public static final String NEW_COMMENT_UPVOTES = "CP";
        public static final String NEW_COMMENT_DOWNVOTES = "CD";
    }

    public interface UserGroupInfo {
        public static final String ID = "_id";
        public static final String USER_ID = "U";
        public static final String GROUP_ID = "G";
        public static final String COHORT_LIST = "CH";
        public static final String STRENGTH_UPDATE_TIME = "Su";
        public static final String STRENGTH = "S";
        public static final String NEW_VIEWS = "V";
        public static final String NEW_OPENS = "O";
        public static final String NEW_COMMENTS = "C";
        public static final String NEW_UPVOTES = "P";
        public static final String NEW_DOWNVOTES = "D";
        public static final String NEW_COMMENT_UPVOTES = "CP";
        public static final String NEW_COMMENT_DOWNVOTES = "CD";
    }

    public interface UserBlahInfo {
        public static final String ID = "_id";
        public static final String USER_ID = "U";
        public static final String BLAH_ID = "B";
        //public static final String AUTHOR_ID = "A";
        //public static final String GROUP_ID = "G";
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
        public static final String CURRENT_GENERATION = "CG";
    }

    public interface GenerationInfo {
        public static final String ID = "_id";
        public static final String COHORT_INBOX_INFO = "CHI";
    }
}
