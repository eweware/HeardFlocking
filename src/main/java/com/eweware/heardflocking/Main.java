package com.eweware.heardflocking;

import com.eweware.heardflocking.cohort.CohortWorker;
import com.eweware.heardflocking.inbox.InboxMonitor;
import com.eweware.heardflocking.strength.StrengthMonitor;
import com.eweware.heardflocking.strength.StrengthWorker;
import com.eweware.heardflocking.util.RandomNewActivity;
import com.eweware.heardflocking.util.TransferInfoData;

import java.io.*;
import java.util.Properties;

/**
 * Created by weihan on 11/1/14.
 */
public class Main {
    private String DB_SERVER;

    public static void main(String[] args) {
        new Main().execute(args);
    }

    private void execute(String[] args) {
        try {
            String service = parseArgument(args);
            if (service == null) return;

            getProperties();

            runService(service);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void getProperties() throws IOException {
        System.out.println("Load service properties");
        Properties prop = new Properties();
        String propFileName;

        if (DB_SERVER.equals(DBConstants.DEV_DB_SERVER)) propFileName = "./dev.properties";
        else if (DB_SERVER.equals(DBConstants.QA_DB_SERVER)) propFileName = "./qa.properties";
        else propFileName = "./prod.properties";

        FileInputStream is = new FileInputStream(propFileName);

        if (is == null) {
            throw new FileNotFoundException("property file '" + propFileName + "' not found");
        }
        prop.load(is);

        // global
        ServiceProperties.TEST_ONLY_TECH = Boolean.parseBoolean(prop.getProperty("test_only_tech", "false"));
        // TransferInfoData
        ServiceProperties.TransferInfoData.USER_BLAH_INFO_TO_STATS = Boolean.parseBoolean(prop.getProperty("td.user_blah_info_to_stats", "false"));
        // RandomNewActivity
        ServiceProperties.RandomNewActivity.BLAH_NEW_ACTIVITY = Boolean.parseBoolean(prop.getProperty("ra.blah_new_activity","false"));
        ServiceProperties.RandomNewActivity.USER_NEW_ACTIVITY = Boolean.parseBoolean(prop.getProperty("ra.user_new_activity","false"));
        ServiceProperties.RandomNewActivity.BLAH_WAIT_MILLIS = Long.parseLong(prop.getProperty("ra.blah_wait_millis", "1000"));
        ServiceProperties.RandomNewActivity.USER_WAIT_MILLIS = Long.parseLong(prop.getProperty("ra.user_wait_millis", "1000"));
        ServiceProperties.RandomNewActivity.BLAH_NEW_ACT_PROBABILITY = Double.parseDouble(prop.getProperty("ra.blah_new_act_probability", "0.01"));
        ServiceProperties.RandomNewActivity.User_NEW_ACT_PROBABILITY = Double.parseDouble(prop.getProperty("ra.user_new_act_probability", "0.01"));

        // CohortWorker
        ServiceProperties.CohortWorker.START_HOUR = Integer.parseInt(prop.getProperty("cw.start_hour", "0"));
        ServiceProperties.CohortWorker.PERIOD_HOURS = Integer.parseInt(prop.getProperty("cw.period_hours", "72"));
        ServiceProperties.CohortWorker.TRIVIAL_CLUSTERING = Boolean.parseBoolean(prop.getProperty("cw.trivial_clustering", "true"));
        ServiceProperties.CohortWorker.KMEANS_CLUSTERING = Boolean.parseBoolean(prop.getProperty("cw.kmeans_clustering", "false"));
        ServiceProperties.CohortWorker.RANDOM_CLUSTERING = Boolean.parseBoolean(prop.getProperty("cw.random_clustering", "false"));
        ServiceProperties.CohortWorker.NUM_COHORTS = Integer.parseInt(prop.getProperty("cw.num_cohorts", "1"));
        ServiceProperties.CohortWorker.RECENT_BLAH_DAYS = Integer.parseInt(prop.getProperty("cw.recent_blah_days", "365"));
        ServiceProperties.CohortWorker.WEIGHT_VIEW = Double.parseDouble(prop.getProperty("cw.weight_view", "1.0"));
        ServiceProperties.CohortWorker.WEIGHT_OPEN = Double.parseDouble(prop.getProperty("cw.weight_open", "2.0"));
        ServiceProperties.CohortWorker.WEIGHT_COMMENT = Double.parseDouble(prop.getProperty("cw.weight_comment", "5.0"));
        ServiceProperties.CohortWorker.WEIGHT_PROMOTION = Double.parseDouble(prop.getProperty("cw.weight_promotion", "10.0"));

        // StrengthMonitor
        ServiceProperties.StrengthMonitor.START_HOUR = Integer.parseInt(prop.getProperty("sm.start_hour", "0"));
        ServiceProperties.StrengthMonitor.PERIOD_HOURS = Integer.parseInt(prop.getProperty("sm.period_hours", "3"));
        ServiceProperties.StrengthMonitor.RECENT_BLAH_MONTHS = Integer.parseInt(prop.getProperty("sm.recent_blah_months", "24"));

        // StrengthWorker
        ServiceProperties.StrengthWorker.QUEUE_VISIBLE_TIMEOUT_SECONDS = Integer.parseInt(prop.getProperty("sw.queue_visible_timeout_seconds","30"));
        ServiceProperties.StrengthWorker.NO_TASK_WAIT_MILLIS = Long.parseLong(prop.getProperty("sw.no_task_wait_millis","10000"));
        ServiceProperties.StrengthWorker.RECENT_BLAH_DAYS = Integer.parseInt(prop.getProperty("sw.recent_blah_months","12"));
        ServiceProperties.StrengthWorker.WEIGHT_VIEW = Double.parseDouble(prop.getProperty("sw.weight_view", "1.0"));
        ServiceProperties.StrengthWorker.WEIGHT_OPEN = Double.parseDouble(prop.getProperty("sw.weight_open", "2.0"));
        ServiceProperties.StrengthWorker.WEIGHT_COMMENT = Double.parseDouble(prop.getProperty("sw.weight_comment", "5.0"));
        ServiceProperties.StrengthWorker.WEIGHT_PROMOTION = Double.parseDouble(prop.getProperty("sw.weight_promotion", "10.0"));

        // InboxMonitor
//        ServiceProperties.InboxMonitor.START_HOUR = Integer.parseInt(prop.getProperty("im.start_hour", "0"));
        ServiceProperties.InboxMonitor.PERIOD_HOURS = Integer.parseInt(prop.getProperty("im.period_hours", "1"));


    }

    private String parseArgument(String[] args) {
        if (args.length == 0 || args.length > 2) {
            System.out.println("Usage: java heard.jar service [dev|qa|prod]");
            System.out.println("service:");
            System.out.println("\tcw: CohortClusteringService");
            System.out.println("\tsm: StrengthMonitor");
            System.out.println("\tsw: StrengthTaskWorker");
            System.out.println("\tim: InboxMonitor");
            System.out.println("\ttd: TransferInfoData");
            System.out.println("\tra: RandomeNewActivity");
            System.out.println();
            return null;
        }
        else {
            if (args.length == 2) {
                if (args[1].equals("dev")) DB_SERVER = DBConstants.DEV_DB_SERVER;
                else if (args[1].equals("qa")) DB_SERVER = DBConstants.QA_DB_SERVER;
                else if (args[1].equals("prod")) DB_SERVER = DBConstants.PROD_DB_SERVER;
                else {
                    throw new IllegalArgumentException("Argument error : server can only be [dev|qa|prod] or not specified");
                }
                System.out.println("MongoDB server=" + args[1] + " host=" + DB_SERVER);
            }
            else {
                // defalut use dev server (localhost)
                DB_SERVER = DBConstants.DEV_DB_SERVER;
                System.out.println("MongoDB server=dev" + " host=" + DB_SERVER);
            }
            return args[0];
        }
    }

    private void runService(String service) {
        System.out.println("Run service=" + service);
        // choose process to run
        if (service.equals("cw")) {
            CohortWorker.execute(DB_SERVER);
        }
        else if (service.equals("sm")) {
            StrengthMonitor.execute(DB_SERVER);
        }
        else if (service.equals("sw")) {
            new StrengthWorker(DB_SERVER).execute();
        }
        else if (service.equals("im")) {
            InboxMonitor.execute(DB_SERVER);
        }
        else if (service.equals("td")) {
            new TransferInfoData(DB_SERVER).execute();
        }
        else if (service.equals("ra")) {
            new RandomNewActivity(DB_SERVER).execute();
        }
        else {
            throw new IllegalArgumentException("Argument error : process can only be [cw|sm|sw|im|td|ra]");
        }
    }
}
