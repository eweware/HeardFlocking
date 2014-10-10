package com.eweware.heardflocking;

import java.net.UnknownHostException;

/**
 * Created by weihan on 10/10/14.
 */
public class Main {
    public static void main(String[] args) {
        String server = "localhost";
        int port = 21191;

        String groupId = "522ccb78e4b0a35dadfcf73f";

        try {
            new CohortClustering(server, port, groupId).run();
            //new CohortClusteringAllGroup().run();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.out.println(e.toString());
        }
    }
}
