package org.example;

public class Config {
    public static int range = 1000_000;
    public static int messageCount = 250_000_000;
    public static int producerThreadCount = 8;
    public static int consumerThreadCount = 8;
    public static int aggregateReceiveThreadCount = 2;
    public static int nodeCount = 3;
    public static String mainNode = "172.15.10.63";
    public static String myNode = "172.15.10.61";
}
