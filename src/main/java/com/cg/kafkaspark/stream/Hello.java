package com.cg.kafkaspark.stream;

public class Hello {
    public static void main(String[] args) {
        String topic="kafkacasestudy";
        int numEvent=10;
        HelloProducer hp=new HelloProducer(topic,numEvent);
        HelloConsumer hc=new HelloConsumer(topic);
    }
}
