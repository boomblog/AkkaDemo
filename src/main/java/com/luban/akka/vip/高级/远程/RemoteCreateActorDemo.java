package com.luban.akka.vip.高级.远程;

import akka.actor.*;
import akka.remote.RemoteScope;
import com.typesafe.config.ConfigFactory;

/**
 * *************书山有路勤为径***************
 * 鲁班学院
 * 往期资料加木兰老师  QQ: 2746251334
 * VIP课程加安其拉老师 QQ: 3164703201
 * 讲师：周瑜老师
 * *************学海无涯苦作舟***************
 */
public class RemoteCreateActorDemo {

    static class RmtCreateActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(msg -> {
                System.out.println("remote msg====" + msg);
            }).build();
        }
    }

    public static void main(String[] args) {
//        ActorSystem system = ActorSystem.create("sys", ConfigFactory.load("remoteactor.conf"));
//        ActorRef ref = system.actorOf(Props.create(RmtCreateActor.class), "rmtCrtActor");
//        ref.tell("hello rmt", ActorRef.noSender());


        ActorSystem system = ActorSystem.create("sys", ConfigFactory.load("remoteactor.conf"));
        Address address = new Address("akka.tcp", "sys", "127.0.0.1", 2552);
        ActorRef ref = system.actorOf(Props.create(RmtCreateActor.class).withDeploy(new Deploy(new RemoteScope(address))), "rmtCrtActor");
        ref.tell("hello rmt", ActorRef.noSender());
    }
}
