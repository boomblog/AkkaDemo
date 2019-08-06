package com.luban.akka.vip.高级.集群;

import akka.actor.ActorPath;
import akka.actor.ActorPaths;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.client.ClusterClient;
import akka.cluster.client.ClusterClientSettings;
import akka.dispatch.OnSuccess;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * *************书山有路勤为径***************
 * 鲁班学院
 * 往期资料加木兰老师  QQ: 2746251334
 * VIP课程加安其拉老师 QQ: 3164703201
 * 讲师：周瑜老师
 * *************学海无涯苦作舟***************
 */
public class UserClient {

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("sys", ConfigFactory.load("remote.conf"));

        Set<ActorPath> initContacts = new HashSet<ActorPath>(
                Arrays.asList(
                        ActorPaths.fromString("akka.tcp://sys@127.0.0.1:2551/system/receptionist")));
        ActorRef c = system.actorOf(ClusterClient.props(ClusterClientSettings.create(system).withInitialContacts(initContacts)), "client1");

        Timeout timeout = new Timeout(Duration.create(2, TimeUnit.SECONDS));
        for (int i=0; i<100; i++) {
            Future<Object> f = Patterns.ask(c, new ClusterClient.Send("/user/router", "hello"), timeout);
            f.onSuccess(new OnSuccess<Object>() {
                @Override
                public void onSuccess(Object result) throws Throwable {
                    System.out.println( result);
                }
            }, system.getDispatcher());

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
