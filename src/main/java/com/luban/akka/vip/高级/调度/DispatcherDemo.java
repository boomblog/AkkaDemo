package com.luban.akka.vip.高级.调度;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.ConfigFactory;

/**
 * *************书山有路勤为径***************
 * 鲁班学院
 * 往期资料加木兰老师  QQ: 2746251334
 * VIP课程加安其拉老师 QQ: 3164703201
 * 讲师：周瑜老师
 * *************学海无涯苦作舟***************
 */
public class DispatcherDemo {

    static class SimpleDemo extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(msg -> {
                System.out.println(getSelf() + "--->" + msg + " " + Thread.currentThread().getName());
                Thread.sleep(5000);
            }).build();
        }
    }

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("sys", ConfigFactory.load("dispacher.conf"));
        for(int i=0;i<50;i++) {
            // 每个Actor一个线程池（池中只有一个线程）
//            ActorRef ref = system.actorOf(Props.create(SimpleDemo.class).withDispatcher("my-pinned-dispatcher"),"actorDemo"+i);
            // 公用一个线程池
            ActorRef ref = system.actorOf(Props.create(SimpleDemo.class).withDispatcher("my-forkjoin-dispatcher"),"actorDemo"+i);
//            ActorRef ref = system.actorOf(Props.create(SimpleDemo.class).withDispatcher("my-threadpool-dispatcher"),"actorDemo"+i);
            ref.tell("hello",ActorRef.noSender());
        }
    }
}
