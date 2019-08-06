package com.luban.akka.vip.高级.邮箱;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.dispatch.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.Option;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * *************书山有路勤为径***************
 * 鲁班学院
 * 往期资料加木兰老师  QQ: 2746251334
 * VIP课程加安其拉老师 QQ: 3164703201
 * 讲师：周瑜老师
 * *************学海无涯苦作舟***************
 */

class CustomMailbox {

    static class BusinessMsgQueue implements MessageQueue {
        private Queue<Envelope> queue = new ConcurrentLinkedQueue<Envelope>();

        // 假如在投递消息的过程中 Actor 不可用了，那么没有投递成功的消息会通过 cleanUp 进入死信队列
        @Override
        public void cleanUp(ActorRef owner, MessageQueue deadLetters) {
            for (Envelope el : queue) {
                deadLetters.enqueue(owner, el);
            }
        }

        // 出队
        @Override
        public Envelope dequeue() {
            return queue.poll();
        }

        // 入队
        @Override
        public void enqueue(ActorRef receiver, Envelope el) {
            queue.offer(el);
        }

        @Override
        public boolean hasMessages() {
            return !queue.isEmpty();
        }

        @Override
        public int numberOfMessages() {
            return queue.size();
        }
    }

    static class BusinessMailBoxType implements MailboxType, ProducesMessageQueue<BusinessMsgQueue> {

        public BusinessMailBoxType(ActorSystem.Settings settings, Config config) {
        }

        @Override
        public MessageQueue create(Option<ActorRef> arg0, Option<ActorSystem> arg1) {
            return new BusinessMsgQueue();
        }
    }
}

class PriorityMailBox extends UnboundedStablePriorityMailbox {

    public PriorityMailBox(ActorSystem.Settings settings, Config config) {
        /**
         * 返回值越小 优先级越高
         */
        super(new PriorityGenerator() {
            @Override
            public int gen(Object message) {
                if (message.equals("张三")) {
                    return 0;
                } else if (message.equals("李四")) {
                    return 1;
                } else if (message.equals("王五")) {
                    return 2;
                } else {
                    return 3;
                }
            }
        });
    }
}

public class MailBoxDemo {

    // ControlMessage 接口并没有需要实现的 API，仅仅是一个标志而已。
    static class ControlMsg implements ControlMessage {
        private final String status;

        public ControlMsg(String status) {
            this.status = status;
        }

        @Override
        public String toString() {
            return this.status;
        }
    }

    static class SimpleActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(System.out::println).build();
        }
    }

    public static void main(String[] args) {
        ActorSystem sys = ActorSystem.create("sys", ConfigFactory.load("mailbox.conf"));

//        ActorRef simpleActor = sys.actorOf(Props.create(SimpleActor.class), "simpleActor");
//        ActorRef simpleActor = sys.actorOf(Props.create(SimpleActor.class).withMailbox("test-mailbox"), "simpleActor");
//        ActorRef simpleActor = sys.actorOf(Props.create(SimpleActor.class).withMailbox("prio-mailbox"), "simpleActor");

//        Object[] messages = {"王五", "李四", "张三", "小二"};
//        for (Object msg : messages) {
//            simpleActor.tell(msg, ActorRef.noSender());
//        }


//        ActorRef simpleActor = sys.actorOf(Props.create(SimpleActor.class).withMailbox("control-aware-mailbox"), "simpleActor");
//        Object[] messages = {"Java", "C#", new ControlMsg("ServerPage"), "PHP"};
//        for (Object msg : messages) {
//            simpleActor.tell(msg, ActorRef.noSender());
//        }
    }
}
