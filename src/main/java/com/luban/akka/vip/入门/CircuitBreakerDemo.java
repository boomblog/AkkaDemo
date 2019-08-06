package com.luban.akka.vip.入门;

import akka.actor.*;
import akka.pattern.CircuitBreaker;

import java.time.Duration;
import java.util.concurrent.Callable;

/**
 * *************书山有路勤为径***************
 * 鲁班学院
 * 往期资料加木兰老师  QQ: 2746251334
 * VIP课程加安其拉老师 QQ: 3164703201
 * 讲师：周瑜老师
 * *************学海无涯苦作舟***************
 */
public class CircuitBreakerDemo {

    static class CircuitBreakerActor extends AbstractActor {
        private ActorRef workerChild;
        private static SupervisorStrategy strategy = new OneForOneStrategy(20, Duration.ofMinutes(1), param -> SupervisorStrategy.resume());

        @Override
        public SupervisorStrategy supervisorStrategy() {
            return strategy;
        }

        @Override
        public void preStart() throws Exception {
            super.preStart();
            workerChild = getContext().actorOf(Props.create(WorkerActor.class), "workerActor");
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(message -> workerChild.tell(message, getSender())).build();
        }
    }

    static class WorkerActor extends AbstractActor {
        private CircuitBreaker breaker;

        @Override
        public void preStart() throws Exception {
            super.preStart();
            // 调用报错或超时（超过1秒）失败次数加1，超过3次后进入开启状态，30秒后进入半开启状态，如果在半开启状态中处理第一个请求成功，则关闭熔断器，如果失败则重回开启状态。
            this.breaker = new CircuitBreaker(getContext().dispatcher(), getContext().system().scheduler(),
                    3, Duration.ofSeconds(1), Duration.ofSeconds(15))
                    .onOpen(new Runnable() {
                        public void run() {
                            System.out.println("---> 熔断器开启");
                        }
                    }).onHalfOpen(new Runnable() {
                        @Override
                        public void run() {
                            System.out.println("---> 熔断器半开启");
                        }
                    }).onClose(new Runnable() {
                        @Override
                        public void run() {
                            System.out.println("---> 熔断器关闭");
                        }
                    });
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(msg -> {
                breaker.callWithSyncCircuitBreaker(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        if (msg.equals("error")) {
                            System.out.println("msg:" + msg);
                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        } else {
                            System.out.println("msg:" + msg);
                        }
                        return "success";
                    }
                });
            }).build();
        }
    }

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("sys");
        ActorRef actorRef = system.actorOf(Props.create(CircuitBreakerActor.class), "circuitBreakerDemo");

        for (int i = 0; i < 40; i++) {
            if (i > 4) {
                actorRef.tell("normal", ActorRef.noSender());
            } else {
                actorRef.tell("error", ActorRef.noSender());
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
