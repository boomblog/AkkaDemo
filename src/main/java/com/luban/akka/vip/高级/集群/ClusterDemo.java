package com.luban.akka.vip.高级.集群;

import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.ClusterEvent.*;
import akka.cluster.Member;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * *************书山有路勤为径***************
 * 鲁班学院
 * 往期资料加木兰老师  QQ: 2746251334
 * VIP课程加安其拉老师 QQ: 3164703201
 * 讲师：周瑜老师
 * *************学海无涯苦作舟***************
 */
public class ClusterDemo extends AbstractActor {

    // 得到当前集群对象
    Cluster cluster = Cluster.get(getContext().system());

    @Override
    public void preStart() {
        // 让当前 Actor 订阅 UnreachableMember 、 MemberEvent 事件，
        // 其中 UnreachableMember 事件会在某个节点被故障检测器（ failure detector ）认定为不可达（ unreachable ）时触发
        cluster.subscribe(getSelf(), ClusterEvent.initialStateAsEvents(), UnreachableMember.class, MemberEvent.class);
    }

    @Override
    public void postStop() {
        cluster.unsubscribe(getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().matchAny(message -> {
            if (message instanceof MemberUp) {
                MemberUp mUp = (MemberUp) message;
                Member m = mUp.member();
                System.out.println("-->Member " + m + "is Up: the role is " + m.roles());
            } else if (message instanceof UnreachableMember) {
                UnreachableMember mUnreachable = (UnreachableMember) message;
                Member m = mUnreachable.member();
                System.out.println("-->Member " + m + " detected as unreachable: the role is " + m.roles());
            } else if (message instanceof MemberRemoved) {
                MemberRemoved mRemoved = (MemberRemoved) message;
                Member m = mRemoved.member();
                System.out.println("-->Member " + m + " is Removed: the role is" + m.roles());
            } else if (message instanceof MemberEvent) {
                MemberEvent me = (MemberEvent) message;
                Member m = me.member();
                System.out.println("-->MemverEvent: " + me + "" + m.roles());
            } else {
                System.out.println("-->Other: " + message);
                unhandled(message);
            }
        }).build();
    }

    public static void main(String[] args) {
        String port = args[0];
        Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + port).withFallback(ConfigFactory.load("cluster.conf"));
        ActorSystem system = ActorSystem.create("sys", config);
        system.actorOf(Props.create(ClusterDemo.class), "clusterDemo" + port);
    }
}
