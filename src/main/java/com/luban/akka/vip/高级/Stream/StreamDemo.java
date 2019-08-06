package com.luban.akka.vip.高级.Stream;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.concurrent.CompletionStage;

/**
 * *************书山有路勤为径***************
 * 鲁班学院
 * 往期资料加木兰老师  QQ: 2746251334
 * VIP课程加安其拉老师 QQ: 3164703201
 * 讲师：周瑜老师
 * *************学海无涯苦作舟***************
 */
public class StreamDemo {

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("sys");
        Materializer materializer = ActorMaterializer.create(system);

        // 数据来源、流的起点
        // Source 包括两个泛型：第一个泛型表示它产生的数据类型，第二个泛型表示运行时产生的其他辅助数据，假如没有则设置为 NotUsed。
        Source<Integer, NotUsed> source = Source.range(1, 5);

        // 定义sink，用来循环打印数据
        Sink<Integer, CompletionStage<Done>> sink1 = Sink.foreach(System.out::println);
        Sink<Integer, CompletionStage<Done>> sink2 = Sink.foreach(System.out::println);

        // 使用sink操作数据，此时会创建出一个RunnableGraph对象
        RunnableGraph<NotUsed> graph = source.alsoTo(sink1).to(sink2);

        // 当执行 RunnableGraph.run 方法时，需要传入一个 Materializer 对象，它主要用来给流分配 Actor 并驱动其执行。
        graph.run(materializer);
    }
}
