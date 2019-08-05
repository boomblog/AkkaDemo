package com.luban.akka.vip.入门;

import java.util.concurrent.*;

/**
 * *************书山有路勤为径***************
 * 鲁班学院
 * 往期资料加木兰老师  QQ: 2746251334
 * VIP课程加安其拉老师 QQ: 3164703201
 * 讲师：周瑜老师
 * *************学海无涯苦作舟***************
 */
public class JavaFutureTest {

    static class ThreadDemo extends Thread{
        @Override
        public void run() {
            System.out.println("run...");
        }
    }

    public static void main(String[] args) {

        ExecutorService executorService = Executors.newFixedThreadPool(4);

        Future<String> future = executorService.submit(new Callable<String>() {

            @Override
            public String call() throws Exception {
                throw new NullPointerException();
//                return "hello";
            }
        });

        try {
            String result = future.get();

            System.out.println(result);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (NullPointerException e) {
            System.out.println("null exception");
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }
}
