package org.example;

import org.example.common.Word;
import org.example.dataprocessing.WordExtractTask;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Phaser;

public class ConcurrentMainFunc {
    public static void main(String[] args){
        Date start, end;

        ConcurrentHashMap<String, Word> globalVocabulary = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Integer> globalKeywords = new ConcurrentHashMap<>();

        start = new Date();
        File source = new File("d:\\test\\data");

        File[] files = source.listFiles(f -> f.getName().endsWith(".txt"));
        if (files == null) {
            System.err.println("The 'data' folder not found!");
            return;
        }
        ConcurrentLinkedDeque<File> concurrentFileListStage1 = new ConcurrentLinkedDeque<>(Arrays.asList(files));
        ConcurrentLinkedDeque<File> concurrentFileListStage2 = new ConcurrentLinkedDeque<>(Arrays.asList(files));

        int numOfDocuments = files.length;

        System.out.println(concurrentFileListStage1.size());
        System.out.println(concurrentFileListStage2.size());

        int factor = 1;
        if (args.length > 0) {
            factor = Integer.parseInt(args[0]);
        }

        int numOfTasks = factor * Runtime.getRuntime().availableProcessors();
        System.out.println("numOfTasks : " + numOfTasks);
        Phaser phaser = new Phaser();

        Thread[] threads = new Thread[numOfTasks];
        WordExtractTask[] tasks = new WordExtractTask[numOfTasks];

        //i=0为主任务
        for (int i = 0; i < numOfTasks; i++) {
            tasks[i] = new WordExtractTask(concurrentFileListStage1, concurrentFileListStage2, phaser, globalVocabulary,
                    globalKeywords, concurrentFileListStage1.size(), "Task " + i, i==0);
            phaser.register();
            System.out.println(phaser.getRegisteredParties() + " tasks arrived to the Phaser.");
        }

        //创建多任务线程并启动
        for (int i = 0; i < numOfTasks; i++) {
            threads[i] = new Thread(tasks[i]);
            threads[i].start();
        }

        //在主线程中调用join，等待启动的子线程执行结束
        for (int i = 0; i < numOfTasks; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Phaser Is Terminated: " + phaser.isTerminated());

        end = new Date();
        System.out.println("Execution Time: " + (end.getTime() - start.getTime()));
        System.out.println("Vocabulary Size: " + globalVocabulary.size());
        System.out.println("Keyword Size: " + globalKeywords.size());
        System.out.println("Number of Documents: " + numOfDocuments);
    }
}
