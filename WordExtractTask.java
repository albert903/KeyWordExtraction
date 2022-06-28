package org.example.dataprocessing;

import org.example.common.Document;
import org.example.common.DocumentTokenize;
import org.example.common.KeyWord;
import org.example.common.Word;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Phaser;

public class WordExtractTask implements Runnable{
    private ConcurrentHashMap<String, Word> globalVocabulary;
    private ConcurrentHashMap<String, Integer> globalKeywords;

    private ConcurrentLinkedDeque<File> concurrentFileListStage1;
    private ConcurrentLinkedDeque<File> concurrentFileListStage2;

    private Phaser phaser;

    private String taskName;
    private boolean isMainTask;

    private int numOfParsedDocument;
    private int numOfDocument;

    public WordExtractTask(
            ConcurrentLinkedDeque<File> concurrentFileListStage1,
            ConcurrentLinkedDeque<File> concurrentFileListStage2,
            Phaser phaser,
            ConcurrentHashMap<String, Word> globalVocabulary,
            ConcurrentHashMap<String, Integer> globalKeywords,
            int numOfDocument,
            String taskName,
            boolean isMainTask) {
        this.concurrentFileListStage1 = concurrentFileListStage1;
        this.concurrentFileListStage2 = concurrentFileListStage2;
        this.globalVocabulary = globalVocabulary;
        this.globalKeywords = globalKeywords;
        this.phaser = phaser;
        this.isMainTask = isMainTask;
        this.taskName = taskName;
        this.numOfDocument = numOfDocument;
        System.out.println(taskName+" is main task: "+isMainTask);
    }
    @Override
    public void run() {
        File file;

        // Phase 1
        //遍历每个文档计算Tf，Df
        phaser.arriveAndAwaitAdvance();
        System.out.println(taskName + ": Phase 1");
        while ((file = concurrentFileListStage1.poll()) != null) {
            Document doc = DocumentTokenize.tokenize(file.getAbsolutePath());
            for (Word word : doc.getVocabulary().values()) {
                globalVocabulary.merge(word.getWord(), word, Word::merge);
            }
            numOfParsedDocument++;
        }

        System.out.println(taskName + ": " + numOfParsedDocument + " parsed.");

        // Phase 2
        //遍历每个文档计算Tf-Idf，并取其排好序的前5个关键词
        phaser.arriveAndAwaitAdvance();
        System.out.println(taskName + ": Phase 2");

        while ((file = concurrentFileListStage2.poll()) != null) {
            Document doc = DocumentTokenize.tokenize(file.getAbsolutePath());
            List<Word> keywords = new ArrayList<>(doc.getVocabulary().values());

            for (Word word : keywords) {
                Word globalWord = globalVocabulary.get(word.getWord());
                word.setTfIdf(globalWord.getDf(), numOfDocument);
            }
            //按Tf-Idf值排序
            Collections.sort(keywords);

            //取前5个
            if(keywords.size() > 5) keywords = keywords.subList(0, 5);
            for (Word word : keywords) {
                addKeyword(globalKeywords, word.getWord());
            }

        }
        System.out.println(taskName + ": " + numOfParsedDocument + " parsed.");

        if (isMainTask) {
            // Phase 3
            //构造排序的KeyWord数组，并重新按Df值排序，得到一个最佳关键词列表
            phaser.arriveAndAwaitAdvance();

            Iterator<Map.Entry<String, Integer>> iterator = globalKeywords
                    .entrySet().iterator();
            KeyWord[] orderedGlobalKeywords = new KeyWord[globalKeywords.size()];
            int index = 0;
            while (iterator.hasNext()) {
                Map.Entry<String, Integer> entry = iterator.next();
                KeyWord keyword = new KeyWord();
                keyword.setWord(entry.getKey());
                keyword.setDf(entry.getValue());
                orderedGlobalKeywords[index] = keyword;
                index++;
            }

            System.out.println("Keyword Size: " + orderedGlobalKeywords.length);

            //按Df值排序
            Arrays.parallelSort(orderedGlobalKeywords);
            int counter = 0;
            for (KeyWord keyword : orderedGlobalKeywords) {
                System.out.println(keyword.getWord() + ": " + keyword.getDf());
                counter++;
                if (counter == 50) {
                    break;
                }
            }
        }
        phaser.arriveAndDeregister();

        System.out.println("Thread " + taskName + " has finished.");
    }

    private synchronized void addKeyword(
            Map<String, Integer> globalKeywords,
            String word) {
        globalKeywords.merge(word, 1, Integer::sum);
    }
}
