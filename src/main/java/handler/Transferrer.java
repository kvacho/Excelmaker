package handler;

import com.fasterxml.jackson.databind.SequenceWriter;
import model.Person;

import java.io.IOException;
import java.util.List;

public class Transferrer implements Runnable {
    private final List<Person> masterList;
    private final int startIndex;
    private final int endIndex;
    private final SequenceWriter writer;

    public Transferrer(List<Person> masterList, int startIndex, int endIndex, SequenceWriter writer) {
        this.masterList = masterList;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.writer = writer;
    }

    @Override
    public void run() {
        int actualEnd = Math.min(endIndex, masterList.size());

        List<Person> myChunk = masterList.subList(startIndex, actualEnd);

        synchronized (writer) {
            try {
                System.out.println(Thread.currentThread().getName() + " writing " + myChunk.size() + " records");
                writer.write(myChunk);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}