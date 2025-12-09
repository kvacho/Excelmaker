import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import handler.Handler;
import handler.Transferrer;
import model.Person;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        String filename = "/home/kvacho/Downloads/MOCK_DATA.xlsx";

        int totalRows = 0;
        try (FileInputStream fileInputStream = new FileInputStream(new File(filename));
             XSSFWorkbook wb = new XSSFWorkbook(fileInputStream)) {
            totalRows = wb.getSheetAt(0).getPhysicalNumberOfRows();
        }

        int threadCount = 12;
        ExecutorService readExecutor = Executors.newFixedThreadPool(threadCount);

        int chunkSize = totalRows / threadCount;

        List<Future<List<Person>>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            int start = (i * chunkSize) + 1;
            int end = (i == threadCount - 1) ? totalRows : (start + chunkSize);

            System.out.println("rows " + start + " to " + end);

            Handler task = new Handler(filename, start, end);
            futures.add(readExecutor.submit(task));
        }

        List<Person> lastList = new ArrayList<>();

        for (Future<List<Person>> future : futures) {
            try {
                lastList.addAll(future.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        readExecutor.shutdown();
        System.out.println("Done. Size: " + lastList.size());

        System.out.println("Write...");

        File jsonFile = new File("data.json");
        ObjectMapper mapper = new ObjectMapper();

        ExecutorService writeExecutor = Executors.newFixedThreadPool(threadCount);

        int a = 0;
        File lastFile = new File("last.txt");
        if (lastFile.exists()) {
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(lastFile))) {
                String line = bufferedReader.readLine();
                if (line != null && !line.trim().isEmpty()) {
                    a = Integer.parseInt(line.trim());
                }
            }
        }

        System.out.println("Starting chunk: " + a);

        try (SequenceWriter writer = mapper.writerWithDefaultPrettyPrinter().writeValuesAsArray(jsonFile)) {

            int totalListSize = lastList.size();
            int writeChunkSize = (int) Math.ceil((double) totalListSize / threadCount);

            for (int i = a; i < threadCount; i++) {
                int start = i * writeChunkSize;
                int end = start + writeChunkSize;

                Transferrer transferTask = new Transferrer(lastList, start, end, writer);
                writeExecutor.submit(transferTask);
            }

            writeExecutor.shutdown();

            boolean finished = writeExecutor.awaitTermination(2, TimeUnit.MINUTES);

            if (finished) {
                System.out.println("Done.");
                try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(lastFile))) {
                    bufferedWriter.write(String.valueOf(threadCount));
                }
            } else {
                System.out.println("Timed out.");
            }
        }

        System.out.println("data.json");
    }
}