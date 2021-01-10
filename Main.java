import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.LinkedList;
import java.util.stream.Stream;

public class Main {
    public static final LinkedList<String> workloads = new LinkedList<>();
    public static String CURRENT_ADDR = System.getProperty("user.dir") + File.separator;
    public static final String SRC_ADDR = CURRENT_ADDR + File.separator + "Articles";
    public static FileSystem CLIENT;
    public static final Object HADOOP_CREATE_SHARE_LOCK = 0;
    public static final Object WORKLOAD_ACCESS_LOCK = 0;


    public static void uploadOneFile(final String baseAddr, final String fileName,
                                     final String remoteAddr, final FileSystem fs) throws IOException {
        try (InputStream input = new FileInputStream(baseAddr + File.separator + fileName)) {
            try (OutputStream output = fs.create(new org.apache.hadoop.fs.Path(remoteAddr))) {
                byte[] buffer = new byte[2 << 12];
                int len = 0;
                while ((len = input.read(buffer)) != -1) {
                    output.write(buffer, 0, len);
                }
            }
        }
    }

    static {
        try (Stream<Path> paths = Files.list(Paths.get(SRC_ADDR))) {
            paths.map(p -> p.getFileName().toString()).forEachOrdered(workloads::add);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        try {
            CLIENT = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void rollBackWorkLoads(String dirsNotFinished) {
        synchronized (WORKLOAD_ACCESS_LOCK) {
            workloads.add(dirsNotFinished);
        }
    }

    public static int getRemainingWorks() {
        synchronized (WORKLOAD_ACCESS_LOCK) {
            return workloads.size();
        }
    }

    public static String[] getNextWorkloads(int numberOfDirectory) {
        synchronized (WORKLOAD_ACCESS_LOCK) {
            if (workloads.size() == 0) {
                return null;
            } else {
                int realWorkSetNum = Math.min(workloads.size(), numberOfDirectory);
                String[] lst = new String[realWorkSetNum];
                for (int i = 0; i < realWorkSetNum; i++) {
                    lst[i] = workloads.pop();
                }
                return lst;
            }
        }
    }

    static class UploadArticles extends Thread {

        @Override
        public void run() {
            String[] workSet = getNextWorkloads(3);
            while (workSet != null) {
                for (String nextDir : workSet) {
                    final String baseDir = SRC_ADDR + File.separator + nextDir;
                    LinkedList<String> files = new LinkedList<>();
                    try (Stream<Path> paths = Files.list(Paths.get(SRC_ADDR + File.separator + nextDir))) {
                        paths.map(p -> p.getFileName().toString()).forEachOrdered(files::add);
                    } catch (IOException e) {
                        System.err.println("Failed to scan directory: " + nextDir);
                        e.printStackTrace();
                        continue;
                    }
                    synchronized (HADOOP_CREATE_SHARE_LOCK) {
                        for (String oneFile : files) {
                            try {
                                uploadOneFile(baseDir, oneFile, nextDir + File.separator + oneFile, CLIENT);
                            } catch (IOException e) {
                                System.err.println("Failed to upload :" + oneFile);
                                e.printStackTrace();
                            }
                        }
                    }
                }
                workSet = getNextWorkloads(5);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        final int THREAD_NUMS = 5;
        final int totalWorks = getRemainingWorks();
        int remainingWorks = getRemainingWorks();
        UploadArticles[] threadPool = new UploadArticles[THREAD_NUMS];
        for (int i = 0; i < THREAD_NUMS; i++) {
            threadPool[i] = new UploadArticles();
            threadPool[i].start();
        }
        while (remainingWorks > 0) {
            System.out.println(new Date().toString() + ": " + (100 * (1 - ((float) remainingWorks / (float) totalWorks))) + "%");
            Thread.sleep(10000); // 10 sec
            remainingWorks = getRemainingWorks();
        }
    }

}
