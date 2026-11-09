import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.*;

/** Latency of FileChannel.force(false) after writing a translog-sized chunk, alone and with several files syncing at the same time. */
public class FsyncBench {
    static double run(int threads, int iterations, int bytes) throws Exception {
        ExecutorService ex = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        Path dir = Files.createTempDirectory(Path.of("."), "fsync");
        java.util.List<Future<Long>> fs = new java.util.ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            fs.add(ex.submit(() -> {
                try (FileChannel ch = FileChannel.open(dir.resolve("f" + id), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                    ByteBuffer buf = ByteBuffer.allocate(bytes);
                    start.await();
                    long t0 = System.nanoTime();
                    for (int i = 0; i < iterations; i++) { buf.clear(); ch.write(buf); ch.force(false); }
                    return System.nanoTime() - t0;
                }
            }));
        }
        start.countDown();
        double worst = 0;
        for (var f : fs) worst = Math.max(worst, f.get() / 1e6 / iterations);
        ex.shutdown();
        try (var s = Files.walk(dir)) { s.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete()); }
        return worst; // ms per write+force, for the slowest thread
    }
    public static void main(String[] a) throws Exception {
        int bytes = 256 * 1024;
        run(1, 20, bytes); // warm up
        for (int threads : new int[] { 1, 2, 4, 8 }) {
            double ms = run(threads, 40, bytes);
            System.out.printf("%d file(s) syncing at the same time: %.2f ms per write+force each, %.0f syncs/s in total%n", threads, ms, threads * 1000.0 / ms);
        }
    }
}
