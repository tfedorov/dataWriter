package tfedorov;

import com.google.appengine.tools.cloudstorage.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
//[START gcs_imports]
//[END gcs_imports]
//[START gcs_imports]
//[END gcs_imports]

public class Generate {

    private static final GcsService gcsService = GcsServiceFactory.createGcsService(new RetryParams.Builder()
            .initialRetryDelayMillis(10)
            .retryMaxAttempts(10)
            .totalRetryPeriodMillis(15000)
            .build());

    public static void main(String[] args) throws IOException {
        System.out.println("Hello world");
        GcsFileOptions instance = GcsFileOptions.getDefaultInstance();
        GcsFilename fileName = new GcsFilename("dataproc-16e500ca-9ea0-4a83-bba0-8b312eff1634-us", "tempU/some");
        GcsOutputChannel outputChannel;
        outputChannel = gcsService.createOrReplace(fileName, instance);
        InputStream targetStream = new ByteArrayInputStream("some".getBytes());
        copy(targetStream, Channels.newOutputStream(outputChannel));
    }

    private static final int BUFFER_SIZE = 2 * 1024 * 1024;

    private static void copy(InputStream input, OutputStream output) throws IOException {
        try {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead = input.read(buffer);
            while (bytesRead != -1) {
                output.write(buffer, 0, bytesRead);
                bytesRead = input.read(buffer);
            }
        } finally {
            input.close();
            output.close();
        }
    }
}
