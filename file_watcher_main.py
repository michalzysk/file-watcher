package com.company.filewatcher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableIntegration
@EnableAsync
public class FileWatcherApplication {

    public static void main(String[] args) {
        SpringApplication.run(FileWatcherApplication.class, args);
    }
}

// Configuration
package com.company.filewatcher.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "app")
@Data
public class AppConfig {
    
    private Smb smb = new Smb();
    private S3 s3 = new S3();
    private Trino trino = new Trino();
    private Processing processing = new Processing();
    private Pattern pattern = new Pattern();
    
    @Data
    public static class Smb {
        private String server = "nas.example.com";
        private String share = "data";
        private String username = "user";
        private String password = "password";
        private String watchPath = "/incoming";
    }
    
    @Data
    public static class S3 {
        private String bucket = "data-lake";
        private String region = "us-east-1";
        private String accessKeyId = "";
        private String secretAccessKey = "";
    }
    
    @Data
    public static class Trino {
        private String host = "trino.example.com";
        private int port = 443;
        private String user = "admin";
        private String catalog = "hive";
        private String schema = "default";
    }
    
    @Data
    public static class Processing {
        private int maxWorkers = 5;
        private int queueCapacity = 100;
        private long pollingInterval = 5000;
    }
    
    @Data
    public static class Pattern {
        private String filenameRegex = "(?<date>\\d{8})_(?<type>[A-Z]+)_(?<source>\\w+)\\.(?<ext>\\w+)";
        private String s3KeyTemplate = "data/{date}/{type}/{source}/{filename}";
    }
}

// SMB Client Service
package com.company.filewatcher.service;

import com.company.filewatcher.config.AppConfig;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import javax.annotation.PreDestroy;
import javax.annotation.PostConstruct;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
@Slf4j
public class SmbService {
    
    @Autowired
    private AppConfig config;
    
    private SMBClient smbClient;
    private Connection connection;
    private Session session;
    private DiskShare share;
    
    @PostConstruct
    public void connect() {
        try {
            smbClient = new SMBClient();
            connection = smbClient.connect(config.getSmb().getServer());
            
            AuthenticationContext authContext = new AuthenticationContext(
                config.getSmb().getUsername(),
                config.getSmb().getPassword().toCharArray(),
                null
            );
            
            session = connection.authenticate(authContext);
            share = (DiskShare) session.connectShare(config.getSmb().getShare());
            
            log.info("SMB connection established to {}\\{}", 
                config.getSmb().getServer(), config.getSmb().getShare());
                
        } catch (Exception e) {
            log.error("Failed to connect to SMB", e);
            throw new RuntimeException("SMB connection failed", e);
        }
    }
    
    @PreDestroy
    public void disconnect() {
        try {
            if (share != null) share.close();
            if (session != null) session.close();
            if (connection != null) connection.close();
            if (smbClient != null) smbClient.close();
            log.info("SMB connection closed");
        } catch (Exception e) {
            log.warn("Error during SMB disconnect", e);
        }
    }
    
    public boolean isConnected() {
        return connection != null && connection.isConnected();
    }
    
    public Path downloadFile(String remotePath) throws IOException {
        String filename = Paths.get(remotePath).getFileName().toString();
        Path localPath = Paths.get(System.getProperty("java.io.tmpdir"), filename);
        
        try (File remoteFile = share.openFile(remotePath, null, null, null, null, null);
             InputStream inputStream = remoteFile.getInputStream();
             FileOutputStream outputStream = new FileOutputStream(localPath.toFile())) {
            
            byte[] buffer = new byte[64 * 1024]; // 64KB buffer
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            
            log.info("File downloaded: {} -> {}", remotePath, localPath);
            return localPath;
            
        } catch (Exception e) {
            log.error("Failed to download file: {}", remotePath, e);
            if (Files.exists(localPath)) {
                Files.delete(localPath);
            }
            throw new IOException("Download failed: " + remotePath, e);
        }
    }
}

// S3 Service
package com.company.filewatcher.service;

import com.company.filewatcher.config.AppConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.file.Path;

@Service
@Slf4j
public class S3Service {
    
    @Autowired
    private AppConfig config;
    
    private S3Client s3Client;
    
    @PostConstruct
    public void initialize() {
        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(
            config.getS3().getAccessKeyId(),
            config.getS3().getSecretAccessKey()
        );
        
        s3Client = S3Client.builder()
            .region(Region.of(config.getS3().getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
            .build();
            
        log.info("S3 client initialized for bucket: {}", config.getS3().getBucket());
    }
    
    @PreDestroy
    public void cleanup() {
        if (s3Client != null) {
            s3Client.close();
        }
    }
    
    public void uploadFile(Path localPath, String s3Key) {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                .bucket(config.getS3().getBucket())
                .key(s3Key)
                .build();
                
            s3Client.putObject(request, localPath);
            log.info("File uploaded to S3: {}", s3Key);
            
        } catch (Exception e) {
            log.error("Failed to upload file to S3: {}", s3Key, e);
            throw new RuntimeException("S3 upload failed: " + s3Key, e);
        }
    }
    
    public boolean fileExists(String s3Key) {
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(config.getS3().getBucket())
                .key(s3Key)
                .build();
                
            s3Client.headObject(request);
            return true;
            
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            log.warn("Error checking S3 object existence: {}", s3Key, e);
            return false;
        }
    }
}

// Trino Service
package com.company.filewatcher.service;

import com.company.filewatcher.config.AppConfig;
import io.trino.jdbc.TrinoDriver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TrinoService {
    
    @Autowired
    private AppConfig config;
    
    private Connection connection;
    
    @PostConstruct
    public void initialize() {
        try {
            DriverManager.registerDriver(new TrinoDriver());
            
            String url = String.format("jdbc:trino://%s:%d/%s/%s",
                config.getTrino().getHost(),
                config.getTrino().getPort(),
                config.getTrino().getCatalog(),
                config.getTrino().getSchema()
            );
            
            Properties props = new Properties();
            props.setProperty("user", config.getTrino().getUser());
            props.setProperty("SSL", "true");
            
            connection = DriverManager.getConnection(url, props);
            log.info("Trino connection established to {}", url);
            
        } catch (Exception e) {
            log.error("Failed to connect to Trino", e);
            throw new RuntimeException("Trino connection failed", e);
        }
    }
    
    @PreDestroy
    public void cleanup() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception e) {
            log.warn("Error closing Trino connection", e);
        }
    }
    
    public void registerPartition(String tableName, Map<String, String> partitionSpec, String location) {
        String partitionClause = partitionSpec.entrySet().stream()
            .map(entry -> entry.getKey() + "='" + entry.getValue() + "'")
            .collect(Collectors.joining(", "));
            
        String query = String.format(
            "ALTER TABLE %s.%s.%s ADD IF NOT EXISTS PARTITION (%s) LOCATION 's3://%s/%s'",
            config.getTrino().getCatalog(),
            config.getTrino().getSchema(),
            tableName,
            partitionClause,
            config.getS3().getBucket(),
            location
        );
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(query);
            log.info("Partition registered - Table: {}, Partition: {}", tableName, partitionSpec);
            
        } catch (Exception e) {
            log.error("Failed to register partition - Table: {}, Partition: {}", tableName, partitionSpec, e);
            throw new RuntimeException("Partition registration failed", e);
        }
    }
}

// File Processing Service
package com.company.filewatcher.service;

import com.company.filewatcher.config.AppConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j
public class FileProcessingService {
    
    @Autowired
    private AppConfig config;
    
    @Autowired
    private SmbService smbService;
    
    @Autowired
    private S3Service s3Service;
    
    @Autowired
    private TrinoService trinoService;
    
    private final Counter filesProcessedSuccessCounter;
    private final Counter filesProcessedSkippedCounter;
    private final Counter filesProcessedDuplicateCounter;
    private final Counter filesProcessedErrorCounter;
    private final Timer processingTimer;
    private final Pattern filenamePattern;
    
    public FileProcessingService(MeterRegistry meterRegistry, AppConfig config) {
        this.filesProcessedSuccessCounter = Counter.builder("files.processed.total")
            .description("Total files processed successfully")
            .tag("status", "success")
            .register(meterRegistry);
            
        this.filesProcessedSkippedCounter = Counter.builder("files.processed.total")
            .description("Total files skipped")
            .tag("status", "skipped")
            .register(meterRegistry);
            
        this.filesProcessedDuplicateCounter = Counter.builder("files.processed.total")
            .description("Total duplicate files")
            .tag("status", "duplicate")
            .register(meterRegistry);
            
        this.filesProcessedErrorCounter = Counter.builder("files.processed.total")
            .description("Total files with errors")
            .tag("status", "error")
            .register(meterRegistry);
            
        this.processingTimer = Timer.builder("file.processing.duration")
            .description("Time spent processing files")
            .register(meterRegistry);
            
        this.filenamePattern = Pattern.compile(config.getPattern().getFilenameRegex());
    }
    
    public void processFile(String filePath) {
        Timer.Sample sample = Timer.start();
        String filename = Paths.get(filePath).getFileName().toString();
        Map<String, String> metadata;
        String s3Key;
        Path localPath = null;
        
        try {
            log.info("Processing file: {}", filename);
            
            // Parse filename
            Map<String, String> metadata = parseFilename(filename);
            if (metadata.isEmpty()) {
                log.warn("Filename doesn't match pattern: {}", filename);
                filesProcessedCounter.increment(Tags.of("status", "skipped"));
                return;
            }
            
            // Generate S3 key
            String s3Key = generateS3Key(filename, metadata);
            
            // Check if already processed
            if (s3Service.fileExists(s3Key)) {
                log.info("File already exists in S3, skipping: {}", s3Key);
                filesProcessedCounter.increment(Tags.of("status", "duplicate"));
                return;
            }
            
            // Download from SMB
            Path localPath = smbService.downloadFile(filePath);
            
            try {
                // Upload to S3
                s3Service.uploadFile(localPath, s3Key);
                
                // Register Trino partition
                registerTrinoPartition(metadata, s3Key);
                
                log.info("File processed successfully: {} -> {}", filename, s3Key);
                filesProcessedCounter.increment(Tags.of("status", "success"));
                
            } finally {
                // Cleanup local file
                try {
                    Files.deleteIfExists(localPath);
                } catch (Exception e) {
                    log.warn("Failed to delete temporary file: {}", localPath, e);
                }
            }
            
        } catch (Exception e) {
            log.error("Error processing file: {}", filename, e);
            filesProcessedCounter.increment(Tags.of("status", "error"));
            throw new RuntimeException("File processing failed: " + filename, e);
            
        } finally {
            sample.stop(processingTimer);
        }
    }
    
    private Map<String, String> parseFilename(String filename) {
        Matcher matcher = filenamePattern.matcher(filename);
        if (matcher.matches()) {
            Map<String, String> metadata = new HashMap<>();
            try {
                metadata.put("date", matcher.group("date"));
                metadata.put("type", matcher.group("type"));
                metadata.put("source", matcher.group("source"));
                metadata.put("ext", matcher.group("ext"));
                return metadata;
            } catch (IllegalArgumentException e) {
                log.warn("Failed to extract groups from filename: {}", filename, e);
            }
        }
        return Map.of(); // Java 9+ Map.of() for empty map
    }
    
    private String generateS3Key(String filename, Map<String, String> metadata) {
        String template = config.getPattern().getS3KeyTemplate();
        String result = template.replace("{filename}", filename);
        
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            result = result.replace(STR."{{\{entry.getKey()}}}", entry.getValue()); // Java 21 String Templates
        }
        
        return result;
    }
    
    private void registerTrinoPartition(Map<String, String> metadata, String s3Key) {
        try {
            String tableName = STR."\{metadata.get("type").toLowerCase()}_data"; // Java 21 String Templates
            
            Map<String, String> partitionSpec = Map.of(
                "date", metadata.get("date"),
                "source", metadata.get("source")
            );
            
            // Remove filename from S3 key to get location path
            String location = s3Key.substring(0, s3Key.lastIndexOf('/'));
            
            trinoService.registerPartition(tableName, partitionSpec, location);
            
        } catch (Exception e) {
            log.warn("Failed to register Trino partition for S3 key: {}", s3Key, e);
            // Don't fail the whole process if partition registration fails
        }
    }
}

// Spring Integration Configuration
package com.company.filewatcher.config;

import com.company.filewatcher.service.FileProcessingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import java.io.File;

@Configuration
@EnableIntegration
@Slf4j
public class IntegrationConfig {
    
    @Autowired
    private AppConfig appConfig;
    
    @Autowired
    private FileProcessingService fileProcessingService;
    
    @Bean
    public MessageChannel fileInputChannel() {
        return new DirectChannel();
    }
    
    @Bean
    public MessageChannel fileProcessingChannel() {
        return new QueueChannel(appConfig.getProcessing().getQueueCapacity());
    }
    
    @Bean
    public MessageChannel errorChannel() {
        return new QueueChannel(100);
    }
    
    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(10); // Support multiple directory watchers
        scheduler.setThreadNamePrefix("directory-watcher-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(30);
        scheduler.initialize();
        return scheduler;
    }
    
    @Bean
    public ThreadPoolTaskExecutor fileProcessingTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(appConfig.getProcessing().getMaxWorkers());
        executor.setQueueCapacity(appConfig.getProcessing().getQueueCapacity());
        executor.setThreadNamePrefix("file-processor-");
        executor.setRejectedExecutionHandler((r, executor1) -> {
            log.warn("Task rejected, queue is full. Consider increasing capacity or processing speed.");
            throw new MessagingException("Processing queue is full");
        });
        executor.initialize();
        return executor;
    }
    
    @Transformer(inputChannel = "fileInputChannel", outputChannel = "fileProcessingChannel")
    public Message<File> fileTransformer(Message<File> message) {
        File file = message.getPayload();
        String sourceDirectory = (String) message.getHeaders().get("sourceDirectory");
        
        log.info("File detected: {} from directory: {} at {}", 
            file.getName(), sourceDirectory, message.getHeaders().get("processedAt"));
        
        return MessageBuilder.fromMessage(message)
                .setHeader("originalPath", file.getAbsolutePath())
                .setHeader("sourceDirectory", sourceDirectory)
                .build();
    }
    
    @ServiceActivator(inputChannel = "fileProcessingChannel")
    public void handleFileProcessing(Message<File> message) {
        File file = message.getPayload();
        String sourceDirectory = (String) message.getHeaders().get("sourceDirectory");
        
        try {
            log.info("Processing file: {} from directory: {}", file.getName(), sourceDirectory);
            fileProcessingService.processFile(file.getAbsolutePath());
        } catch (Exception e) {
            log.error("Failed to process file: {} from directory: {}", 
                file.getAbsolutePath(), sourceDirectory, e);
            
            // Send to error channel
            Message<Exception> errorMessage = MessageBuilder.withPayload(e)
                .setHeader("originalFile", file.getAbsolutePath())
                .setHeader("sourceDirectory", sourceDirectory)
                .setHeader("errorTime", System.currentTimeMillis())
                .setHeader("originalMessage", message)
                .build();
                
            try {
                errorChannel().send(errorMessage);
            } catch (Exception sendError) {
                log.error("Failed to send error message to error channel", sendError);
            }
            
            throw e; // Re-throw to trigger retry if configured
        }
    }
    
    @ServiceActivator(inputChannel = "errorChannel")
    public void handleError(Message<Exception> errorMessage) {
        Exception exception = errorMessage.getPayload();
        String originalFile = (String) errorMessage.getHeaders().get("originalFile");
        String sourceDirectory = (String) errorMessage.getHeaders().get("sourceDirectory");
        Long errorTime = (Long) errorMessage.getHeaders().get("errorTime");
        
        log.error("Error processing file: {} from directory: {} at time: {}", 
            originalFile, sourceDirectory, errorTime, exception);
        
        // Here you could implement:
        // - Dead letter queue
        // - Alert notifications
        // - Retry logic with backoff
        // - Error statistics per directory
    }
    
    // Optional: Monitoring endpoint to check queue sizes
    @Bean
    public QueueChannelMonitor queueChannelMonitor() {
        return new QueueChannelMonitor();
    }
    
    public static class QueueChannelMonitor {
        
        @Autowired
        private MessageChannel fileProcessingChannel;
        
        @Autowired
        private MessageChannel errorChannel;
        
        public int getProcessingQueueSize() {
            if (fileProcessingChannel instanceof QueueChannel) {
                return ((QueueChannel) fileProcessingChannel).getQueueSize();
            }
            return 0;
        }
        
        public int getErrorQueueSize() {
            if (errorChannel instanceof QueueChannel) {
                return ((QueueChannel) errorChannel).getQueueSize();
            }
            return 0;
        }
    }
}

// Health Check Controller
package com.company.filewatcher.controller;

import com.company.filewatcher.service.SmbService;
import com.company.filewatcher.config.IntegrationConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuator.health.Health;
import org.springframework.boot.actuator.health.HealthIndicator;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@RestController
public class HealthController implements HealthIndicator {
    
    @Autowired
    private SmbService smbService;
    
    @Autowired
    private IntegrationConfig.QueueChannelMonitor queueMonitor;
    
    @Override
    public Health health() {
        boolean smbConnected = smbService.isConnected();
        int processingQueueSize = queueMonitor.getProcessingQueueSize();
        int errorQueueSize = queueMonitor.getErrorQueueSize();
        
        Health.Builder healthBuilder = smbConnected ? Health.up() : Health.down();
        
        return healthBuilder
                .withDetail("smb", smbConnected ? "connected" : "disconnected")
                .withDetail("processing_queue_size", processingQueueSize)
                .withDetail("error_queue_size", errorQueueSize)
                .withDetail("timestamp", LocalDateTime.now())
                .build();
    }
    
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> status = new HashMap<>();
        status.put("status", "UP");
        status.put("timestamp", LocalDateTime.now());
        status.put("smb_connected", smbService.isConnected());
        status.put("processing_queue_size", queueMonitor.getProcessingQueueSize());
        status.put("error_queue_size", queueMonitor.getErrorQueueSize());
        
        return ResponseEntity.ok(status);
    }
    
    @GetMapping("/ready")
    public ResponseEntity<Map<String, Object>> readinessCheck() {
        Map<String, Object> status = new HashMap<>();
        boolean ready = smbService.isConnected();
        int processingQueueSize = queueMonitor.getProcessingQueueSize();
        
        // Consider not ready if queue is too full
        boolean queueHealthy = processingQueueSize < 80; // 80% of capacity
        ready = ready && queueHealthy;
        
        status.put("status", ready ? "ready" : "not ready");
        status.put("timestamp", LocalDateTime.now());
        status.put("smb_connected", smbService.isConnected());
        status.put("queue_healthy", queueHealthy);
        status.put("processing_queue_size", processingQueueSize);
        
        return ResponseEntity.status(ready ? 200 : 503).body(status);
    }
    
    @GetMapping("/metrics-summary")
    public ResponseEntity<Map<String, Object>> metricsCheck() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("processing_queue_size", queueMonitor.getProcessingQueueSize());
        metrics.put("error_queue_size", queueMonitor.getErrorQueueSize());
        metrics.put("timestamp", LocalDateTime.now());
        
        return ResponseEntity.ok(metrics);
    }
}