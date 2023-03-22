package com.dist.zja.minio.config;

import com.dist.zja.minio.MinioBucketService;
import com.dist.zja.minio.MinioObjectService;
import com.dist.zja.minio.properties.MinioProperties;
import io.minio.MinioClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * Company: 上海数慧系统技术有限公司
 * Department: 数据中心
 * Date: 2021-01-25 9:10
 * Author: zhengja
 * Email: zhengja@dist.com.cn
 * Desc：
 */
@Configuration
@EnableConfigurationProperties({MinioProperties.class})
@ConditionalOnProperty(name = "dist.minio.enabled", matchIfMissing = true)
public class MinioAutoConfig {

    private MinioProperties minIo;

    public MinioAutoConfig(MinioProperties minioProperties) {
        if (minioProperties.getDefaultBucket() != null) {
            validateBucketName(minioProperties.getDefaultBucket());
        }
        this.minIo = minioProperties;
    }

    @Bean
    @ConditionalOnMissingBean
    public MinioClient minioClient() {
        // Create a minioClient with the MinIO server playground, its access key and secret key.
        MinioClient minioClient = MinioClient.builder()
                .endpoint(minIo.getEndpoint(), minIo.getPort(), minIo.isSecure())
                .credentials(minIo.getAccessKey(), minIo.getSecretKey())
                .build();
        return minioClient;
    }

    @Bean(initMethod = "init")
    @ConditionalOnMissingBean
    public MinioBucketService minioBucketService(MinioClient minioClient) {
        if (StringUtils.isEmpty(minIo.getDefaultBucket())) {
            return new MinioBucketService(minioClient);
        }
        return new MinioBucketService(minioClient, minIo.getDefaultBucket());
    }

    @Bean(initMethod = "init")
    @ConditionalOnMissingBean
    public MinioObjectService minIoObjectService(MinioClient minioClient) {
        if (StringUtils.isEmpty(minIo.getDefaultBucket())) {
            return new MinioObjectService(minioClient);
        }
        return new MinioObjectService(minioClient, minIo);
    }


    /**
     * 验证桶名称
     * @param name
     */
    private static void validateBucketName(String name) {
        // Bucket names cannot be no less than 3 and no more than 63 characters long.
        if (name.length() < 3 || name.length() > 63) {
            throw new IllegalArgumentException(name + " : " + "defaultBucket name must be at least 3 and no more than 63 characters long");
        }
        // Successive periods in bucket names are not allowed.
        if (name.contains("..")) {
            String msg = "defaultBucket name cannot contain successive periods.";
            throw new IllegalArgumentException(name + " : " + msg);
        }
        // Bucket names should be dns compatible.
        if (!name.matches("^[a-z0-9][a-z0-9\\.\\-]+[a-z0-9]$")) {
            String msg = "defaultBucket name does not follow Amazon S3 standards.";
            throw new IllegalArgumentException(name + " : " + msg);
        }
    }

}
