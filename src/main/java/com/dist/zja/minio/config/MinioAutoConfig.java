package com.dist.zja.minio.config;

import com.dist.zja.minio.MinIoObjectService;
import com.dist.zja.minio.MinioBucketService;
import com.dist.zja.minio.properties.MinioProperties;
import io.minio.MinioClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
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
@ConditionalOnProperty(name = "dist.minio.config.enabled", matchIfMissing = true)
public class MinioAutoConfig {

    private MinioProperties minIo;

    public MinioAutoConfig(MinioProperties minioProperties) {
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
    public MinIoObjectService minIoObjectService(MinioClient minioClient) {
        if (StringUtils.isEmpty(minIo.getDefaultBucket())) {
            return new MinIoObjectService(minioClient);
        }
        return new MinIoObjectService(minioClient, minIo.getDefaultBucket());
    }
}
