package com.dist.zja.minio.properties;

import com.dist.zja.minio.common.annotations.AttributeComment;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Company: 上海数慧系统技术有限公司
 * Department: 数据中心
 * Date: 2021-01-25 9:13
 * Author: zhengja
 * Email: zhengja@dist.com.cn
 * Desc：
 */
@ConfigurationProperties(prefix = "dist.minio")
public class MinioProperties {

    /**
     * Minio 启用
     */
    @AttributeComment("Minio 启用")
    private boolean enabled = true;

    /**
     * Minio 启动 https
     */
    @AttributeComment("Minio 启动 https , 需配置Minio TLS证书")
    private boolean secure = false;

    /**
     * Minio 服务地址
     */
    @AttributeComment("Minio服务端 例 http://127.0.0.1")
    private String endpoint;

    /**
     * Minio port
     */
    @AttributeComment("Minio 端口 例 9000")
    private int port;

    /**
     * Minio ACCESS_KEY
     */
    @AttributeComment("Minio ACCESS_KEY")
    private String accessKey;

    /**
     * Minio SECRET_KEY
     */
    @AttributeComment("Minio SECRET_KEY")
    private String secretKey;

    /**
     * Minio DEFAULT_BUCKET ,可选
     */
    @AttributeComment("Minio DEFAULT_BUCKET,可选的 defaultBucket")
    private String defaultBucket;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public String getDefaultBucket() {
        return defaultBucket;
    }

    public void setDefaultBucket(String defaultBucket) {
        this.defaultBucket = defaultBucket;
    }

    @Override
    public String toString() {
        return "MinioProperties{" +
                "enabled=" + enabled +
                ", secure=" + secure +
                ", endpoint='" + endpoint + '\'' +
                ", port=" + port +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", defaultBucket='" + defaultBucket + '\'' +
                '}';
    }
}
