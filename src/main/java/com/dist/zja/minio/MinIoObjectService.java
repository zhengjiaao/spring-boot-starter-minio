package com.dist.zja.minio;

import com.dist.zja.minio.common.annotations.ClassComment;
import com.dist.zja.minio.common.annotations.MethodComment;
import com.dist.zja.minio.common.annotations.Param;
import com.google.common.io.ByteStreams;
import io.minio.*;
import io.minio.errors.*;
import io.minio.http.Method;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Company: 上海数慧系统技术有限公司
 * Department: 数据中心
 * Date: 2021-01-25 10:40
 * Author: zhengja
 * Email: zhengja@dist.com.cn
 * Desc：
 */
@ClassComment(value = "Minio 文件服务-操作文件对象", author = "zhengja")
public class MinIoObjectService {

    public static Logger logger = LoggerFactory.getLogger(MinIoObjectService.class);

    // default expiration for a presigned URL is 7 days in seconds
    public static final int DEFAULT_EXPIRY_TIME = (int) TimeUnit.DAYS.toSeconds(7);

    private MinioClient minioClient;

    private String defaultBucket;

    public MinIoObjectService(MinioClient minioClient) {
        this.minioClient = minioClient;
    }

    public MinIoObjectService(MinioClient minioClient, String defaultBucket) {
        this.minioClient = minioClient;
        this.defaultBucket = defaultBucket;
    }

    public void init() {
        logger.info("com.dist.zja.minio.MinIoObjectService  Init Success！");
    }

    protected void validateBucketName(String name) {
        validateNotNull(name, "bucket name");

        // Bucket names cannot be no less than 3 and no more than 63 characters long.
        if (name.length() < 3 || name.length() > 63) {
            throw new IllegalArgumentException(
                    name + " : " + "bucket name must be at least 3 and no more than 63 characters long");
        }
        // Successive periods in bucket names are not allowed.
        if (name.contains("..")) {
            String msg =
                    "bucket name cannot contain successive periods. For more information refer "
                            + "http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html";
            throw new IllegalArgumentException(name + " : " + msg);
        }
        // Bucket names should be dns compatible.
        if (!name.matches("^[a-z0-9][a-z0-9\\.\\-]+[a-z0-9]$")) {
            String msg =
                    "bucket name does not follow Amazon S3 standards. For more information refer "
                            + "http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html";
            throw new IllegalArgumentException(name + " : " + msg);
        }
    }

    protected void validateNotNull(Object arg, String argName) {
        if (arg == null) {
            throw new IllegalArgumentException(argName + " must not be null,Must be configured dist.minio.config.default-bucket=");
        }
    }

    @MethodComment(
            function = "文件上传-本地文件路径",
            params = {
                    @Param(name = "objectName", description = "文件id(存储名称)"),
                    @Param(name = "filePath", description = "本地文件路径")
            },
            description = "使用默认桶 defaultBucket，必须配置 dist.minio.config.default-bucket= ")
    public ObjectWriteResponse putObject(String objectName, String filePath) throws IOException, ServerException, InsufficientDataException, InternalException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, XmlParserException, ErrorResponseException {
        validateBucketName(defaultBucket);
        return minioClient.uploadObject(
                UploadObjectArgs.builder()
                        .bucket(defaultBucket)
                        .object(objectName)
                        .filename(filePath)
                        .build());
    }

    @MethodComment(
            function = "文件上传-本地文件路径",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "文件id(存储名称)"),
                    @Param(name = "filePath", description = "本地文件路径")
            })
    public ObjectWriteResponse putObject(String bucketName, String objectName, String filePath) throws IOException, ServerException, InsufficientDataException, InternalException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, XmlParserException, ErrorResponseException {
        return minioClient.uploadObject(
                UploadObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .filename(filePath)
                        .build());
    }

    @MethodComment(
            function = "上传文件-本地文件路径",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "objectName", description = "文件ID(文件名)"),
                    @Param(name = "filePath", description = "本机文件路径")
            })
    public ObjectWriteResponse putObject(String bucketName, String region, String objectName, String filePath) throws IOException, ServerException, InsufficientDataException, InternalException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, XmlParserException, ErrorResponseException {
        return minioClient.uploadObject(
                UploadObjectArgs.builder()
                        .bucket(bucketName)
                        .region(region)
                        .object(objectName)
                        .filename(filePath)
                        .build());
    }

    @MethodComment(
            function = "上传文件-multipartFile",
            params = {
                    @Param(name = "multipartFile", description = "多部分单个文件")
            },
            description = "按默认上传文件名称存储,使用默认桶 defaultBucket，必须配置 dist.minio.config.default-bucket= ")
    public ObjectWriteResponse putUploadObjectByMultipartFile(MultipartFile multipartFile) {
        validateBucketName(defaultBucket);
        try (InputStream inputStream = multipartFile.getInputStream()) {
            // 上传文件的名称
            String objectName = multipartFile.getOriginalFilename();
            return minioClient.putObject(PutObjectArgs.builder()
                    .bucket(defaultBucket)
                    .object(objectName)
                    .stream(inputStream, multipartFile.getSize(), ObjectWriteArgs.MIN_MULTIPART_SIZE)
                    .contentType(multipartFile.getContentType())
                    .build());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            e.printStackTrace();
        }
        return null;
    }

    @MethodComment(
            function = "上传文件-multipartFile",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "multipartFile", description = "多部分单个文件")
            },
            description = "按默认上传文件名称存储")
    public ObjectWriteResponse putUploadObjectByMultipartFile(String bucketName, MultipartFile multipartFile) {
        try (InputStream inputStream = multipartFile.getInputStream()) {
            // 上传文件的名称
            String objectName = multipartFile.getOriginalFilename();
            return minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(inputStream, multipartFile.getSize(), ObjectWriteArgs.MIN_MULTIPART_SIZE)
                    .contentType(multipartFile.getContentType())
                    .build());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            e.printStackTrace();
        }
        return null;
    }

    @MethodComment(
            function = "上传文件-multipartFile",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "multipartFile", description = "多部分单个文件")
            },
            description = "按默认上传文件名称存储")
    public ObjectWriteResponse putUploadObjectByMultipartFileAndregion(String bucketName, String region, MultipartFile multipartFile) {
        try (InputStream inputStream = multipartFile.getInputStream()) {
            // 上传文件的名称
            String objectName = multipartFile.getOriginalFilename();
            return minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .region(region)
                    .object(objectName)
                    .stream(inputStream, multipartFile.getSize(), ObjectWriteArgs.MIN_MULTIPART_SIZE)
                    .contentType(multipartFile.getContentType())
                    .build());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            e.printStackTrace();
        }
        return null;
    }

    @MethodComment(
            function = "文件上传-multipartFile",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "multipartFile", description = "多部分单个文件")
            },
            description = "按指定 objectName 存储")
    public ObjectWriteResponse putUploadObjectByMultipartFile(String bucketName, String objectName, MultipartFile multipartFile) {
        try (InputStream inputStream = multipartFile.getInputStream()) {
            return minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(inputStream, multipartFile.getSize(), ObjectWriteArgs.MIN_MULTIPART_SIZE)
                    .contentType(multipartFile.getContentType())
                    .build());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            e.printStackTrace();
        }
        return null;
    }

    @MethodComment(
            function = "上传文件-multipartFile",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "multipartFile", description = "多部分单个文件")
            },
            description = "按指定 objectName 存储")
    public ObjectWriteResponse putUploadObjectByMultipartFile(String bucketName, String region, String objectName, MultipartFile multipartFile) {
        try (InputStream inputStream = multipartFile.getInputStream()) {
            return minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .region(region)
                    .object(objectName)
                    .stream(inputStream, multipartFile.getSize(), ObjectWriteArgs.MIN_MULTIPART_SIZE)
                    .contentType(multipartFile.getContentType())
                    .build());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @MethodComment(
            function = "上传文件-InputStream",
            params = {
                    @Param(name = "region", description = "域"),
                    @Param(name = "InputStream", description = "文件流")
            })
    public ObjectWriteResponse putObject(String objectName, InputStream stream) {
        validateBucketName(defaultBucket);
        try {
            ObjectWriteResponse objectWriteResponse = minioClient.putObject(PutObjectArgs.builder()
                    .bucket(defaultBucket)
                    .object(objectName)
                    .stream(stream, stream.available(), ObjectWriteArgs.MIN_MULTIPART_SIZE)
                    .contentType("application/octet-stream")
                    .build());
            return objectWriteResponse;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @MethodComment(
            function = "上传文件-InputStream",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "InputStream", description = "文件流")
            })
    public ObjectWriteResponse putObject(String bucketName, String objectName, InputStream stream) {
        try {
            ObjectWriteResponse objectWriteResponse = minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(stream, stream.available(), ObjectWriteArgs.MIN_MULTIPART_SIZE)
                    .contentType("application/octet-stream")
                    .build());
            return objectWriteResponse;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @MethodComment(
            function = "上传文件-InputStream",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "InputStream", description = "文件流")
            })
    public ObjectWriteResponse putObject(String bucketName, String region, String objectName, InputStream stream) {
        try {
            ObjectWriteResponse objectWriteResponse = minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .region(region)
                    .object(objectName)
                    .stream(stream, stream.available(), ObjectWriteArgs.MIN_MULTIPART_SIZE)
                    .contentType("application/octet-stream")
                    .build());
            return objectWriteResponse;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @MethodComment(
            function = "上传文件-InputStream",
            params = {
                    @Param(name = "objectName", description = "对象名称"),
                    @Param(name = "InputStream", description = "文件流"),
                    @Param(name = "contentType", description = "内容类型")
            })
    public ObjectWriteResponse putObject(String objectName, InputStream stream, String contentType) {
        validateBucketName(defaultBucket);
        try {
            ObjectWriteResponse objectWriteResponse = minioClient.putObject(PutObjectArgs.builder()
                    .bucket(defaultBucket)
                    .object(objectName)
                    .stream(stream, stream.available(), ObjectWriteArgs.MIN_MULTIPART_SIZE)
                    .contentType(contentType)
                    .build());
            return objectWriteResponse;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @MethodComment(
            function = "上传文件-InputStream",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "InputStream", description = "文件流"),
                    @Param(name = "contentType", description = "内容类型")
            })
    public ObjectWriteResponse putObject(String bucketName, String objectName, InputStream stream, String contentType) {
        try {
            ObjectWriteResponse objectWriteResponse = minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(stream, stream.available(), ObjectWriteArgs.MIN_MULTIPART_SIZE)
                    .contentType(contentType)
                    .build());
            return objectWriteResponse;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @MethodComment(
            function = "上传文件-InputStream",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "InputStream", description = "文件流"),
                    @Param(name = "contentType", description = "内容类型")
            })
    public ObjectWriteResponse putObject(String bucketName, String region, String objectName, InputStream stream, String contentType) {
        try {
            ObjectWriteResponse objectWriteResponse = minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .region(region)
                    .object(objectName)
                    .stream(stream, stream.available(), ObjectWriteArgs.MIN_MULTIPART_SIZE)
                    .contentType(contentType)
                    .build());
            return objectWriteResponse;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @MethodComment(
            function = "下载文件-流",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称")
            })
    public GetObjectResponse getObjectResponse(String objectName) throws Exception {
        validateBucketName(defaultBucket);
        return minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(defaultBucket)
                        .object(objectName)
                        .build());
    }

    @MethodComment(
            function = "下载文件-流",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称")
            })
    public GetObjectResponse getObjectResponse(String bucketName, String objectName) throws Exception {
        return minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .build());
    }

    @MethodComment(
            function = "下载文件-流",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "objectName", description = "存储桶里的对象名称")
            })
    public GetObjectResponse getObjectResponse(String bucketName, String region, String objectName) throws Exception {
        return minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(bucketName)
                        .region(region)
                        .object(objectName)
                        .build());
    }

    @MethodComment(
            function = "下载文件-流-支持断点下载",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "offset", description = "offset 是起始字节的位置"),
                    @Param(name = "length", description = "length是要读取的长度 (可选，如果无值则代表读到文件结尾)")
            },
            description = "下载对象指定区域的字节数组做为流。（断点下载）")
    public GetObjectResponse getObjectResponse(String objectName, Long offset, Long length) throws Exception {
        validateBucketName(defaultBucket);
        return minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(defaultBucket)
                        .object(objectName)
                        .offset(offset)
                        .length(length)
                        .build());
    }

    @MethodComment(
            function = "下载文件-流-支持断点下载",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "offset", description = "offset 是起始字节的位置"),
                    @Param(name = "length", description = "length是要读取的长度 (可选，如果无值则代表读到文件结尾)")
            },
            description = "下载对象指定区域的字节数组做为流。（断点下载）")
    public GetObjectResponse getObjectResponse(String bucketName, String objectName, Long offset, Long length) throws Exception {
        return minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .offset(offset)
                        .length(length)
                        .build());
    }

    @MethodComment(
            function = "下载文件-流-支持断点下载",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "offset", description = "offset 是起始字节的位置"),
                    @Param(name = "length", description = "length是要读取的长度 (可选，如果无值则代表读到文件结尾)")
            },
            description = "下载对象指定区域的字节数组做为流。（断点下载）")
    public GetObjectResponse getObjectResponse(String bucketName, String region, String objectName, Long offset, Long length) throws Exception {
        return minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(bucketName)
                        .region(region)
                        .object(objectName)
                        .offset(offset)
                        .length(length)
                        .build());
    }

    @MethodComment(
            function = "文件下载-流-response方式",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "HttpServletResponse", description = "response")
            })
    public void downloadObject(String objectName, HttpServletResponse response) {
        validateBucketName(defaultBucket);
        // 设置编码
        response.setCharacterEncoding("UTF-8");
        try (ServletOutputStream os = response.getOutputStream();
             GetObjectResponse is = minioClient.getObject(
                     GetObjectArgs.builder()
                             .bucket(defaultBucket)
                             .object(objectName)
                             .build());) {

            response.setHeader("Content-Disposition", "attachment;objectName=" +
                    new String(objectName.getBytes("gb2312"), "ISO8859-1"));
            ByteStreams.copy(is, os);
            os.flush();
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @MethodComment(
            function = "文件下载-流-response方式",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "HttpServletResponse", description = "response")
            })
    public void downloadObject(String bucketName, String objectName, HttpServletResponse response) {
        // 设置编码
        response.setCharacterEncoding("UTF-8");
        try (ServletOutputStream os = response.getOutputStream();
             GetObjectResponse is = minioClient.getObject(
                     GetObjectArgs.builder()
                             .bucket(bucketName)
                             .object(objectName)
                             .build());) {

            response.setHeader("Content-Disposition", "attachment;objectName=" +
                    new String(objectName.getBytes("gb2312"), "ISO8859-1"));
            ByteStreams.copy(is, os);
            os.flush();
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @MethodComment(
            function = "文件下载-流-response方式",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "HttpServletResponse", description = "response")
            })
    public void downloadObject(String bucketName, String region, String objectName, HttpServletResponse response) {
        // 设置编码
        response.setCharacterEncoding("UTF-8");
        try (ServletOutputStream os = response.getOutputStream();
             GetObjectResponse is = minioClient.getObject(
                     GetObjectArgs.builder()
                             .bucket(bucketName)
                             .region(region)
                             .object(objectName)
                             .build());) {

            response.setHeader("Content-Disposition", "attachment;objectName=" +
                    new String(objectName.getBytes("gb2312"), "ISO8859-1"));
            ByteStreams.copy(is, os);
            os.flush();
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @MethodComment(
            function = "获取文件-URL",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称")
            })
    public String getObjectUrl(String objectName) throws Exception {
        validateBucketName(defaultBucket);
        return minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .method(Method.GET)
                        .bucket(defaultBucket)
                        .object(objectName)
                        .build());
    }

    @MethodComment(
            function = "获取文件-URL",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称")
            })
    public String getObjectUrl(String bucketName, String objectName) throws Exception {
        return minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .method(Method.GET)
                        .bucket(bucketName)
                        .object(objectName)
                        .build());
    }

    @MethodComment(
            function = "获取文件-URL",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "objectName", description = "存储桶里的对象名称")
            })
    public String getObjectUrl(String bucketName, String region, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        return minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .method(Method.GET)
                        .bucket(bucketName)
                        .region(region)
                        .object(objectName)
                        .build());
    }

    @MethodComment(
            function = "下载文件-下载到本服务器",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "filename", description = "文件存储位置")
            })
    public void downloadObject(String objectName, String filename) throws Exception {
        validateBucketName(defaultBucket);
        minioClient.downloadObject(DownloadObjectArgs.builder()
                .bucket(defaultBucket)
                .object(objectName)
                .filename(filename)
                .build());
    }

    @MethodComment(
            function = "下载文件-下载到本服务器",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "filename", description = "文件存储位置")
            })
    public void downloadObject(String bucketName, String objectName, String filename) throws Exception {
        minioClient.downloadObject(DownloadObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .filename(filename)
                .build());
    }

    @MethodComment(
            function = "下载文件-下载到本服务器",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "filename", description = "文件存储位置")
            })
    public void downloadObject(String bucketName, String region, String objectName, String filename) throws Exception {
        minioClient.downloadObject(DownloadObjectArgs.builder()
                .bucket(bucketName)
                .region(region)
                .object(objectName)
                .filename(filename)
                .build());
    }

    @MethodComment(
            function = "获取对象的元数据",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "filename", description = "文件存储位置")
            },
            description = "调用statObject()来判断对象是否存在,如果不存在, statObject()抛出异常")
    public Map<String, String> getObjectUserMetadata(String objectName) throws Exception {
        validateBucketName(defaultBucket);
        return minioClient.statObject(StatObjectArgs.builder()
                .bucket(defaultBucket)
                .object(objectName)
                .build()).userMetadata();
    }

    @MethodComment(
            function = "获取对象的元数据",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "filename", description = "文件存储位置")
            },
            description = "调用statObject()来判断对象是否存在,如果不存在, statObject()抛出异常")
    public Map<String, String> getObjectUserMetadata(String bucketName, String objectName) throws Exception {
        return minioClient.statObject(StatObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build()).userMetadata();
    }

    @MethodComment(
            function = "获取对象的元数据",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "filename", description = "文件存储位置")
            },
            description = "调用statObject()来判断对象是否存在,如果不存在, statObject()抛出异常")
    public Map<String, String> getObjectUserMetadata(String bucketName, String region, String objectName) throws Exception {
        return minioClient.statObject(StatObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .region(region)
                .build()).userMetadata();
    }

    @MethodComment(
            function = "统计对象(含元数据)-判断对象是否存在",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "filename", description = "文件存储位置")
            },
            description = "调用statObject()来判断对象是否存在,如果不存在, statObject()抛出异常")
    public StatObjectResponse statObjectResponse(String objectName) throws Exception {
        validateBucketName(defaultBucket);
        return minioClient.statObject(StatObjectArgs.builder()
                .bucket(defaultBucket)
                .object(objectName)
                .build());
    }

    @MethodComment(
            function = "统计对象(含元数据)-判断对象是否存在",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "filename", description = "文件存储位置")
            },
            description = "调用statObject()来判断对象是否存在,如果不存在, statObject()抛出异常")
    public StatObjectResponse statObjectResponse(String bucketName, String objectName) throws Exception {
        return minioClient.statObject(StatObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build());
    }

    @MethodComment(
            function = "统计对象(含元数据)-判断对象是否存在",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "filename", description = "文件存储位置")
            },
            description = "调用statObject()来判断对象是否存在,如果不存在, statObject()抛出异常")
    public StatObjectResponse statObjectResponse(String bucketName, String region, String objectName) throws Exception {
        return minioClient.statObject(StatObjectArgs.builder()
                .bucket(bucketName)
                .region(region)
                .object(objectName)
                .build());
    }

    @MethodComment(
            function = "获取文件外链-url",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "文件ID(存储桶里的对象名称)"),
                    @Param(name = "expiry", description = "失效时间（以秒为单位），默认是7天，不得大于七天")
            },
            description = "生成一个给HTTP GET请求用的presigned URL。浏览器/移动端的客户端可以用这个URL进行下载，即使其所在的存储桶是私有的。这个presigned URL可以设置一个失效时间，默认值是7天")
    public String presignedGetObjectGetUrl(String bucketName, String objectName, int expiry) throws Exception {
        return minioClient.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .expiry(expiry)
                .build());
    }

    @MethodComment(
            function = "获取文件外链-url",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "objectName", description = "文件ID(存储桶里的对象名称)"),
                    @Param(name = "expiry", description = "失效时间（以秒为单位），默认是7天，不得大于七天")
            },
            description = "生成一个给HTTP GET请求用的presigned URL。浏览器/移动端的客户端可以用这个URL进行下载，即使其所在的存储桶是私有的。这个presigned URL可以设置一个失效时间，默认值是7天")
    public String presignedGetObjectGetUrl(String bucketName, String region, String objectName, int expiry) throws Exception {
        return minioClient.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
                .bucket(bucketName)
                .region(region)
                .object(objectName)
                .expiry(expiry)
                .build());
    }

    @MethodComment(
            function = "获取文件外链-url",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "objectName", description = "文件ID(存储桶里的对象名称)"),
                    @Param(name = "expiry", description = "失效时间（以秒为单位），默认是7天，不得大于七天")
            },
            description = "生成一个给HTTP PUT请求用的presigned URL,浏览器/移动端的客户端可以用这个URL进行上传，即使其所在的存储桶是私有的。这个presigned URL可以设置一个失效时间，默认值是7天")
    public String getPresignedObjectPutUrl(String bucketName, String objectName, Integer expires) throws Exception {
        String url = "";
        validateExpiry(expires);
        Map<String, String> reqParams = new HashMap<String, String>();
        reqParams.put("response-content-type", "application/json");
        url = minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .method(Method.PUT)
                        .bucket(bucketName)
                        .object(objectName)
                        .expiry(expires)
                        .extraQueryParams(reqParams)
                        .build());
        return url;
    }

    @MethodComment(
            function = "根据文件前缀查询文件",
            params = {
                    @Param(name = "objectName", description = "文件ID(存储桶里的对象名称)"),
                    @Param(name = "recursive", description = "是否递归子目录")
            })
    public List getAllObjectsByPrefix(String prefix, boolean recursive) throws Exception {
        validateBucketName(defaultBucket);
        List<Item> list = new ArrayList<>();
        Iterable<Result<Item>> objectsIterator = minioClient.listObjects(ListObjectsArgs.builder()
                .bucket(defaultBucket)
                .prefix(prefix)
                .recursive(recursive)
                .build());
        if (objectsIterator != null) {
            Iterator<Result<Item>> iterator = objectsIterator.iterator();
            if (iterator != null) {
                while (iterator.hasNext()) {
                    Result<Item> result = iterator.next();
                    Item item = result.get();
                    list.add(item);
                }
            }
        }

        return list;
    }

    @MethodComment(
            function = "根据文件前缀查询文件",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "文件ID(存储桶里的对象名称)"),
                    @Param(name = "recursive", description = "是否递归子目录")
            })
    public List getAllObjectsByPrefix(String bucketName, String prefix, boolean recursive) throws Exception {
        List<Item> list = new ArrayList<>();
        Iterable<Result<Item>> objectsIterator = minioClient.listObjects(ListObjectsArgs.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .recursive(recursive)
                .build());
        if (objectsIterator != null) {
            Iterator<Result<Item>> iterator = objectsIterator.iterator();
            if (iterator != null) {
                while (iterator.hasNext()) {
                    Result<Item> result = iterator.next();
                    Item item = result.get();
                    list.add(item);
                }
            }
        }

        return list;
    }

    @MethodComment(
            function = "删除文件-单个",
            params = {
                    @Param(name = "objectName", description = "文件ID(存储桶里的对象名称)")
            })
    public void deleteObject(String objectName) throws Exception {
        validateBucketName(defaultBucket);
        minioClient.removeObject(
                RemoveObjectArgs.builder()
                        .bucket(defaultBucket)
                        .object(objectName)
                        .build());
    }


    @MethodComment(
            function = "删除文件-单个",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "文件ID(存储桶里的对象名称)")
            })
    public void deleteObject(String bucketName, String objectName) throws Exception {
        minioClient.removeObject(
                RemoveObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .build());
    }

    @MethodComment(
            function = "删除文件-单个",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "objectName", description = "文件ID(存储桶里的对象名称)")
            })
    public void deleteObject(String bucketName, String region, String objectName) throws Exception {
        minioClient.removeObject(
                RemoveObjectArgs.builder()
                        .bucket(bucketName)
                        .region(region)
                        .object(objectName)
                        .build());
    }

    @MethodComment(
            function = "删除文件-多个",
            params = {
                    @Param(name = "objectName", description = "文件ID(存储桶里的对象名称)")
            })
    public void deleteObjects(List<DeleteObject> objectNames) throws Exception {
        validateBucketName(defaultBucket);
        for (Result<DeleteError> errorResult : minioClient.removeObjects(RemoveObjectsArgs.builder().bucket(defaultBucket).objects(objectNames).build())) {
            DeleteError deleteError = errorResult.get();
            logger.error("Failed to remove {}，DeleteError:", deleteError.message());
        }
    }

    @MethodComment(
            function = "删除文件-多个",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "文件ID(存储桶里的对象名称)")
            })
    public void deleteObjects(String bucketName, List<DeleteObject> objectNames) throws Exception {
        for (Result<DeleteError> errorResult : minioClient.removeObjects(RemoveObjectsArgs.builder().bucket(bucketName).objects(objectNames).build())) {
            DeleteError deleteError = errorResult.get();
            logger.error("Failed to remove {}，DeleteError:", deleteError.message());
        }
    }

    @MethodComment(
            function = "删除文件-多个",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "objectName", description = "文件ID(存储桶里的对象名称)")
            })
    public void deleteObjects(String bucketName, String region, List<DeleteObject> objectNames) throws Exception {
        for (Result<DeleteError> errorResult : minioClient.removeObjects(RemoveObjectsArgs.builder().bucket(bucketName).region(region).objects(objectNames).build())) {
            DeleteError deleteError = errorResult.get();
            logger.error("Failed to remove {}，DeleteError:", deleteError.message());
        }
    }

    /**
     * 验证url到期时间
     * @param expiry
     */
    private void validateExpiry(int expiry) {
        if (expiry < 1 || expiry > DEFAULT_EXPIRY_TIME) {
            throw new IllegalArgumentException(
                    "expiry must be minimum 1 second to maximum "
                            + TimeUnit.SECONDS.toDays(DEFAULT_EXPIRY_TIME)
                            + " days");
        }
    }

}
