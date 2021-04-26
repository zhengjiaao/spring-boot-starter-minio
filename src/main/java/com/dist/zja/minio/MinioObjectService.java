package com.dist.zja.minio;

import com.dist.zja.minio.common.annotations.ClassComment;
import com.dist.zja.minio.common.annotations.MethodComment;
import com.dist.zja.minio.common.annotations.Param;
import com.dist.zja.minio.properties.MinioProperties;
import com.google.common.io.ByteStreams;
import io.minio.*;
import io.minio.http.Method;
import io.minio.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Company: 上海数慧系统技术有限公司
 * Department: 数据中心
 * Date: 2021-01-25 10:40
 * Author: zhengja
 * Email: zhengja@dist.com.cn
 * Desc：
 */
@ClassComment(value = "Minio 对象服务-操作对象", author = "zhengja")
public class MinioObjectService {

    public static Logger logger = LoggerFactory.getLogger(MinioObjectService.class);

    private MinioClient minioClient;

    private String url;

    private String defaultBucket;

    public MinioObjectService(MinioClient minioClient) {
        this.minioClient = minioClient;
    }

    public MinioObjectService(MinioClient minioClient, MinioProperties minIo) {
        this.minioClient = minioClient;
        this.url = minIo.getEndpoint() + ":" + minIo.getPort();
        this.defaultBucket = minIo.getDefaultBucket();
    }

    public void init() {
        logger.info("com.dist.zja.minio.MinIoObjectService  Init Success！");
    }

    /**
     * 验证桶名称
     * @param name
     */
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

    /**
     * 验证是否配置默认桶
     * @param arg
     * @param argName
     */
    protected void validateNotNull(Object arg, String argName) {
        if (arg == null) {
            throw new IllegalArgumentException(argName + " must not be null,Must be configured dist.minio.config.default-bucket=");
        }
    }

    @MethodComment(
            function = "默认桶-对象上传-本地对象路径",
            params = {
                    @Param(name = "objectName", description = "对象id(存储名称)"),
                    @Param(name = "filePath", description = "本地对象路径")
            })
    public ObjectWriteResponse putObject(String objectName, String filename) throws Exception {
        validateBucketName(defaultBucket);
        if (Files.isDirectory(Paths.get(filename))) {
            File dirs = new File(filename);
            if (dirs.list().length < 0) {
                return putObjectByFolder(objectName);
            }
            for (File f : dirs.listFiles()) {

            }
        }

        return putObject(defaultBucket, objectName, filename);
    }




    @MethodComment(
            function = "指定桶-对象上传-本地对象路径",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "对象id(存储名称)"),
                    @Param(name = "filePath", description = "本地对象路径")
            })
    public ObjectWriteResponse putObject(String bucketName, String objectName, String filename) throws Exception {
        return minioClient.uploadObject(
                UploadObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .filename(filename)
                        .build());
    }

    @MethodComment(
            function = "默认桶-创建对象(文件夹or目录)",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "folderName", description = "对象id(存储名称)")
            }, description = "创建对象以“ /”结尾（也称为文件夹或目录）")
    public ObjectWriteResponse putObjectByFolder(String folderName) throws Exception {
        validateBucketName(defaultBucket);
        return putObjectByFolder(defaultBucket, folderName);
    }

    @MethodComment(
            function = "指定桶-创建对象(文件夹or目录)",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "folderName", description = "对象id(存储名称)")
            }, description = "创建对象以“ /”结尾（也称为文件夹或目录）")
    public ObjectWriteResponse putObjectByFolder(String bucketName, String folderName) throws Exception {
        return minioClient.putObject(
                PutObjectArgs.builder().bucket(bucketName).object(folderName + "/").stream(
                        new ByteArrayInputStream(new byte[]{}), 0, -1)
                        .build());
    }

    @MethodComment(
            function = "默认桶-对象上传-multipartFile",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "multipartFile", description = "多部分单个对象")
            },
            description = "按指定 objectName 存储")
    public ObjectWriteResponse putObjectByMultipartFile(String objectName, MultipartFile multipartFile) {
        validateBucketName(defaultBucket);
        return putObjectByMultipartFile(defaultBucket, objectName, multipartFile);
    }

    @MethodComment(
            function = "指定桶-对象上传-multipartFile",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "multipartFile", description = "多部分单个对象")
            },
            description = "按指定 objectName 存储")
    public ObjectWriteResponse putObjectByMultipartFile(String bucketName, String objectName, MultipartFile multipartFile) {
        try (InputStream inputStream = multipartFile.getInputStream()) {
            return minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    // Upload known sized input stream  上载已知大小的输入流
                    .stream(inputStream, multipartFile.getSize(), -1)
                    .contentType(multipartFile.getContentType())
                    .build());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            e.printStackTrace();
        }
        return null;
    }

    @MethodComment(
            function = "默认桶-上传对象-InputStream",
            params = {
                    @Param(name = "objectName", description = "对象名称"),
                    @Param(name = "InputStream", description = "对象流")
            })
    public ObjectWriteResponse putObject(String objectName, InputStream stream) {
        validateBucketName(defaultBucket);
        return putObject(defaultBucket, objectName, stream);
    }

    @MethodComment(
            function = "指定桶-上传对象-InputStream",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "对象名称"),
                    @Param(name = "InputStream", description = "对象流")
            })
    public ObjectWriteResponse putObject(String bucketName, String objectName, InputStream stream) {
        try {
            ObjectWriteResponse objectWriteResponse = minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    // Upload unknown sized input stream 上载大小未知的输入流
//                    .stream(stream, stream.available(), ObjectWriteArgs.MAX_PART_SIZE)
                    .stream(stream, stream.available(), -1)
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
            function = "默认桶-上传对象-InputStream",
            params = {
                    @Param(name = "objectName", description = "对象名称"),
                    @Param(name = "InputStream", description = "对象流")
            })
    public ObjectWriteResponse putObject(String objectName, InputStream stream, Map<String, String> headers, Map<String, String> userMetadata) {
        validateBucketName(defaultBucket);
        return putObject(defaultBucket, objectName, stream, headers, userMetadata);
    }

    @MethodComment(
            function = "指定桶-上传对象-InputStream",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "对象名称"),
                    @Param(name = "InputStream", description = "对象流")
            })
    public ObjectWriteResponse putObject(String bucketName, String objectName, InputStream stream, Map<String, String> headers, Map<String, String> userMetadata) {
        try {
            ObjectWriteResponse objectWriteResponse = minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    // Upload unknown sized input stream 上载大小未知的输入流，底层自动检查分片上传
//                    .stream(stream, -1, ObjectWriteArgs.MAX_OBJECT_SIZE)
                    .stream(stream, stream.available(), -1)
                    // Upload input stream with headers and user metadata  上传带有标题和用户元数据的输入流
                    .headers(headers)
                    .userMetadata(userMetadata)
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
            function = "默认桶-上传对象-InputStream",
            params = {
                    @Param(name = "objectName", description = "对象名称"),
                    @Param(name = "InputStream", description = "对象流"),
                    @Param(name = "contentType", description = "内容类型")
            })
    public ObjectWriteResponse putObject(String objectName, InputStream stream, String contentType) {
        validateBucketName(defaultBucket);
        return putObject(defaultBucket, objectName, stream, contentType);
    }

    @MethodComment(
            function = "指定桶-上传对象-InputStream",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "对象名称"),
                    @Param(name = "InputStream", description = "对象流"),
                    @Param(name = "contentType", description = "内容类型")
            })
    public ObjectWriteResponse putObject(String bucketName, String objectName, InputStream stream, String contentType) {
        try {
            ObjectWriteResponse objectWriteResponse = minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(stream, stream.available(), ObjectWriteArgs.MAX_OBJECT_SIZE)
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
            function = "默认桶-获取对象流",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称")
            })
    public GetObjectResponse getObject(String objectName) throws Exception {
        validateBucketName(defaultBucket);
        return getObject(defaultBucket, objectName);
    }

    @MethodComment(
            function = "指定桶-获取对象流",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称")
            })
    public GetObjectResponse getObject(String bucketName, String objectName) throws Exception {
        return minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .build());
    }

    @MethodComment(
            function = "默认桶-获取对象流-支持断点下载",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "offset", description = "offset 是起始字节的位置"),
                    @Param(name = "length", description = "length是要读取的长度 (可选，如果无值则代表读到对象结尾)")
            },
            description = "下载对象指定区域的字节数组做为流。（断点下载）")
    public GetObjectResponse getObject(String objectName, Long offset, Long length) throws Exception {
        validateBucketName(defaultBucket);
        return getObject(defaultBucket, objectName, offset, length);
    }

    @MethodComment(
            function = "指定桶-获取对象流-支持断点下载",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "offset", description = "offset 是起始字节的位置"),
                    @Param(name = "length", description = "length是要读取的长度 (可选，如果无值则代表读到对象结尾)")
            },
            description = "下载对象指定区域的字节数组做为流。（断点下载）")
    public GetObjectResponse getObject(String bucketName, String objectName, Long offset, Long length) throws Exception {
        return minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .offset(offset)
                        .length(length)
                        .build());
    }

    @MethodComment(
            function = "默认桶-对象下载-流-response方式",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "HttpServletResponse", description = "response")
            })
    public void downloadObject(String objectName, HttpServletResponse response) {
        validateBucketName(defaultBucket);
        downloadObject(defaultBucket, objectName, response);
    }

    @MethodComment(
            function = "指定桶-对象下载-流-response方式",
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
            function = "默认桶-下载对象-下载到本服务器",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "filename", description = "对象存储位置")
            })
    public void downloadObject(String objectName, String filename) throws Exception {
        validateBucketName(defaultBucket);
        downloadObject(defaultBucket, objectName, filename);
    }

    @MethodComment(
            function = "指定桶-下载对象-下载到本服务器",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "filename", description = "对象存储位置")
            })
    public void downloadObject(String bucketName, String objectName, String filename) throws Exception {
        minioClient.downloadObject(DownloadObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .filename(filename)
                .build());
    }

    @MethodComment(
            function = "默认桶-获取对象信息和对象的元数据",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "filename", description = "对象存储位置")
            },
            description = "调用statObject()来判断对象是否存在,如果不存在, statObject()抛出异常")
    public StatObjectResponse statObject(String objectName) throws Exception {
        validateBucketName(defaultBucket);
        return statObject(defaultBucket, objectName);
    }

    @MethodComment(
            function = "指定桶-获取对象信息和对象的元数据",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "filename", description = "对象存储位置")
            },
            description = "调用statObject()来判断对象是否存在,如果不存在, statObject()抛出异常")
    public StatObjectResponse statObject(String bucketName, String objectName) throws Exception {
        return minioClient.statObject(StatObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build());
    }

    @MethodComment(
            function = "默认桶-获取对象的元数据",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称")
            },
            description = "调用statObject()来判断对象是否存在,如果不存在, statObject()抛出异常")
    public Map<String, String> getObjectUserMetadata(String objectName) throws Exception {
        validateBucketName(defaultBucket);
        return getObjectUserMetadata(defaultBucket, objectName);
    }

    @MethodComment(
            function = "指定桶-获取对象的元数据",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称")
            },
            description = "调用statObject()来判断对象是否存在,如果不存在, statObject()抛出异常")
    public Map<String, String> getObjectUserMetadata(String bucketName, String objectName) throws Exception {
        return minioClient.statObject(StatObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build()).userMetadata();
    }

    @MethodComment(
            function = "默认桶-通过SQL表达式选择对象的内容",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "sqlExpression", description = "sql表达式 例如：select * from S3Object")
            }, description = "具体使用参考官网")
    public SelectResponseStream selectObjectContent(String objectName, String sqlExpression) throws Exception {
        validateBucketName(defaultBucket);
        return selectObjectContent(defaultBucket, objectName, sqlExpression);
    }

    @MethodComment(
            function = "指定桶-通过SQL表达式选择对象的内容",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称"),
                    @Param(name = "sqlExpression", description = "sql表达式 例如：select * from S3Object")
            }, description = "具体使用参考官网")
    public SelectResponseStream selectObjectContent(String bucketName, String objectName, String sqlExpression) throws Exception {
        InputSerialization is = new InputSerialization(null, false, null, null, FileHeaderInfo.USE, null, null, null);
        OutputSerialization os = new OutputSerialization(null, null, null, QuoteFields.ASNEEDED, null);
        return minioClient.selectObjectContent(
                SelectObjectContentArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .sqlExpression(sqlExpression)
                        .inputSerialization(is)
                        .outputSerialization(os)
                        .requestProgress(true)
                        .build());
    }

    @MethodComment(
            function = "默认桶-获取对象永久网址-URL",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称")
            }, description = "必须设置桶策略为可读(下载),只写权限，用户直接访问地址是查看不了的")
    public String getObjectURL(String objectName) {
        validateBucketName(defaultBucket);
        return getObjectURL(defaultBucket, objectName);
    }

    @MethodComment(
            function = "指定桶-获取对象永久网址-URL",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称")
            }, description = "必须设置桶策略为可读(下载),只写权限，用户直接访问地址是查看不了的")
    public String getObjectURL(String bucketName, String objectName) {
        try {
            statObject(bucketName, objectName);
            return url + "/" + bucketName + "/" + objectName;
        } catch (Exception e) {
//            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return null;
    }

    @MethodComment(
            function = "默认桶-获取预签名对象网址-URL",
            params = {
                    @Param(name = "objectName", description = "存储桶里的对象名称")
            }, description = "默认分享链接地址失效时间为7天")
    public String getPresignedObjectUrl(String objectName) throws Exception {
        validateBucketName(defaultBucket);
        return getPresignedObjectUrl(defaultBucket, objectName);
    }

    @MethodComment(
            function = "指定桶-获取预签名对象网址-URL",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "存储桶里的对象名称")
            }, description = "默认分享链接地址失效时间为7天")
    public String getPresignedObjectUrl(String bucketName, String objectName) throws Exception {
        return minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .method(Method.GET)
                        .bucket(bucketName)
                        .object(objectName)
                        .build());
    }

    @MethodComment(
            function = "默认桶-获取对象外链-url-自定义设置分享过期时间",
            params = {
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)"),
                    @Param(name = "expiry", description = "失效时间（以秒为单位），默认是7天，不得大于七天")
            },
            description = "设置有效期的分享链接（共享文件时间最大7天）。生成一个给HTTP GET请求用的presigned URL。浏览器/移动端的客户端可以用这个URL进行下载，即使其所在的存储桶是私有的。这个presigned URL可以设置一个失效时间，默认值是7天")
    public String presignedGetObjectGetUrl(String objectName, int expiry) throws Exception {
        validateBucketName(defaultBucket);
        return presignedGetObjectGetUrl(defaultBucket, objectName, expiry);
    }

    @MethodComment(
            function = "指定桶-获取对象外链-url-自定义设置分享过期时间",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)"),
                    @Param(name = "expiry", description = "失效时间（以秒为单位），默认是7天，不得大于七天")
            },
            description = "设置有效期的分享链接（共享文件时间最大7天）。生成一个给HTTP GET请求用的presigned URL。浏览器/移动端的客户端可以用这个URL进行下载，即使其所在的存储桶是私有的。这个presigned URL可以设置一个失效时间，默认值是7天")
    public String presignedGetObjectGetUrl(String bucketName, String objectName, int expiry) throws Exception {
        return minioClient.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .expiry(expiry)
                .build());
    }

    @MethodComment(
            function = "默认桶-根据对象前缀查询对象",
            params = {
                    @Param(name = "prefix", description = "桶中对象的前缀 默认 空字符串"),
                    @Param(name = "recursive", description = "是否递归子目录")
            })
    public List getAllObjectsByPrefix(String prefix, boolean recursive) throws Exception {
        validateBucketName(defaultBucket);
        return getAllObjectsByPrefix(defaultBucket, prefix, recursive);
    }

    @MethodComment(
            function = "指定桶-根据对象前缀查询对象",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "prefix", description = "桶中对象的前缀 默认 空字符串"),
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
            function = "默认桶-组合源对象列表",
            params = {
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)")
            }, description = "通过使用服务器端副本组合来自不同源对象的数据来创建对象，服务器上已存在的对象列表再次组合成一个对象")
    public ObjectWriteResponse composeObject(String objectName, List<ComposeSource> sourceObjectList) throws Exception {
        validateBucketName(defaultBucket);
        return composeObject(defaultBucket, objectName, sourceObjectList);
    }

    @MethodComment(
            function = "指定桶-组合源对象列表",
            params = {
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)")
            }, description = "通过使用服务器端副本组合来自不同源对象的数据来创建对象，服务器上已存在的对象列表再次组合成一个对象")
    public ObjectWriteResponse composeObject(String bucketName, String objectName, List<ComposeSource> sourceObjectList) throws Exception {
        return minioClient.composeObject(
                ComposeObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .sources(sourceObjectList)
                        .build());
    }

    @MethodComment(
            function = "默认桶-通过服务器端从另一个对象复制数据来创建对象",
            params = {
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)")
            }, description = "通过使用服务器端副本组合来自不同源对象的数据来创建对象，服务器上已存在的对象列表再次组合成一个对象")
    public ObjectWriteResponse copyObject(String objectName, CopySource source) throws Exception {
        validateBucketName(defaultBucket);
        return copyObject(defaultBucket, objectName, source);
    }

    @MethodComment(
            function = "指定桶-通过服务器端从另一个对象复制数据来创建对象",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)"),
                    @Param(name = "source", description = "已存在的源对象")
            }, description = "通过使用服务器端副本组合来自不同源对象的数据来创建对象，服务器上已存在的对象列表再次组合成一个对象")
    public ObjectWriteResponse copyObject(String bucketName, String objectName, CopySource source) throws Exception {
        return minioClient.copyObject(
                CopyObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .source(source)
                        .build());
    }

    @MethodComment(
            function = "默认桶-设置对象的标签",
            params = {
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)")
            })
    public void setObjectTags(String objectName, Tags tags) throws Exception {
        validateBucketName(defaultBucket);
        setObjectTags(defaultBucket, objectName, tags);
    }

    @MethodComment(
            function = "指定桶-设置对象的标签",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)")
            })
    public void setObjectTags(String bucketName, String objectName, Tags tags) throws Exception {
        minioClient.setObjectTags(
                SetObjectTagsArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .tags(tags).build());
    }

    @MethodComment(
            function = "默认桶-获取对象的标签",
            params = {
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)")
            })
    public void getObjectTags(String objectName) throws Exception {
        validateBucketName(defaultBucket);
        getObjectTags(defaultBucket, objectName);
    }

    @MethodComment(
            function = "指定桶-获取对象的标签",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)")
            })
    public Tags getObjectTags(String bucketName, String objectName) throws Exception {
        return minioClient.getObjectTags(
                GetObjectTagsArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .build());
    }

    @MethodComment(
            function = "默认桶-删除对象标签",
            params = {
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)")
            })
    public void deleteObjectTags(String objectName) throws Exception {
        validateBucketName(defaultBucket);
        deleteObjectTags(defaultBucket, objectName);
    }

    @MethodComment(
            function = "指定桶-删除对象标签",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)")
            })
    public void deleteObjectTags(String bucketName, String objectName) throws Exception {
        minioClient.deleteObjectTags(
                DeleteObjectTagsArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .build());
    }

    @MethodComment(
            function = "默认桶-删除对象-单个",
            params = {
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)")
            })
    public void deleteObject(String objectName) throws Exception {
        validateBucketName(defaultBucket);
        deleteObject(defaultBucket, objectName);
    }

    @MethodComment(
            function = "指定桶-删除对象-单个",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)")
            })
    public void deleteObject(String bucketName, String objectName) throws Exception {
        minioClient.removeObject(
                RemoveObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .build());
    }

    @MethodComment(
            function = "默认桶-删除对象-多个",
            params = {
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)")
            })
    public void deleteObjects(List<DeleteObject> objectNames) throws Exception {
        validateBucketName(defaultBucket);
        deleteObjects(defaultBucket, objectNames);
    }

    @MethodComment(
            function = "指定桶-删除对象-多个",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "objectName", description = "对象ID(存储桶里的对象名称)")
            })
    public void deleteObjects(String bucketName, List<DeleteObject> objectNames) throws Exception {
        for (Result<DeleteError> errorResult : minioClient.removeObjects(RemoveObjectsArgs.builder().bucket(bucketName).objects(objectNames).build())) {
            DeleteError deleteError = errorResult.get();
            logger.error("Failed to remove {}，DeleteError:", deleteError.message());
        }
    }

}
