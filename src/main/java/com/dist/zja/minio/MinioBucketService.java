package com.dist.zja.minio;

import com.dist.zja.minio.common.annotations.ClassComment;
import com.dist.zja.minio.common.annotations.MethodComment;
import com.dist.zja.minio.common.annotations.Param;
import io.minio.*;
import io.minio.errors.*;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Company: 上海数慧系统技术有限公司
 * Department: 数据中心
 * Date: 2021-01-25 16:38
 * Author: zhengja
 * Email: zhengja@dist.com.cn
 * Desc：
 */
@ClassComment("Minio 文件服务-操作存储桶")
public class MinioBucketService {

    public static Logger logger = LoggerFactory.getLogger(MinioBucketService.class);

    private MinioClient minioClient;

    public MinioBucketService(MinioClient minioClient) {
        this.minioClient = minioClient;
    }

    public void init() {
        logger.info("com.dist.zja.minio.MinioBucketService  Init Success！");
    }


    @MethodComment(
            function = "判断bucket是否存在",
            params = {@Param(name = "bucketName", description = "桶名")},
            author = "zhengja")
    public boolean bucketExists(String bucketName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        return minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
    }

    @MethodComment(
            function = "判断bucket是否存在",
            params = {@Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域")},
            author = "zhengja")
    public boolean bucketExists(String bucketName, String region) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        return minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).region(region).build());
    }

    @MethodComment(
            function = "创建 bucket",
            params = {@Param(name = "bucketName", description = "桶名")},
            author = "zhengja")
    public boolean makeBucket(String bucketName) {
        try {
            boolean isExist = bucketExists(bucketName);
            if (!isExist) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            }
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    @MethodComment(
            function = "创建 bucket",
            params = {@Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域")},
            author = "zhengja")
    public boolean makeBucket(String bucketName, String region) {
        try {
            boolean isExist = bucketExists(bucketName, region);
            if (!isExist) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).region(region).build());
            }
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    @MethodComment(
            function = "列出所有存储桶名称列表", params = {@Param(name = "无", description = "无")}, author = "zhengja")
    public List<String> listBucketNames() throws Exception {
        List<Bucket> bucketList = listBuckets();
        List<String> bucketListName = new ArrayList<>();
        for (Bucket bucket : bucketList) {
            bucketListName.add(bucket.name());
        }
        return bucketListName;
    }

    @MethodComment(
            function = "列出所有存储桶", params = {@Param(name = "无", description = "无")}, author = "zhengja")
    public List<Bucket> listBuckets() throws Exception {
        return minioClient.listBuckets();
    }

    @MethodComment(
            function = "列出存储桶中的所有对象名称",
            params = {
                    @Param(name = "bucketName", description = "桶名")
            },
            author = "zhengja")
    public List<String> listObjectNames(String bucketName) throws Exception {
        List<String> listObjectNames = new ArrayList<>();
        boolean flag = bucketExists(bucketName);
        if (flag) {
            Iterable<Result<Item>> myObjects = listObjects(bucketName);
            for (Result<Item> result : myObjects) {
                Item item = result.get();
                listObjectNames.add(item.objectName());
            }
        }
        return listObjectNames;
    }

    @MethodComment(
            function = "列出存储桶中的所有对象",
            params = {
                    @Param(name = "bucketName", description = "桶名")
            },
            author = "zhengja")
    public Iterable<Result<Item>> listObjects(String bucketName) throws Exception {
        boolean flag = bucketExists(bucketName);
        if (flag) {
            return minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).build());
        }
        return null;
    }

    @MethodComment(
            function = "获取存储桶策略",
            params = {
                    @Param(name = "bucketName", description = "桶名")
            },
            author = "zhengja")
    public void getBucketPolicy(String bucketName) throws Exception {
        minioClient.getBucketPolicy(
                GetBucketPolicyArgs.builder()
                        .bucket(bucketName)
                        .build());
    }

    @MethodComment(
            function = "获取存储桶策略",
            params = {
                    @Param(name = "bucketName", description = "桶名")
            },
            author = "zhengja")
    public void getBucketPolicy(String bucketName, String region) throws Exception {
        minioClient.getBucketPolicy(
                GetBucketPolicyArgs.builder()
                        .bucket(bucketName)
                        .region(region)
                        .build());
    }


    @MethodComment(
            function = "设定存储桶策略",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "policyJson", description = "策略json")

            },
            author = "zhengja")
    public void setBucketPolicy(String bucketName, String policyJson) throws Exception {
        minioClient.setBucketPolicy(
                SetBucketPolicyArgs.builder()
                        .bucket(bucketName)
                        .config(policyJson)
                        .build());
    }

    @MethodComment(
            function = "设定存储桶策略",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域"),
                    @Param(name = "policyJson", description = "策略json")

            },
            author = "zhengja")
    public void setBucketPolicy(String bucketName, String region, String policyJson) throws Exception {
        minioClient.setBucketPolicy(
                SetBucketPolicyArgs.builder()
                        .bucket(bucketName)
                        .region(region)
                        .config(policyJson)
                        .build());
    }


    @MethodComment(
            function = "删除 bucket",
            params = {
                    @Param(name = "bucketName", description = "桶名")
            },
            author = "zhengja")
    public boolean deleteBucket(String bucketName) {
        try {
            minioClient.deleteBucketEncryption(
                    DeleteBucketEncryptionArgs.builder().bucket(bucketName).build());
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    @MethodComment(
            function = "删除 bucket",
            params = {
                    @Param(name = "bucketName", description = "桶名"),
                    @Param(name = "region", description = "域")
            },
            author = "zhengja")
    public boolean deleteBucket(String bucketName, String region) {
        try {
            minioClient.deleteBucketEncryption(
                    DeleteBucketEncryptionArgs.builder().bucket(bucketName).region(region).build());
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    @MethodComment(
            function = "删除 bucket 只能删除空桶",
            params = {
                    @Param(name = "bucketName", description = "桶名")
            },
            author = "zhengja")
    public boolean deleteBucketByNull(String bucketName) throws Exception {
        boolean flag = bucketExists(bucketName);
        if (flag) {
            Iterable<Result<Item>> myObjects = listObjects(bucketName);
            for (Result<Item> result : myObjects) {
                Item item = result.get();
                // 有对象文件，则删除失败
                if (item.size() > 0) {
                    return false;
                }
            }
            // 删除存储桶，注意，只有存储桶为空时才能删除成功。
            minioClient.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
            flag = bucketExists(bucketName);
            if (!flag) {
                return true;
            }
        }
        return false;
    }


}
