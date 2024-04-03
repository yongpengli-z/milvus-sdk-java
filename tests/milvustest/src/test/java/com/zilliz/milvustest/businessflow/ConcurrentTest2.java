package com.zilliz.milvustest.businessflow;

import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.SearchResults;
import io.milvus.param.*;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.credential.CreateCredentialParam;
import io.milvus.param.credential.DeleteCredentialParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.role.AddUserToRoleParam;
import io.milvus.response.SearchResultsWrapper;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * @Author yongpeng.li
 * @Date 2023/5/5 11:27
 */
@Slf4j

public class ConcurrentTest2 {
    int THREAD = System.getProperty("thread") == null ? 10 : Integer.parseInt(System.getProperty("thread"));
    int dataNum = System.getProperty("dataNum") == null ? 10000 : Integer.parseInt(System.getProperty("dataNum"));
    int searchNum = System.getProperty("searchNum") == null ? 10 : Integer.parseInt(System.getProperty("searchNum"));
    int TopK = System.getProperty("TopK") == null ? 1 : Integer.parseInt(System.getProperty("TopK"));
    int nprobe = System.getProperty("nprobe") == null ? 1 : Integer.parseInt(System.getProperty("nprobe"));
    int nq = System.getProperty("nq") == null ? 1 : Integer.parseInt(System.getProperty("nq"));
    String host = System.getProperty("host") == null ? "10.104.31.122" : System.getProperty("host");
    int port = System.getProperty("port") == null ? 19530 : Integer.parseInt(System.getProperty("port"));

    int runTime = System.getProperty("runtime") == null ? 1 : Integer.parseInt(System.getProperty("runtime"));

    Object[][] objects = new Object[][]{};

    @DataProvider(name = "UserInfo")
    public Object[][] provideUser() {
        String[][] userinfo = new String[THREAD][2];
        for (int i = 0; i < THREAD; i++) {
            userinfo[i][0] = "Username" + i;
            userinfo[i][1] = "Password" + i;
        }
        return userinfo;
    }


    @AfterClass()
    public void cleanTestData() {
        MilvusServiceClient milvusClient =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withHost(host)
                                .withPort(port)
                                .withSecure(false)
                                .withAuthorization("root", "Milvus")
                                .build());
        for (int i = 0; i < THREAD; i++) {
            milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName("collection" + i).build());
            milvusClient.deleteCredential(DeleteCredentialParam.newBuilder().withUsername("Username" + i).build());
        }
    }

    @Test(dataProvider = "UserInfo")
    public void registerUserInfo(String username, String password) {
        MilvusServiceClient milvusClient =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withHost(host)
                                .withPort(port)
                                .withSecure(false)
                                .withAuthorization("root", "Milvus")
                                .build());
        R<RpcStatus> credential = milvusClient.createCredential(CreateCredentialParam.newBuilder().withUsername(username).withPassword(password).build());
        log.info(String.valueOf(credential.getStatus()));
        log.info(credential.getData().toString());
        milvusClient.close();

    }

    @Test(dataProvider = "UserInfo", dependsOnMethods = "registerUserInfo")
    public void addUserToRole(String username, String password) {
        MilvusServiceClient milvusClient =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withHost(host)
                                .withPort(port)
                                .withSecure(false)
                                .withAuthorization("root", "Milvus")
                                .build());
        R<RpcStatus> rpcStatusR =
                milvusClient.addUserToRole(
                        AddUserToRoleParam.newBuilder()
                                .withUserName(username)
                                .withRoleName("admin")
                                .build());
        Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
        milvusClient.close();
    }

    @Test(dependsOnMethods = "addUserToRole")
    public void repeatCreateCollection() throws ExecutionException, InterruptedException {
        int threads = THREAD;
        LocalDateTime endTime = LocalDateTime.now().plusHours(runTime);
        ArrayList<Future> list = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        for (int e = 0; e < threads; e++) {
            int finalE = e;
            Callable callable = () -> {
                MilvusServiceClient milvusClient =
                        new MilvusServiceClient(
                                ConnectParam.newBuilder()
                                        .withHost(host)
                                        .withPort(port)
                                        .withSecure(false)
                                        .withAuthorization("Username" + finalE, "Password" + finalE)
                                        .build());
                do {
                    // 创建collection
                    String collectionName = "collection" + finalE;
                    FieldType fieldType1 =
                            FieldType.newBuilder()
                                    .withName("book_id")
                                    .withDataType(DataType.Int64)
                                    .withPrimaryKey(true)
                                    .withAutoID(false)
                                    .build();
                    FieldType fieldType2 =
                            FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
                    FieldType fieldType3 =
                            FieldType.newBuilder()
                                    .withName("book_intro")
                                    .withDataType(DataType.FloatVector)
                                    .withDimension(128)
                                    .build();
                    CreateCollectionParam createCollectionReq =
                            CreateCollectionParam.newBuilder()
                                    .withCollectionName(collectionName)
                                    .withDescription("Test " + collectionName + " search")
                                    .withShardsNum(1)
                                    .addFieldType(fieldType1)
                                    .addFieldType(fieldType2)
                                    .addFieldType(fieldType3)
                                    .build();
                    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
                    System.out.println("线程" + finalE + ":用户Username" + finalE + "创建collection：" + collectionName);
                    System.out.println("线程" + finalE + ":用户Username" + finalE + "state：" + collection.getStatus());
                    try {
                        Thread.sleep(1000l);
                    } catch (InterruptedException ex) {
                        System.out.println("exception:"+ex.getMessage());
                    }
                    List<InsertParam.Field> fields = CommonFunction.generateData(dataNum);
                    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder().withCollectionName(collectionName).withFields(fields).build());
                    System.out.println("线程" + finalE + ":用户Username" + finalE + "insert data：" + insert.getStatus());
                    R<RpcStatus> rpcStatusR =
                            milvusClient.createIndex(
                                    CreateIndexParam.newBuilder()
                                            .withCollectionName(collectionName)
                                            .withFieldName(CommonData.defaultVectorField)
                                            .withIndexName(CommonData.defaultIndex)
                                            .withMetricType(MetricType.L2)
                                            .withIndexType(IndexType.HNSW)
                                            .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                                            .withSyncMode(Boolean.TRUE)
                                            .withSyncWaitingTimeout(30L)
                                            .withSyncWaitingInterval(500L)
                                            .build());
                    System.out.println("线程" + finalE + ":用户Username" + finalE + "create index：" + rpcStatusR.getStatus());
                    R<RpcStatus> rpcStatusLoad = milvusClient.loadCollection(LoadCollectionParam.newBuilder().withCollectionName(collectionName)
                            .withSyncLoad(true)
                            .withSyncLoadWaitingInterval(500L)
                            .withSyncLoadWaitingTimeout(300L).build());
                    System.out.println("线程" + finalE + ":用户Username" + finalE + "load：" + rpcStatusLoad.getStatus());
                    // search
                    int vectorNq = nq;
                    for (int i = 0; i < searchNum; i++) {
                        Integer SEARCH_K = TopK; // TopK
                        String SEARCH_PARAM = "{\"nprobe\":" + nprobe + "}";
                        List<String> search_output_fields = Arrays.asList("book_id");

                        List<List<Float>> search_vectors = CommonFunction.generateFloatVectors(vectorNq, 128);
                        SearchParam searchParam =
                                SearchParam.newBuilder()
                                        .withCollectionName(collectionName)
                                        .withMetricType(MetricType.L2)
                                        .withOutFields(search_output_fields)
                                        .withTopK(SEARCH_K)
                                        .withVectors(search_vectors)
                                        .withVectorFieldName(CommonData.defaultVectorField)
                                        .withParams(SEARCH_PARAM)
                                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                                        .build();
                        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
                        System.out.println("线程" + finalE + ":用户Username" + finalE + "search:" + searchResultsR.getStatus());
                    }
                    // drop collection
                    milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(collectionName).build());
                    System.out.println("线程" + finalE + ":用户Username" + finalE + "drop:" + collectionName);

                } while (  LocalDateTime.now().isBefore(endTime));
                return finalE;
            };
            Future future = executorService.submit(callable);
            list.add(future);
        }
        for (Future future : list) {
            System.out.println("运行结果:"+future.get().toString());
        }
        executorService.shutdown();

    }


//    @Test()
    public void createCollection1() {
        int threads = THREAD;
        LocalDateTime endTime = LocalDateTime.now().plusHours(runTime);
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        int finalE=0;
                MilvusServiceClient milvusClient =
                        new MilvusServiceClient(
                                ConnectParam.newBuilder()
                                        .withHost(host)
                                        .withPort(port)
                                        .withSecure(false)
                                        .withAuthorization("Username" + finalE, "Password" + finalE)
                                        .build());
                do {
                    // 创建collection
                    String collectionName = "collection" + finalE;
                    FieldType fieldType1 =
                            FieldType.newBuilder()
                                    .withName("book_id")
                                    .withDataType(DataType.Int64)
                                    .withPrimaryKey(true)
                                    .withAutoID(false)
                                    .build();
                    FieldType fieldType2 =
                            FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
                    FieldType fieldType3 =
                            FieldType.newBuilder()
                                    .withName("book_intro")
                                    .withDataType(DataType.FloatVector)
                                    .withDimension(128)
                                    .build();
                    CreateCollectionParam createCollectionReq =
                            CreateCollectionParam.newBuilder()
                                    .withCollectionName(collectionName)
                                    .withDescription("Test " + collectionName + " search")
                                    .withShardsNum(1)
                                    .addFieldType(fieldType1)
                                    .addFieldType(fieldType2)
                                    .addFieldType(fieldType3)
                                    .build();
                    R<RpcStatus> collection = milvusClient.createCollection(createCollectionReq);
                    System.out.println("线程" + finalE + ":用户Username" + finalE + "创建collection：" + collectionName);
                    System.out.println(collection.getStatus());
                    List<InsertParam.Field> fields = CommonFunction.generateData(dataNum);
                    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder().withCollectionName("collection" + finalE).withFields(fields).build());
                    System.out.println("线程" + finalE + ":用户Username" + finalE + "insert data：" + insert);
                    R<RpcStatus> rpcStatusR =
                            milvusClient.createIndex(
                                    CreateIndexParam.newBuilder()
                                            .withCollectionName("collection" + finalE)
                                            .withFieldName(CommonData.defaultVectorField)
                                            .withIndexName(CommonData.defaultIndex)
                                            .withMetricType(MetricType.L2)
                                            .withIndexType(IndexType.HNSW)
                                            .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                                            .withSyncMode(Boolean.TRUE)
                                            .withSyncWaitingTimeout(30L)
                                            .withSyncWaitingInterval(500L)
                                            .build());
                    System.out.println("线程" + finalE + ":用户Username" + finalE + "create index：" + rpcStatusR);
                    R<RpcStatus> rpcStatusLoad = milvusClient.loadCollection(LoadCollectionParam.newBuilder().withCollectionName("collection" + finalE)
                            .withSyncLoad(true)
                            .withSyncLoadWaitingInterval(500L)
                            .withSyncLoadWaitingTimeout(300L).build());
                    System.out.println("线程" + finalE + ":用户Username" + finalE + "load：" + rpcStatusLoad);
                    // search
                    int vectorNq = nq;
                    for (int i = 0; i < searchNum; i++) {
                        Integer SEARCH_K = TopK; // TopK
                        String SEARCH_PARAM = "{\"nprobe\":" + nprobe + "}";
                        List<String> search_output_fields = Arrays.asList("book_id");
                        List<List<Float>> search_vectors = CommonFunction.generateFloatVectors(vectorNq, 128);
                        SearchParam searchParam =
                                SearchParam.newBuilder()
                                        .withCollectionName(collectionName)
                                        .withMetricType(MetricType.L2)
                                        .withOutFields(search_output_fields)
                                        .withTopK(SEARCH_K)
                                        .withVectors(search_vectors)
                                        .withVectorFieldName(CommonData.defaultVectorField)
                                        .withParams(SEARCH_PARAM)
                                        .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                                        .build();
                        R<SearchResults> searchResultsR = milvusClient.search(searchParam);
                        System.out.println("线程" + finalE + ":用户Username" + finalE + "search:" + searchResultsR);
                    }
                    // drop collection
                    milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(collectionName).build());
                    System.out.println("线程" + finalE + ":用户Username" + finalE + "drop:" + collectionName);
//
                } while ( LocalDateTime.now().isBefore(endTime));


            }







}


