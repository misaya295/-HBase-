package com.cwk.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * 发布微博
 * 互粉
 * 取关
 * 查看微博
 *
 * @author cwk
 */
public class Weibo {
    // Hbase的配置对象
    private Configuration configuration= HBaseConfiguration.create();
    //  创建weibo这个业务的命名空间，3张表
    private static  final  byte[]  NS_WEIBO = Bytes.toBytes("ns_wiebo");
    private static  final  byte[] TABLE_CONTENT = Bytes.toBytes("ns_wiebo:content");
    private static  final  byte[] TABLE_RELATION = Bytes.toBytes("ns_wiebo:relation");
    private static  final  byte[] TABLE_INBOX = Bytes.toBytes("ns_wiebo:inbox");

    private void init() throws IOException {

//         创建微博的命名空间
        initNamespace();
//         创建微博命名表
        initTableContent();
//         创建用户关系表
        initTableRelation();
//         创建收件箱表
        initTableInbox();







    }
    // 创建微博的命名空间
    private void initNamespace() throws IOException {
        Connection connection  = ConnectionFactory.createConnection(configuration);
        Admin  admin=  connection.getAdmin();
        //创建命名空间描述器
        NamespaceDescriptor ns_weibo = NamespaceDescriptor
                .create("ns_wiebo")
                .addConfiguration("creator", "JinJI")
                .addConfiguration("create_time",String.valueOf(System.currentTimeMillis()))
                .build();



        admin.createNamespace(ns_weibo);
        admin.close();
        connection.close();

    }

    /**
     * 表名：ns_weibo:content
     * 列族名：info
     * 列名：content
     * rowkey：用户id_时间戳
     * value:微博内容（文字内容，图片url，视频url，语音url）
     * @throws IOException
     */
    private void initTableRelation() throws IOException {
        Connection connection  = ConnectionFactory.createConnection(configuration);
        Admin  admin=  connection.getAdmin();

        //创建表描述器
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_CONTENT));
        //创建列描述器
        HColumnDescriptor hColumnDescriptor=new HColumnDescriptor("info");
        //设置块缓存
        hColumnDescriptor.setBlockCacheEnabled(true);
        //设置块缓存大小
        hColumnDescriptor.setBlocksize(2 * 1024 * 1024);
        //设置版本确界
        hColumnDescriptor.setMinVersions(1);
        hColumnDescriptor.setMaxVersions(1);

        //将列描述器添加到表描述器汇总
        tableDescriptor.addFamily(hColumnDescriptor);

        //创建表
        admin.createTable(tableDescriptor);
        admin.close();
        connection.close();
    }

    /**
     * 表名：ns_weibo:relation
     * 列族名：attends，fans
     * 列名：用户id
     * value：用户id
     * rowkey：当前操作人用户的id
     * verison：1
     * @throws IOException
     */
    private void initTableInbox() throws IOException {
        Connection connection  = ConnectionFactory.createConnection(configuration);
        Admin  admin=  connection.getAdmin();
        //创建用户描述器
        HTableDescriptor relationtableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_RELATION));
        //创建attends列描述器
        HColumnDescriptor attendColumnDescriptor = new HColumnDescriptor("attends");
        //设置块缓存
        attendColumnDescriptor.setBlockCacheEnabled(true);
        //设置块缓存大小
        attendColumnDescriptor.setBlocksize(2 * 1024 * 1024);

        //创建fans列描述器
        HColumnDescriptor fanshColumnDescriptor = new HColumnDescriptor("fans");
        //设置块缓存
        fanshColumnDescriptor.setBlockCacheEnabled(true);
        //设置块缓存大小
        fanshColumnDescriptor.setBlocksize(2 * 1024 * 1024);
        //设置版本确界
        attendColumnDescriptor.setMinVersions(1);
        attendColumnDescriptor.setMaxVersions(1);

        //设置版本确界
        fanshColumnDescriptor.setMinVersions(1);
        fanshColumnDescriptor.setMaxVersions(1);
        //将两个描述器添加到表描述器中
        relationtableDescriptor.addFamily(attendColumnDescriptor);
        relationtableDescriptor.addFamily(fanshColumnDescriptor);
        //创建表
        admin.createTable(relationtableDescriptor);
        admin.close();
        connection.close();
    }


    /**
     * 表名：ns_weibo:inbox
     * 列族：info
     * 列：当前用户所关注人的id
     * value：微博rowkey
     * rowkey：用户id
     * version：100
     * @throws IOException
     */
    private void initTableContent() throws IOException {
        Connection connection  = ConnectionFactory.createConnection(configuration);
        Admin  admin=  connection.getAdmin();


        HTableDescriptor inboxtableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_INBOX));
        HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor("info");
        //设置块缓存
        infoColumnDescriptor.setBlockCacheEnabled(true);
        //设置块缓存大小
        infoColumnDescriptor.setBlocksize(2 * 1024 * 1024);
        //设置版本确界
        infoColumnDescriptor.setMinVersions(100);
        infoColumnDescriptor.setMaxVersions(100);

        inboxtableDescriptor.addFamily(infoColumnDescriptor);
        admin.createTable(inboxtableDescriptor);
        admin.close();
        connection.close();

    }

    /**
     * 发布微博
     * a、向微博内容中添加刚发布的内容，多了一个微博rowkey
     * b、向发布微博人的粉丝的收件箱表中，添加该微博rowkey
     *
     */
    public void  publishContent(String uid,String content) throws IOException {

        Connection connection = ConnectionFactory.createConnection(configuration);
        //得到微博表对象
        Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        //a
        //组装rowkey
        String rowkey = uid + "_" +System.currentTimeMillis();
        //添加微博内容到微博表
        Put contentPut = new Put(Bytes.toBytes(rowkey));
        contentPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes("content"));
        contentTable.put(contentPut);
        //b
        //查询用户关系表，得到当前用户的粉丝id
        Table relationtable = connection.getTable(TableName.valueOf(TABLE_RELATION));
        //获取粉丝的用户id
        Get get= new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));
        //先取出所有fans的用户id，存放在一个集合当中
        List<byte[]> fans = new ArrayList<byte[]>();

        Result result = relationtable.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells){
            //取出当前用户所有的粉丝uid
            fans.add(CellUtil.cloneValue(cell));
        }
        //如果没有粉丝，则不需要操作粉丝的收件箱表
        if (fans.size() <= 0) {
            return;
        }

        //开始操作收件箱表
        Table inboTable = connection.getTable(TableName.valueOf(TABLE_INBOX));


        //封装用于操作粉丝收件箱表的Put对象集合
        List<Put> puts = new ArrayList<Put>();
        for (byte[]fansRowKey : fans){

            Put inboxPut = new Put(fansRowKey);

            inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid),
                    Bytes.toBytes(rowkey));
            puts.add(inboxPut);
        }
        //向收件箱表放置数据
        inboTable.put(puts);


        //关闭表与连接器释放资源
        inboTable.close();
        relationtable.close();
        contentTable.close();
        connection.close();
    }

    /**
     * a、在用户关系表中，对当前主动操作的用户id进行添加关注的操作
     * b、在用户关系表中，对被关注的人的用户ID，添加粉丝操作
     * c、对当前用户操作的用户的收件箱表中，添加他所关注的最近的微博rowkey
     */
    public void addAttends(String uid,String... attens) throws IOException {

        //参数过滤：如果没有传递关注的人的uid，则直接返回
        if(attens == null || attens.length <=0 || uid == null) return;
        //a
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
        List<Put> puts = new ArrayList<Put>();
        //在微博关注用户关系表中，添加新关注的好友
        Put attendPut = new Put(Bytes.toBytes(uid));
        for (String attend : attens){

            //为当前用户添加关注人
            attendPut.addColumn(Bytes.toBytes("attends"),Bytes.toBytes(attend)
                    ,Bytes.toBytes(attend));
            //  被关注的人添加粉丝uid
            Put fansput= new Put(Bytes.toBytes(attend));
            fansput.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
            puts.add(fansput);




        }
        relationTable.put(attendPut);
        relationTable.put(puts);
        //c
        //取的微博内容表
        Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        Scan scan = new Scan();
        //用于存放扫描出来的我所关注人的微博rowkey
        List<byte[]> rowkeys  = new ArrayList<byte[]>();

        for (String attend : attens) {
            //1002_124155314
            //扫描微博rowkey，用户rowFiltet过滤器
            RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator(attend+"_"));
            scan.setFilter(filter);
            //通过scan扫描结果
            ResultScanner results =contentTable.getScanner(scan);
            Iterator<Result> iterator = results.iterator();
            while (iterator.hasNext()){
                Result result =iterator.next();
                rowkeys.add(result.getRow());
            }

        }
        //将取出微博的rowkey放置于当前操作的这个用户的收件箱中
        //如果所关注的人，没有一条微博，则直接返回
        if (rowkeys.size() <= 0)return;

        //操作indexTable
        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
        Put inboxput= new Put(Bytes.toBytes(uid));
        for (byte[] rowkey : rowkeys) {
            String rowString = Bytes.toString(rowkey);
            String addtendUID = rowString.split("_")[0];
            String attendWeiboT = rowString.split("_")[1];
            inboxput.addColumn(Bytes.toBytes("info"), Bytes.toBytes(addtendUID),
                    Long.valueOf(attendWeiboT),rowkey);


        }
        //关闭资源
        inboxTable.put(inboxput);
        inboxTable.close();
        relationTable.close();
        contentTable.close();

    }

    /**
     * 取关
     *a.删除你要取关的人的uuid
     *b.删除被你取关的那个人的粉丝中的当前操作用户的id
     *c.删除收件箱表中你取关的人微博的rowkey
     *
     */
    public void removeAttends(String uid, String... attends) throws IOException {
        //过滤参数如果没有传递关注人的uid，则直接返回
        if(attends == null || attends.length <=0 || uid == null )return;
        Connection connection = ConnectionFactory.createConnection(configuration);
        //a
        //得到用户关系
        Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
        Delete attendDelete = new Delete(Bytes.toBytes(uid));
        List<Delete> fansDelete = new ArrayList<Delete>();
        for (String attend : attends) {
            //b 将对面用户关系表中移除粉丝
            attendDelete.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend));
            Delete delete =new Delete(Bytes.toBytes(attend));
            delete.addColumn(Bytes.toBytes("fans"), Bytes.toBytes("uid"));
            fansDelete.add(delete);
        }
        relationTable.delete(attendDelete);

        //c
        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
        Delete delete = new Delete(Bytes.toBytes(uid));

        for (String attend : attends) {
            delete.addColumns(Bytes.toBytes("info"), Bytes.toBytes(attend));

        }
        inboxTable.delete(delete);

        //释放资源
        inboxTable.close();
        relationTable.close();
        connection.close();
    }
    /**
     * 查看微博内容
     * a. 从微博收件箱中获取所有关注的额人发布的微博rowkey
     * b.根部得到的微博rowkey。取微博内容中得到数据
     * c.将取出的数据解码然后封装到messa
     */
    public List<Message> getAttendsContent(String uid ) throws IOException {

        //a
        Connection connection= ConnectionFactory.createConnection(configuration);
        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
        //从收件箱表中获取微博的rowkey
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.addFamily(Bytes.toBytes("info"));
        //每个cell中存储了100个版本我们只取出5个版本
        inboxGet.setMaxVersions(5);

        Result inboxResult = inboxTable.get(inboxGet);
        //准备一个存放所有微博的rowkey的集合
        List<byte[]> rowkeys = new ArrayList<byte[]>();
        Cell[] inboxCells = inboxResult.rawCells();
        //组装rowkeys集合
        for (Cell cell : inboxCells) {
            rowkeys.add(CellUtil.cloneValue(cell));

        }
        //b
        // 根据微博rowkeys，去内容表中取的微博实际内容的数据
        Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
        List<Get> contentGets = new ArrayList<Get>();
        for (byte[] rowkey : rowkeys) {
            Get contentGet = new Get(rowkey);
            contentGets.add(contentGet);

        }
        List<Message> messages = new ArrayList<Message>();
        //所有结果的数据
        Result[] contentResults = contentTable.get(contentGets);
        for (Result r : contentResults) {
            Cell[] cs= r.rawCells();
            for (Cell c : cs){
                //取得contentTable中的rowkey
                String rk = Bytes.toString(r.getRow());
                //发布微博人的uid
                String pubilcuid = rk.split("_")[0];

                Long publicTs=Long.valueOf(rk.split("_")[1]);
                Message msg = new Message();
                msg.setUid(pubilcuid);
                msg.setTimestamp(publicTs);
                msg.setContent(Bytes.toString(CellUtil.cloneValue(c)));

                messages.add(msg);
            }
        }


        contentTable.close();
        inboxTable.close();
        connection.close();

    return messages;



    }


    /**
     * 测试用例
     *
     *
     */
    //发布微博
    public  static void publishWeiboTest(Weibo weibo ,String uid,String content) throws IOException {


        weibo.publishContent(uid,content);





    }
    public static void  addattendTest(Weibo weibo ,String uid,String... attends) throws IOException {


        weibo.addAttends(uid,attends);


    }

    //
    public static void removeAttendTest(Weibo weibo ,String uid,String... attends) throws IOException {
        weibo.removeAttends(uid,attends);

    }
    public static void scanWeiboContent(Weibo weibo,String uid) throws IOException {


        List<Message> list = weibo.getAttendsContent(uid);
        System.out.println(list);

    }
    
    //刷微博

    public static void main(String[] args) throws IOException {


        Weibo wb= new Weibo();
//        wb.init();
//        publishWeiboTest(wb,"1002","尼玛买才超级加倍");
//        publishWeiboTest(wb,"1002","尼玛买才超级加倍");
//        publishWeiboTest(wb,"1002","尼玛买 才超级加倍");
//        publishWeiboTest(wb,"1003","尼玛买才超级加倍");
//        publishWeiboTest(wb,"1003","尼玛买才超级加倍");

//        addattendTest(wb, "1001", "1002", "1003");
//        removeAttendTest(wb,"1001","1002");
        addattendTest(wb,"1003","1002","1001");
        scanWeiboContent(wb,"1003");
        publishWeiboTest(wb, "1001", "dasd");
        publishWeiboTest(wb, "1001", "daweqsd");
        publishWeiboTest(wb, "1001", "dasd");
        publishWeiboTest(wb, "1001", "dasewqd");
        publishWeiboTest(wb, "1001", "dasd");
        publishWeiboTest(wb, "1001", "dasd");
        publishWeiboTest(wb, "1001", "dasweqd");
        publishWeiboTest(wb, "1001", "dasd");
        publishWeiboTest(wb, "1001", "dasd");




        scanWeiboContent(wb,"1003");

    }
}
