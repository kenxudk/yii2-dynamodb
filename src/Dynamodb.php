<?php
namespace app\components;
use Yii;
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;
use Aws\DynamoDb\DynamoDbClient;
use Aws\Sdk;
class Dynamodb
{
    protected static $_client;
    protected static $_marshaler;
    protected static $item;
    protected static $connection;
    protected static $table_prefix;
    private function __construct()
    {
    }
    private function __clone()
    {
    }
    public function client()
    {
        if (self::$_client instanceof self) {
            return self::$_client;
        }
        $awskey = Yii::$app->params["aws"]["key"];
        $secret = Yii::$app->params["aws"]["secret"];
        $region = Yii::$app->params["aws"]["region"];
        $version = Yii::$app->params["aws"]["version"];
        return  new DynamoDbClient([
            // 'endpoint'   => 'http://localhost:8000',
            'region'   => $region,
            'version'  => $version,
            'credentials' => [
                'key' => $awskey,
                'secret' => $secret,
            ],
        ]);
    }

    public  function Marshal()
    {
        if (self::$_marshaler !== null)
            return self::$_marshaler;

        return self::$_marshaler = new Marshaler();
    }

    //数组转化为dynamodb的格式
    public  function marshalItem($array=[]){
        return $this->Marshal()->marshalItem($array);
    }
   
    public  function unmarshalItem($array=[]){
         return $this->Marshal()->unmarshalItem($array);
    }


    public static function getConnection()
    {
        if (self::$connection !== null)
            return self::$connection;

        return self::$connection = new self();
    }


    public  static function table($table_name='',$IndexName=''){
        if(empty($table_name)){
            return false;
        }
        self::$item = [];
        self::$item['TableName'] = $table_name;
        if(!empty($IndexName)){
            //二级索引名
            self::$item['IndexName'] = $IndexName;
        }
        return self::getConnection();
    }

    /**
     * [where 条件拼接]
     * @param  [type] $where [description]如果为string则为queryitem条件样式，数组这位主键条件
     * 在ueryitem条件样式时expressValue()必填
     * @return [type]           [description]
     */
    public function kwhere($where){
        if(is_array($where)){
            if(count($where) == count($where,1)){
                self::$item['Key'] = $this->marshalItem($where);
            }else{
                foreach ($where as $key => $value) {
                    $where[$key] = $this->marshalItem($value);
                }
                self::$item['Keys'] = $where;
            }
            
        }else if(is_string($where)){
            self::$item['KeyConditionExpression'] = $where;
        }
        return self::getConnection();
    }

    
    //拼入 FilterExpression ，过滤条件
    public function fwhere($str){
        self::$item['FilterExpression'] = $str;
        return self::getConnection();
    }
  
    //拼入conditionExpression在删除时非key值条件用这个
    public function cwhere($str){
        self::$item['ConditionExpression'] = $str;
        return self::getConnection();
    }
    //拼入 ExpressionAttributeValues
    //$array 数组 ，key作为占位字符,以:开头，value作为值
    //[':v1'=>'string',':v2'=>11,...]
    public function expressValue($array=[]){
        //$midarr = [];
        // if($array && is_array($array)){
        //      foreach ($array as $key => $value) {
        //         $midarr[':'.$key] = $value;
        //      }
        // }
        self::$item['ExpressionAttributeValues'] = $this->marshalItem($array);
        return self::getConnection();
    }
    //在名字与dbnamodb保留关键词冲突是使用
    //['#A'=>'filename','#A2'=>'filename2',...]
    public function  expressName($array=[]){
        self::$item['ExpressionAttributeNames'] = $array;
        return self::getConnection();
    }
    //排序，这里默认为false，排序主键 倒叙
    public function order($order=false){
        self::$item['ScanIndexForward'] = $order;
        return self::getConnection();
    }

    //是否强制一致性，默认不
    public function force($is_force=false){
        self::$item['ConsistentRead'] = $is_force;
        return self::getConnection();
    }

   //对结果再次筛选
   //"FilterExpression": "delstatus = :v3",
   public function having($str=''){
        self::$item['FilterExpression'] =  $str;
        return self::getConnection();
   }

   //限制条数
   //$lastEvaluatedKey 如果有值，下次query从这离开时
   public function limit($num=10,$lastEvaluatedKey=[]){
        if(intval($num) > 0){
            self::$item['Limit'] = $num;
        }
        if(!empty($lastEvaluatedKey) && is_array($lastEvaluatedKey)){
            self::$item['ExclusiveStartKey'] = $this->marshalItem($lastEvaluatedKey);
        }
        return self::getConnection();
   }

   //要查询字段
   public function select($str=''){
       if($str){
           self::$item['ProjectionExpression'] = $str;
       }
       return self::getConnection();
   }


    //数据单个插入
    public function insert($array=[]){
         try {
            self::$item['Item'] = $this->marshalItem($array);
            // var_dump(self::$item);exit;
            $result = $this->client()->putItem(self::$item);
            // var_dump($result);
            return true;//$result;

        } catch (DynamoDbException $e) {
            \Yii::error(json_encode(self::$item).':'.$e->getMessage(),'kuDynamodb');
            // var_dump($e->getMessage());
            return false;
            // return $e->getMessage();
        }
    }
    //插入多个
    public function inserts($arrays=[]){
       try {
            $items = [];
            foreach ($arrays as $key => $value) {
                $value = $this->marshalItem($value);
                $items[]['PutRequest']['Item'] = $value;
            }
            $tablename = self::$item['TableName'];
            unset(self::$item['TableName']);
            $result = $this->client()->batchWriteItem([
                'RequestItems' => [ $tablename => $items ],
            ]);
            return true;
        }catch (DynamoDbException $e) {
            \Yii::error(json_encode(self::$item).':'.$e->getMessage(),'kuDynamodb');
           return false;//$e->getMessage();
        }
    }
    public function deletes($arrays=[]){
       try {
            $items = [];
            foreach ($arrays as $key => $value) {
                $value = $this->marshalItem($value);
                $items[]['DeleteRequest']['Key'] = $value;
            }
            $tablename = self::$item['TableName'];
            unset(self::$item['TableName']);
            $result = $this->client()->batchWriteItem([
                'RequestItems' => [ $tablename => $items ],
            ]);
            return true;
        }catch (DynamoDbException $e) {
            \Yii::error(json_encode(self::$item).':'.$e->getMessage(),'kuDynamodb');
           return false;//$e->getMessage();
        }
    }

    //根据区键与排序键单个查询
    public function one(){
        try {
            $result = $this->client()->getItem(self::$item);
            if(is_array($result['Item'])){
                return $this->unmarshalItem($result['Item']);
            }
            return $result['Item'];
        } catch (DynamoDbException $e) {
            \Yii::error(json_encode(self::$item).':'.$e->getMessage(),'kuDynamodb');
            return false;//$e->getMessage();
        }
    }

    //条件查询
    public function query(){
        try {
            $result = $this->client()->query(self::$item);
            $returndata = [];
            if(is_array($result['Items']) && !empty($result['Items'])){
                $returndata['data'] = [];
                foreach ($result['Items'] as $key => $value) {
                    $value = $this->unmarshalItem($value);
                    $returndata['data'][] = $value;
                }
                $returndata['lastEvaluatedKey'] = isset($result['LastEvaluatedKey']) ? $this->unmarshalItem($result['LastEvaluatedKey']) : [];   
                return $returndata;     
            }
            return $returndata;
        } catch (DynamoDbException $e) {
            \Yii::error(json_encode(self::$item).':'.$e->getMessage(),'kuDynamodb');
            return false;
            // return $e->getMessage();
        }
    }

    //基于区键与排序键多个查询 
    public function ones(){
        try {
            $tablename = self::$item['TableName'];
            unset(self::$item['TableName']);
            $result = $this->client()->batchGetItem([
                'RequestItems' => [$tablename => self::$item],
            ]);
            $result = $result['Responses'][$tablename];
            if(is_array($result)){
                foreach ($result as $key => $value) {
                    $value = $this->unmarshalItem($value);
                    $result[$key] = $value;
                }
            }
            return $result;
        }catch (DynamoDbException $e) {
            \Yii::error(json_encode(self::$item).':'.$e->getMessage(),'kuDynamodb');
            return false;//$e->getMessage();
        }
    }

    //修改
    //array  数组
    //[['createat',':v1'],['createat',':v2',-]]
    //待修改字段名
    //value修改的展位符名
    public function update($array=[]){
        $str = '';
        if(!empty($array)){
            $str = 'SET ';
            if(count($array) == count($array,1)){
                 $str .= $this->getStr($array);
            }else{
                foreach ($array as  $value) {
                    $str .= $this->getStr($value);
                }                
            }
            $str = trim($str,',');
        }
        try {
            self::$item['UpdateExpression'] = $str;
            $result = $this->client()->updateItem(self::$item);
            return $result;

        } catch (DynamoDbException $e) {
            \Yii::error(json_encode(self::$item).':'.$e->getMessage(),'kuDynamodb');
            return false;//$e->getMessage();
        }
    }
    
    public function getStr($value=[]){
        $str = '';
        if(isset($value[2])){
            $str .= ' '.$value[0].'='.$value[0].$value[2].$value[1].' ,';
        }else{
            $str .= ' '.$value[0].'='.$value[1].' ,';
        }
        return $str;
    }

    //删除
    public function delete(){
         try {
            $result = $this->client()->deleteItem(self::$item);
            return $result;

        } catch (DynamoDbException $e) {
            \Yii::error(json_encode(self::$item).':'.$e->getMessage(),'kuDynamodb');
            return false;
            // return $e->getMessage();
        }
    }

    //全表扫描
    public  function scan()
    {
        try {
            $result = self::GetClient()->scan(self::$item);
            return $result['Items'];

        } catch (DynamoDbException $e) {
            // return $e->getMessage();
            \Yii::error(json_encode(self::$item).':'.$e->getMessage(),'kuDynamodb');
            return false;
        }
    }

}